"""
慢控数据读取模块

提供 SCReader 类用于从 MySQL 数据库读取慢控数据
支持全量查询和基于 watermark 的增量读取
"""

import json
import re
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pymysql

from .config import DEFAULT_MYSQL_CONFIG, MySQLConfig
from .spec import TableSpec

# 用于验证表名/列名的正则
SAFE_IDENTIFIER_PATTERN = re.compile(r"^[\w()]+$")


def _validate_identifier(name: str, kind: str = "identifier") -> str:
    """验证 SQL 标识符，防止注入"""
    if not SAFE_IDENTIFIER_PATTERN.match(name):
        raise ValueError(f"非法的 {kind}: '{name}'")
    return name


def _parse_timedelta(s: str) -> timedelta:
    """
    解析时间字符串为 timedelta

    支持: '200ms', '2s', '5m', '1h', '1d', '500us'
    """
    match = re.match(r"^(\d+(?:\.\d+)?)\s*(ms|us|s|m|h|d)$", s.lower())
    if not match:
        raise ValueError(f"无法解析时间: '{s}'")

    value = float(match.group(1))
    unit = match.group(2)

    unit_map = {
        "us": timedelta(microseconds=value),
        "ms": timedelta(milliseconds=value),
        "s": timedelta(seconds=value),
        "m": timedelta(minutes=value),
        "h": timedelta(hours=value),
        "d": timedelta(days=value),
    }
    return unit_map[unit]


class SCReader:
    """
    慢控数据读取器

    提供便捷的 API 用于查询和探索慢控数据库，支持：
    - 全量查询：按时间范围查询数据
    - 增量读取：基于 watermark 机制的增量数据同步
    - 状态持久化：支持断点续传

    Examples:
        >>> # 基础全量查询
        >>> reader = SCReader(host='10.11.50.141', user='read', password='111111')
        >>> data = reader.query_by_time('table_name', '2025-01-01', '2025-01-31')
        >>> tables = reader.list_tables()

        >>> # 增量读取
        >>> reader = SCReader(state_path='./watermark.json')
        >>> spec = TableSpec('temperature', 'Time(s)')
        >>> df = reader.read_incremental(spec, lookback='2s')
        >>> df_new = reader.read_incremental(spec)  # 只返回新数据
    """

    _MAX_READ_MULTIPLE_WORKERS = 8

    def __init__(
        self,
        config: Optional[Union[MySQLConfig, str, Path]] = None,
        state_path: Optional[Union[str, Path]] = None,
        time_zone: Optional[str] = "Asia/Shanghai",
        **kwargs,
    ):
        """
        初始化慢控数据读取器

        Args:
            config: MySQL 配置对象或 JSON 配置文件路径，None 使用默认配置
            state_path: watermark 持久化路径（用于增量读取），支持 str/Path
            time_zone: 时间列统一时区；None 表示保持原始（naive）
            **kwargs: MySQL 连接参数（可覆盖 config）
                - host: 数据库主机
                - user: 用户名
                - password: 密码
                - database: 数据库名
                - port: 端口
                - charset: 字符集

        Examples:
            >>> # 使用默认配置
            >>> reader = SCReader()

            >>> # 使用自定义配置
            >>> reader = SCReader(host='192.168.1.100', user='myuser')

            >>> # 从配置文件路径加载
            >>> reader = SCReader(config='./sc_config.json')

            >>> # 启用增量读取状态持久化
            >>> reader = SCReader(state_path='./watermark.json')
        """
        if config is None:
            cfg = DEFAULT_MYSQL_CONFIG
        elif isinstance(config, MySQLConfig):
            cfg = config
        else:
            cfg = MySQLConfig.from_json(config)

        # 允许 kwargs 覆盖 config
        init_kwargs = cfg.pymysql_kwargs.copy()
        init_kwargs.update(kwargs)

        # 保存连接参数用于重连
        self._host = init_kwargs.get("host", "10.11.50.141")
        self._user = init_kwargs.get("user", "read")
        self._password = init_kwargs.get("password", "111111")
        self._database = init_kwargs.get("database", "slowcontroldata")
        self._port = init_kwargs.get("port", 3306)
        self._charset = init_kwargs.get("charset", "utf8mb4")

        # 建立数据库连接
        self.conn = pymysql.connect(**init_kwargs)
        self.cursor = self.conn.cursor()

        # 时间处理配置
        self._time_zone = time_zone

        # 增量读取相关
        self.state_path = Path(state_path) if state_path is not None else None
        # watermark: {table: {'last_ts': datetime, 'last_id': Any}}
        self._watermarks: Dict[str, Dict[str, Any]] = {}
        # 表结构缓存（避免同一轮增量里重复 DESCRIBE）
        self._table_info_cache: Dict[str, pd.DataFrame] = {}
        self._time_col_cache: Dict[str, str] = {}

        # 加载已有状态
        if self.state_path:
            self.load_state(self.state_path)

    def _normalize_ts(self, ts: Any) -> Optional[datetime]:
        """Normalize timestamps to match reader timezone (avoid naive/aware compare errors)."""
        if ts is None:
            return None
        try:
            if pd.isna(ts):
                return None
        except Exception:
            pass

        try:
            ts = pd.Timestamp(ts)
        except Exception:
            return ts

        if self._time_zone:
            if ts.tzinfo is None:
                ts = ts.tz_localize(self._time_zone, nonexistent="shift_forward", ambiguous="NaT")
            else:
                ts = ts.tz_convert(self._time_zone)
        else:
            if ts.tzinfo is not None:
                ts = ts.tz_convert("UTC").tz_localize(None)

        if ts is pd.NaT:
            return None

        return ts.to_pydatetime()

    # ==================== 连接管理 ====================

    def _ensure_connection(self):
        """确保数据库连接和游标是活跃的"""
        try:
            # 检查连接是否活跃
            self.conn.ping(reconnect=True)
            # 如果游标已关闭，重新创建游标
            if self.cursor is None or not hasattr(self.cursor, "connection"):
                self.cursor = self.conn.cursor()
        except (AttributeError, pymysql.Error):
            # 如果连接失败，重新创建连接
            try:
                self.conn.close()
            except (AttributeError, pymysql.Error):
                pass
            self.conn = pymysql.connect(
                host=self._host,
                user=self._user,
                password=self._password,
                database=self._database,
                port=self._port,
                charset=self._charset,
            )
            self.cursor = self.conn.cursor()

    def close(self):
        """
        关闭连接，保存状态（如果启用了 state_path）

        如果初始化时指定了 state_path，会自动保存 watermark 状态。
        """
        if self.state_path:
            self.save_state()
        self.cursor.close()
        self.conn.close()

    # ==================== 基础查询 ====================

    def query(self, sql):
        """执行 SQL 并返回所有结果（tuple 列表）"""
        self._ensure_connection()
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def _query_df(self, sql, time_column=None, chunksize=None, time_unit=None, time_zone=None):
        """
        执行 SQL 并返回 DataFrame，支持分块读取

        Args:
            sql: SQL 查询语句
            time_column: 时间列名，如果为 None 则自动检测
            chunksize: 分块读取的大小
            time_unit: 时间单位（仅当时间列为 BIGINT 时使用），如 's'/'ms'/'us'
            time_zone: 统一时区，None 表示保持原始（naive）

        Returns:
            DataFrame 或生成器（如果使用 chunksize）
        """
        self._ensure_connection()
        tz = self._time_zone if time_zone is None else time_zone

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")
            df = pd.read_sql(sql, self.conn, chunksize=chunksize)

        # 如果返回的是生成器（分块读取），需要特殊处理
        if chunksize is not None:

            def process_chunk(chunk):
                if time_column is None:
                    # 尝试自动检测时间列
                    time_cols = [
                        col
                        for col in chunk.columns
                        if any(pattern in col.lower() for pattern in ["time", "timestamp", "datetime", "ts"])
                    ]
                    if time_cols:
                        time_col = time_cols[0]
                    else:
                        raise ValueError("无法自动检测时间列，请指定 time_column 参数")
                else:
                    time_col = time_column

                dt = pd.to_datetime(chunk[time_col], unit=time_unit) if time_unit else pd.to_datetime(chunk[time_col])
                if tz:
                    if dt.dt.tz is None:
                        dt = dt.dt.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT")
                    else:
                        dt = dt.dt.tz_convert(tz)
                chunk[time_col] = dt
                chunk = chunk.set_index([time_col])
                return chunk

            return (process_chunk(chunk) for chunk in df)
        else:
            # 单次读取
            if time_column is None:
                # 尝试自动检测时间列
                time_cols = [
                    col
                    for col in df.columns
                    if any(pattern in col.lower() for pattern in ["time", "timestamp", "datetime", "ts"])
                ]
                if time_cols:
                    time_col = time_cols[0]
                else:
                    raise ValueError("无法自动检测时间列，请指定 time_column 参数")
            else:
                time_col = time_column

            dt = pd.to_datetime(df[time_col], unit=time_unit) if time_unit else pd.to_datetime(df[time_col])
            if tz:
                if dt.dt.tz is None:
                    dt = dt.dt.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT")
                else:
                    dt = dt.dt.tz_convert(tz)
            df[time_col] = dt
            df = df.set_index([time_col])
            return df

    # ==================== 表信息查询 ====================

    @property
    def tables_prop(self):
        """每次访问都查询一次表列表"""
        self._ensure_connection()
        self.cursor.execute("SHOW TABLES;")
        return [t[0] for t in self.cursor.fetchall()]

    def list_tables(self) -> List[str]:
        """
        列出数据库中所有的表

        Returns:
            表名列表

        Examples:
            >>> reader = SCReader()
            >>> tables = reader.list_tables()
            >>> print(tables)
        """
        return self.tables_prop

    def get_table_info(self, table_name: str, columns_only: bool = False) -> Union[pd.DataFrame, List[str]]:
        """
        获取表的结构信息

        Args:
            table_name: 表名
            columns_only: 是否只返回列名列表，默认 False

        Returns:
            DataFrame（完整结构信息）或 List[str]（仅列名）

        Examples:
            >>> info = reader.get_table_info('temperature_table')
            >>> print(info)
            >>> columns = reader.get_table_info('temperature_table', columns_only=True)
            >>> print(columns)
        """
        self._ensure_connection()
        cached = self._table_info_cache.get(table_name)
        if cached is None:
            # 使用反引号包围表名，防止特殊字符（如连字符）导致SQL错误
            self.cursor.execute(f"DESCRIBE `{table_name}`;")
            result = self.cursor.fetchall()
            cached = pd.DataFrame(result, columns=["Field", "Type", "Null", "Key", "Default", "Extra"])
            self._table_info_cache[table_name] = cached
        if columns_only:
            return cached["Field"].tolist()
        return cached.copy()

    def preview_table_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """
        预览表中的数据（前几行）

        Args:
            table_name: 表名
            limit: 返回的行数，默认 5 行

        Returns:
            包含数据的 DataFrame

        Examples:
            >>> data = reader.preview_table_data('temperature_table', limit=10)
            >>> print(data.head())
        """
        # 获取时间列名
        time_col = self._get_time_column(table_name)
        # 使用反引号包围表名，防止特殊字符（如连字符）导致SQL错误
        sql = f"SELECT * FROM `{table_name}` LIMIT {limit};"
        return self._query_df(sql, time_column=time_col)

    def _query_single_table(
        self,
        table_name: str,
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None,
        columns: Optional[List[str]] = None,
        chunksize: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        查询单个表的数据（内部方法）

        Args:
            table_name: 表名
            start_time: 开始时间
            end_time: 结束时间
            columns: 要查询的列名列表
            chunksize: 分块读取的大小

        Returns:
            包含查询结果的 DataFrame，索引为时间列
        """
        # 获取表结构信息（只查询一次）
        table_info = self.get_table_info(table_name)
        all_columns = table_info["Field"].tolist()

        # 获取时间列名
        time_col = self._detect_time_column(all_columns, table_name)

        # 检查时间列类型
        time_col_info = table_info[table_info["Field"] == time_col]
        is_bigint_type = False
        if not time_col_info.empty:
            col_type = str(time_col_info.iloc[0]["Type"]).lower()
            if "bigint" in col_type or "int" in col_type:
                is_bigint_type = True

        # 构建 SELECT 子句
        if columns:
            # 从用户指定的列中移除时间列（如果存在），因为时间列会被设置为索引
            user_columns = [col for col in columns if col != time_col]
            # 确保时间列始终包含在查询中（用于 WHERE 和 ORDER BY）
            query_columns = [time_col] + user_columns
            # 使用反引号包围列名，防止特殊字符导致SQL错误
            select_clause = ", ".join([f"`{col}`" for col in query_columns])
        else:
            select_clause = "*"
            user_columns = None  # 表示查询所有列

        where_conditions = []
        if start_time:
            # 解析时间
            if isinstance(start_time, datetime):
                start_dt = start_time
            else:
                start_dt = pd.to_datetime(start_time)

            if is_bigint_type:
                # BIGINT 类型：转换为 Unix 时间戳（秒）
                start_ts = int(start_dt.timestamp())
                where_conditions.append(f"`{time_col}` >= {start_ts}")
            else:
                # DATETIME 或字符串类型：使用字符串比较
                start_time_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
                where_conditions.append(f"`{time_col}` >= '{start_time_str}'")

        if end_time:
            # 解析时间
            if isinstance(end_time, datetime):
                end_dt = end_time
            else:
                end_dt = pd.to_datetime(end_time)
                # 如果只有日期部分，设置为当天结束
                if len(str(end_time).strip()) <= 10:
                    end_dt = end_dt.replace(hour=23, minute=59, second=59)

            if is_bigint_type:
                # BIGINT 类型：转换为 Unix 时间戳（秒）
                end_ts = int(end_dt.timestamp())
                where_conditions.append(f"`{time_col}` <= {end_ts}")
            else:
                # DATETIME 或字符串类型：使用字符串比较
                end_time_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
                where_conditions.append(f"`{time_col}` <= '{end_time_str}'")

        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

        # 构建完整 SQL
        # 使用反引号包围表名，防止特殊字符（如连字符）导致SQL错误
        sql = f"SELECT {select_clause} FROM `{table_name}` WHERE {where_clause} ORDER BY `{time_col}`;"

        # 执行查询
        df = self._query_df(sql, time_column=time_col, chunksize=chunksize)

        # 如果用户指定了列，只返回用户指定的列（时间列已经是索引了，不需要在列中）
        if user_columns is not None:
            # 确保只保留用户请求的列（排除时间列，因为它已经是索引）
            available_columns = [col for col in user_columns if col in df.columns]
            if available_columns:
                df = df[available_columns]

        return df

    def query_df(
        self,
        table_name: Union[str, List[str]],
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None,
        columns: Optional[List[str]] = None,
        chunksize: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        按时间范围查询数据

        Args:
            table_name: 表名（字符串）或表名列表。如果是列表，将从所有表中查询并合并结果
            start_time: 开始时间，格式如 '2025-01-01' 或 '2025-01-01 00:00:00'。
                        如果不指定 start_time 和 end_time，则查询整个表
            end_time: 结束时间，格式如 '2025-01-31' 或 '2025-01-31 23:59:59'。
                      如果不指定 start_time 和 end_time，则查询整个表
            columns: 要查询的列名列表，默认查询所有列
            chunksize: 分块读取的大小，用于大数据量查询

        Returns:
            包含查询结果的 DataFrame，索引为时间列。如果查询多个表，结果会按时间排序并合并

        Examples:
            >>> # 查询整个表（不指定时间范围）
            >>> data = reader.query_df('temperature_table')

            >>> # 查询单个表的所有列（指定时间范围）
            >>> data = reader.query_df('temperature_table', '2025-01-01', '2025-01-31')

            >>> # 查询特定列
            >>> data = reader.query_df(
            ...     'temperature_table',
            ...     start_time='2025-01-01',
            ...     end_time='2025-01-31',
            ...     columns=['temperature', 'pressure']
            ... )

            >>> # 查询多个表
            >>> data = reader.query_df(
            ...     ['piddata', 'tempdata'],
            ...     start_time='2025-01-01',
            ...     end_time='2025-01-31'
            ... )
        """
        # 如果 table_name 是字符串，转换为列表
        if isinstance(table_name, str):
            table_names = [table_name]
        else:
            table_names = table_name

        # 存储所有查询结果
        dataframes = []

        # 遍历所有表并查询
        for table in table_names:
            try:
                # 检查表是否存在
                if table not in self.list_tables():
                    print(f"Warning: Table '{table}' not found, skipping...")
                    continue

                # 查询单个表
                df = self._query_single_table(
                    table_name=table, start_time=start_time, end_time=end_time, columns=columns, chunksize=chunksize
                )

                # 如果查询结果不为空，添加到列表中
                if df is not None and len(df) > 0:
                    dataframes.append(df)

            except Exception as e:
                print(f"Warning: Error querying table '{table}': {e}")
                continue

        # 如果没有查询到任何数据，返回空的 DataFrame
        if not dataframes:
            # 创建一个空的 DataFrame，索引为时间类型
            empty_df = pd.DataFrame()
            empty_df.index = pd.DatetimeIndex([])
            return empty_df

        # 合并所有 DataFrame
        if len(dataframes) == 1:
            result_df = dataframes[0]
        else:
            # 合并多个 DataFrame
            # 使用 outer join 来合并所有列，缺失值用 NaN 填充
            result_df = pd.concat(dataframes, axis=0, sort=False)

            # 按时间索引排序
            result_df = result_df.sort_index()

            # 如果有重复的时间戳，保留最后一个（或者可以根据需要调整策略）
            result_df = result_df[~result_df.index.duplicated(keep="last")]

        return result_df

    def get_time_range(self, table_name: str) -> dict:
        """
        获取表中数据的时间范围

        Args:
            table_name: 表名

        Returns:
            包含最早和最晚时间的字典

        Examples:
            >>> time_range = reader.get_time_range('temperature_table')
            >>> print(f"数据时间范围: {time_range['min_time']} 到 {time_range['max_time']}")
        """
        # 获取时间列名
        time_col = self._get_time_column(table_name)
        # 使用反引号包围表名和列名，防止特殊字符（如连字符）导致SQL错误
        sql = f"SELECT MIN(`{time_col}`) as min_time, MAX(`{time_col}`) as max_time FROM `{table_name}`;"
        result = self.query(sql)

        # 将字符串时间转换为 datetime（如果可能）
        min_time = result[0][0]
        max_time = result[0][1]

        # 尝试转换为 datetime
        try:
            if min_time is not None:
                min_time = pd.to_datetime(min_time, errors="coerce")
            if max_time is not None:
                max_time = pd.to_datetime(max_time, errors="coerce")
        except (ValueError, TypeError):
            pass  # 如果转换失败，保持原值

        return {"min_time": min_time, "max_time": max_time}

    def _get_time_column(self, table_name: str) -> str:
        """
        自动检测表中的时间列名

        尝试匹配常见的时间列名：Time(s), timestamp, time, datetime, ts 等

        Args:
            table_name: 表名

        Returns:
            时间列名

        Raises:
            ValueError: 如果找不到时间列
        """
        cached = self._time_col_cache.get(table_name)
        if cached is not None:
            return cached
        columns = self.get_table_info(table_name, columns_only=True)
        detected = self._detect_time_column(columns, table_name)
        self._time_col_cache[table_name] = detected
        return detected

    def _detect_time_column(self, columns: List[str], table_name: str = "unknown") -> str:
        """
        从列名列表中检测时间列（不查询数据库）

        Args:
            columns: 列名列表
            table_name: 表名（仅用于错误信息）

        Returns:
            时间列名
        """
        # 常见的时间列名模式（按优先级排序）
        time_patterns = ["Time(s)", "timestamp", "time", "datetime", "ts", "date_time", "Time", "TIME"]

        # 首先尝试精确匹配
        for pattern in time_patterns:
            if pattern in columns:
                return pattern

        # 尝试不区分大小写的匹配
        columns_lower = [col.lower() for col in columns]
        for pattern in time_patterns:
            if pattern.lower() in columns_lower:
                idx = columns_lower.index(pattern.lower())
                return columns[idx]

        # 如果都找不到，抛出错误
        raise ValueError(f"无法在表 '{table_name}' 中找到时间列。可用列: {', '.join(columns)}")

    # ==================== 增量读取相关方法 ====================

    def _get_watermark(self, table: str) -> Dict[str, Any]:
        """获取表的 watermark"""
        if table not in self._watermarks:
            self._watermarks[table] = {"last_ts": None, "last_id": None}
        return self._watermarks[table]

    def _update_watermark(self, table: str, last_ts: datetime, last_id: Any = None):
        """更新 watermark"""
        wm = self._get_watermark(table)
        last_ts = self._normalize_ts(last_ts)
        wm_ts = self._normalize_ts(wm["last_ts"])
        if wm_ts is None or (last_ts is not None and last_ts > wm_ts):
            wm["last_ts"] = last_ts
            wm["last_id"] = last_id
        elif last_ts is not None and wm_ts is not None and last_ts == wm_ts and last_id is not None:
            if wm["last_id"] is None or last_id > wm["last_id"]:
                wm["last_id"] = last_id

    def read_incremental(
        self, spec: Union[TableSpec, str], lookback: str = "2s", chunksize: Optional[int] = None
    ) -> pd.DataFrame:
        """
        增量读取表数据

        基于 watermark 机制，只读取上次读取位置之后的新数据。
        支持 lookback 回看窗口处理乱序写入。

        Args:
            spec: TableSpec 或表名字符串
            lookback: 回看窗口，如 '2s'，用于处理乱序写入
            chunksize: 分块大小，用于大数据量查询

        Returns:
            增量数据 DataFrame，索引为时间列

        Examples:
            >>> # 使用 TableSpec
            >>> spec = TableSpec('tempdata', 'timestamp', key_col='id')
            >>> df = reader.read_incremental(spec, lookback='2s')

            >>> # 使用表名字符串（简化）
            >>> df = reader.read_incremental('tempdata', lookback='2s')

            >>> # 再次调用只返回新数据
            >>> df_new = reader.read_incremental(spec)
        """
        # 支持字符串简写
        if isinstance(spec, str):
            spec = TableSpec(table=spec)

        table = _validate_identifier(spec.table, "table")

        # 自动检测时间列
        if spec.time_col:
            time_col = _validate_identifier(spec.time_col, "time_col")
        else:
            time_col = self._get_time_column(table)

        lookback_td = _parse_timedelta(lookback)
        wm = self._get_watermark(table)

        # 构建 SELECT
        if spec.cols:
            cols = [_validate_identifier(c, "column") for c in spec.cols]
            if time_col not in cols:
                cols = [time_col] + cols
            if spec.key_col and spec.key_col not in cols:
                cols.append(_validate_identifier(spec.key_col, "key_col"))
            select_clause = ", ".join([f"`{c}`" for c in cols])
        else:
            select_clause = "*"

        # 构建 WHERE
        where_parts = []
        if wm["last_ts"] is not None:
            cutoff = wm["last_ts"] - lookback_td
            if spec.time_unit:
                # BIGINT 时间戳
                unit_multiplier = {"s": 1, "ms": 1000, "us": 1_000_000}[spec.time_unit]
                cutoff_val = int(cutoff.timestamp() * unit_multiplier)
                where_parts.append(f"`{time_col}` >= {cutoff_val}")
            else:
                cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
                # 检测时间列类型，决定使用哪种比较方式
                table_info = self.get_table_info(table)
                time_col_info = table_info[table_info["Field"] == time_col]
                is_string_type = False
                if not time_col_info.empty:
                    col_type = str(time_col_info.iloc[0]["Type"]).lower()
                    if "varchar" in col_type or "char" in col_type or "text" in col_type:
                        is_string_type = True

                if is_string_type:
                    # 字符串类型时间列：使用直接字符串比较（假设格式为 'YYYY-MM-DD HH:MM:SS'）
                    # 这样可以避免 STR_TO_DATE 格式不匹配的问题
                    where_parts.append(f"`{time_col}` >= '{cutoff_str}'")
                else:
                    # DATETIME 类型：直接比较，不需要 STR_TO_DATE
                    where_parts.append(f"`{time_col}` >= '{cutoff_str}'")

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        sql = f"SELECT {select_clause} FROM `{table}` WHERE {where_clause} ORDER BY `{time_col}`"

        # 执行查询
        df = self._query_df(sql, time_column=time_col, chunksize=chunksize)

        # 处理分块读取
        if chunksize is not None:
            chunks = list(df)  # 消费生成器
            if not chunks:
                return pd.DataFrame()
            df = pd.concat(chunks)

        if df.empty:
            return df

        # 转换 BIGINT 时间戳
        # query_df 已处理时间列转换，这里不需要额外处理

        # 去重
        if spec.key_col and spec.key_col in df.columns:
            df = df.reset_index()
            df = df.drop_duplicates(subset=[time_col, spec.key_col], keep="last")
            df = df.set_index(time_col)
        else:
            df = df[~df.index.duplicated(keep="last")]

        # 排序
        df = df.sort_index()

        # 更新 watermark
        if not df.empty:
            last_ts = df.index[-1]
            if isinstance(last_ts, pd.Timestamp):
                last_ts = last_ts.to_pydatetime()
            last_id = df.iloc[-1][spec.key_col] if spec.key_col and spec.key_col in df.columns else None
            self._update_watermark(table, last_ts, last_id)

        return df

    def read_multiple(self, specs: List[TableSpec], lookback: str = "2s") -> Dict[str, pd.DataFrame]:
        """
        增量读取多个表（顺序执行，复用连接和缓存）

        Args:
            specs: 表规格列表
            lookback: 回看窗口

        Returns:
            {表名: DataFrame} 字典

        Examples:
            >>> specs = [
            ...     TableSpec('tempdata', 'timestamp'),
            ...     TableSpec('runlidata', 'timestamp')
            ... ]
            >>> results = reader.read_multiple(specs, lookback='2s')
        """
        if not specs:
            return {}

        # 顺序读取，复用连接和表结构缓存
        results = {}
        for spec in specs:
            results[spec.table] = self.read_incremental(spec, lookback)

        return results

    def reset_watermark(self, table: Optional[str] = None):
        """
        重置 watermark

        Args:
            table: 表名，None 表示重置所有表的 watermark

        Examples:
            >>> # 重置单个表
            >>> reader.reset_watermark('tempdata')

            >>> # 重置所有表
            >>> reader.reset_watermark()
        """
        if table is None:
            self._watermarks.clear()
        elif table in self._watermarks:
            del self._watermarks[table]

    def save_state(self, path: Optional[Union[str, Path]] = None):
        """
        保存 watermark 到 JSON 文件

        Args:
            path: 保存路径，支持 str/Path；None 使用初始化时的 state_path

        Examples:
            >>> reader.save_state('./watermark.json')
        """
        path_obj = Path(path) if path is not None else self.state_path
        if path_obj is None:
            return

        path_obj.parent.mkdir(parents=True, exist_ok=True)

        state = {}
        for table, wm in self._watermarks.items():
            state[table] = {"last_ts": wm["last_ts"].isoformat() if wm["last_ts"] else None, "last_id": wm["last_id"]}
        path_obj.write_text(json.dumps(state, indent=2))

    def load_state(self, path: Optional[Union[str, Path]] = None):
        """
        从 JSON 文件加载 watermark

        Args:
            path: 加载路径，支持 str/Path；None 使用初始化时的 state_path

        Examples:
            >>> reader.load_state('./watermark.json')
        """
        path_obj = Path(path) if path is not None else self.state_path
        if path_obj is None:
            return

        if not path_obj.exists():
            return

        state = json.loads(path_obj.read_text())
        for table, wm in state.items():
            self._watermarks[table] = {
                "last_ts": datetime.fromisoformat(wm["last_ts"]) if wm["last_ts"] else None,
                "last_id": wm["last_id"],
            }
