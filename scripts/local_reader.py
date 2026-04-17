"""
本地 Parquet 数据读取器

提供与 SCReader 兼容的 API，从本地 Parquet 文件读取数据，
无需连接远程 MySQL 数据库。

用法：
    from local_reader import LocalParquetReader

    # 创建本地读取器
    reader = LocalParquetReader('./data/parquet')

    # 与 SCReader 相同的 API
    tables = reader.list_tables()
    df = reader.query_df('tempdata', '2025-01-01', '2025-01-31')

    # 也可以直接读取整个表
    df = reader.read_table('tempdata')
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd


class LocalParquetReader:
    """
    本地 Parquet 数据读取器

    提供与 SCReader 兼容的 API，从本地 Parquet 文件读取数据。
    """

    def __init__(
        self,
        data_dir: str = "./data/parquet",
        state_file: Optional[str] = None,
    ):
        """
        初始化本地读取器

        Args:
            data_dir: Parquet 文件目录
            state_file: watermark 状态文件路径（用于增量读取）
        """
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"数据目录不存在: {data_dir}")

        self.state_file = Path(state_file) if state_file else None
        self._watermarks: Dict[str, Dict] = {}

        # 缓存已加载的表
        self._cache: Dict[str, pd.DataFrame] = {}

        # 加载状态
        if self.state_file and self.state_file.exists():
            self._load_state()

    def _load_state(self):
        """加载 watermark 状态"""
        if self.state_file and self.state_file.exists():
            state = json.loads(self.state_file.read_text())
            for table, wm in state.items():
                self._watermarks[table] = {
                    "last_ts": datetime.fromisoformat(wm["last_ts"]) if wm.get("last_ts") else None,
                    "last_id": wm.get("last_id"),
                }

    def save_state(self, path: Optional[str] = None):
        """保存 watermark 状态"""
        save_path = Path(path) if path else self.state_file
        if not save_path:
            return

        state = {}
        for table, wm in self._watermarks.items():
            state[table] = {
                "last_ts": wm["last_ts"].isoformat() if wm.get("last_ts") else None,
                "last_id": wm.get("last_id"),
            }
        save_path.write_text(json.dumps(state, indent=2))

    def list_tables(self) -> List[str]:
        """列出所有可用的表"""
        return [f.stem for f in self.data_dir.glob("*.parquet")]

    def get_table_info(self, table_name: str, columns_only: bool = False) -> Union[pd.DataFrame, List[str]]:
        """获取表结构信息"""
        df = self.read_table(table_name)
        columns = list(df.columns)

        if columns_only:
            return columns

        # 构造类似 MySQL DESCRIBE 的输出
        info = []
        for col in columns:
            dtype = str(df[col].dtype)
            info.append({
                "Field": col,
                "Type": dtype,
                "Null": "YES",
                "Key": "",
                "Default": None,
                "Extra": "",
            })
        return pd.DataFrame(info)

    def read_table(self, table_name: str, use_cache: bool = True) -> pd.DataFrame:
        """
        读取整个表

        Args:
            table_name: 表名
            use_cache: 是否使用缓存

        Returns:
            DataFrame
        """
        if use_cache and table_name in self._cache:
            return self._cache[table_name]

        parquet_path = self.data_dir / f"{table_name}.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(f"表不存在: {table_name}")

        df = pd.read_parquet(parquet_path, engine="pyarrow")

        # 确保索引是 DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        if use_cache:
            self._cache[table_name] = df

        return df

    def query_df(
        self,
        table_name: Union[str, List[str]],
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        按时间范围查询数据（与 SCReader.query_df 兼容）

        Args:
            table_name: 表名或表名列表
            start_time: 开始时间
            end_time: 结束时间
            columns: 要查询的列

        Returns:
            DataFrame
        """
        if isinstance(table_name, str):
            table_names = [table_name]
        else:
            table_names = table_name

        dataframes = []

        for table in table_names:
            try:
                df = self.read_table(table)

                # 时间过滤
                if start_time is not None:
                    start_ts = pd.to_datetime(start_time)
                    df = df[df.index >= start_ts]

                if end_time is not None:
                    end_ts = pd.to_datetime(end_time)
                    # 如果只有日期，设置为当天结束
                    if len(str(end_time).strip()) <= 10:
                        end_ts = end_ts.replace(hour=23, minute=59, second=59)
                    df = df[df.index <= end_ts]

                # 列过滤
                if columns:
                    available_cols = [c for c in columns if c in df.columns]
                    if available_cols:
                        df = df[available_cols]

                if not df.empty:
                    dataframes.append(df)

            except FileNotFoundError:
                print(f"Warning: Table '{table}' not found, skipping...")
                continue

        if not dataframes:
            return pd.DataFrame()

        if len(dataframes) == 1:
            return dataframes[0]

        # 合并多个表
        result = pd.concat(dataframes, axis=0, sort=False)
        result = result.sort_index()
        result = result[~result.index.duplicated(keep="last")]
        return result

    def query_by_time(
        self,
        table_name: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> pd.DataFrame:
        """query_df 的别名，保持兼容性"""
        return self.query_df(table_name, start_time, end_time)

    def get_time_range(self, table_name: str) -> dict:
        """获取表的时间范围"""
        df = self.read_table(table_name)
        if df.empty:
            return {"min_time": None, "max_time": None}
        return {
            "min_time": df.index.min(),
            "max_time": df.index.max(),
        }

    def preview_table_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """预览表数据"""
        df = self.read_table(table_name)
        return df.head(limit)

    def read_incremental(
        self,
        table_name: str,
        lookback: str = "2s",
    ) -> pd.DataFrame:
        """
        增量读取（基于内存 watermark）

        Args:
            table_name: 表名
            lookback: 回看窗口

        Returns:
            新增数据
        """
        df = self.read_table(table_name, use_cache=False)

        if df.empty:
            return df

        wm = self._watermarks.get(table_name, {})
        last_ts = wm.get("last_ts")

        if last_ts is not None:
            # 解析 lookback
            import re
            match = re.match(r"^(\d+(?:\.\d+)?)\s*(ms|us|s|m|h|d)$", lookback.lower())
            if match:
                value = float(match.group(1))
                unit = match.group(2)
                unit_map = {
                    "us": pd.Timedelta(microseconds=value),
                    "ms": pd.Timedelta(milliseconds=value),
                    "s": pd.Timedelta(seconds=value),
                    "m": pd.Timedelta(minutes=value),
                    "h": pd.Timedelta(hours=value),
                    "d": pd.Timedelta(days=value),
                }
                lookback_td = unit_map[unit]
            else:
                lookback_td = pd.Timedelta(seconds=2)

            cutoff = pd.Timestamp(last_ts) - lookback_td
            df = df[df.index > cutoff]

        # 更新 watermark
        if not df.empty:
            last_idx = df.index[-1]
            if hasattr(last_idx, 'to_pydatetime'):
                last_ts_val = last_idx.to_pydatetime()
            else:
                last_ts_val = pd.Timestamp(last_idx).to_pydatetime()
            self._watermarks[table_name] = {
                "last_ts": last_ts_val,
                "last_id": None,
            }

        return df

    def reset_watermark(self, table: Optional[str] = None):
        """重置 watermark"""
        if table is None:
            self._watermarks.clear()
        elif table in self._watermarks:
            del self._watermarks[table]

    def clear_cache(self, table: Optional[str] = None):
        """清除缓存"""
        if table is None:
            self._cache.clear()
        elif table in self._cache:
            del self._cache[table]

    def close(self):
        """关闭读取器（保存状态）"""
        if self.state_file:
            self.save_state()
        self._cache.clear()

    @property
    def memory_usage_mb(self) -> float:
        """缓存内存占用（MB）"""
        total = 0
        for df in self._cache.values():
            total += df.memory_usage(deep=True).sum()
        return total / 1024 / 1024

    def __repr__(self):
        tables = self.list_tables()
        return f"LocalParquetReader(dir='{self.data_dir}', tables={len(tables)}, cache_mb={self.memory_usage_mb:.1f})"


# 便捷函数
def create_local_reader(data_dir: str = "./data/parquet") -> LocalParquetReader:
    """创建本地读取器的便捷函数"""
    return LocalParquetReader(data_dir)
