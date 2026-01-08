from functools import cached_property

import pandas as pd
import pymysql


class MySQLReader:
    def __init__(
        self,
        host="10.11.50.141",
        user="read",
        password="111111",
        database="slowcontroldata",
        port=3306,
        charset="utf8mb4",
    ):
        """初始化数据库连接"""
        # 保存连接参数以便重新连接
        self._host = host
        self._user = user
        self._password = password
        self._database = database
        self._port = port
        self._charset = charset

        self.conn = pymysql.connect(
            host=host, user=user, password=password, database=database, port=port, charset=charset
        )
        self.cursor = self.conn.cursor()

    def query(self, sql):
        """执行 SQL 并返回所有结果（tuple 列表）"""
        self._ensure_connection()
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def query_df(self, sql, time_column=None, chunksize=None):
        """
        执行 SQL 并返回 DataFrame，支持分块读取

        Args:
            sql: SQL 查询语句
            time_column: 时间列名，如果为 None 则自动检测
            chunksize: 分块读取的大小
        """
        self._ensure_connection()
        # 使用 warnings 来抑制 pandas 的警告
        import warnings

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")
            df = pd.read_sql(sql, self.conn, chunksize=chunksize)

        # 如果返回的是生成器（分块读取），需要特殊处理
        if chunksize is not None:
            # 对于分块读取，返回生成器，每个块都需要处理
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

                chunk[time_col] = pd.to_datetime(chunk[time_col])
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

            df[time_col] = pd.to_datetime(df[time_col])
            df = df.set_index([time_col])
            return df

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

    def describe_table(self, table_name):
        """查看表结构"""
        self._ensure_connection()
        # 使用反引号包围表名，防止特殊字符（如连字符）导致SQL错误
        self.cursor.execute(f"DESCRIBE `{table_name}`;")
        return self.cursor.fetchall()

    def close(self):
        """关闭连接"""
        self.cursor.close()
        self.conn.close()

    @property
    def tables_prop(self):
        """每次访问都查询一次"""
        self._ensure_connection()
        self.cursor.execute("SHOW TABLES;")
        return [t[0] for t in self.cursor.fetchall()]

    @cached_property
    def tables(self):
        """首次访问查询并缓存，后续当普通属性使用"""
        self.cursor.execute("SHOW TABLES;")
        return [t[0] for t in self.cursor.fetchall()]

    def refresh_tables_cache(self):
        """失效 tables 缓存"""
        self.__dict__.pop("tables", None)


def plot_data(df_date=None, char="B_Temperature", fig=None, ax=None, **kwargs):
    """绘制单个慢控通道的时间序列（遗留辅助函数）。

    注意：之前版本把默认参数写成了 `df.loc[...]`，会在 import 时立即求值，
    如果作用域里没有 `df` 就会导致 `NameError`，进而让 `sc_reader` 无法导入。
    现在改为运行时显式传入 `df_date`。
    """
    # 局部导入，避免让数据库模块在导入阶段强依赖 matplotlib
    import matplotlib.pyplot as plt

    if df_date is None:
        raise ValueError("plot_data(df_date=...) 需要显式传入 df_date（DataFrame/Series），不再提供隐式默认值")

    if ax is None or fig is None:
        fig, ax = plt.subplots(figsize=(12, 5))

    plot_kwargs = {"alpha": 0.7, "linewidth": 1}
    plot_kwargs.update(kwargs)

    ax.plot(df_date.index, df_date[char], **plot_kwargs)
    fig.autofmt_xdate()  # 自动格式化x轴日期标签

    ax.set_ylabel(char, fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="x", rotation=30)
    return ax
