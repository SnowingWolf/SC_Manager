"""
规格定义模块

定义 TableSpec 数据类，用于描述表结构。
"""
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class TableSpec:
    """
    表规格定义

    Attributes:
        table: 表名
        time_col: 时间列名，None 则自动检测
        cols: 要读取的列列表，None 表示读取所有列
        key_col: 主键列名，用于更精确的增量读取
        time_unit: 时间单位，用于 BIGINT 时间戳转换
            - None: DATETIME 类型（自动处理）
            - 'ms': 毫秒时间戳
            - 'us': 微秒时间戳
            - 's': 秒时间戳

    Examples:
        # DATETIME 类型，自动检测时间列
        >>> spec1 = TableSpec('temperature')

        # 指定时间列
        >>> spec2 = TableSpec('sensor', time_col='Time(s)')

        # BIGINT 毫秒时间戳
        >>> spec3 = TableSpec('events', time_col='ts', time_unit='ms')

        # 指定列和主键
        >>> spec4 = TableSpec(
        ...     table='pressure',
        ...     time_col='timestamp',
        ...     cols=['value', 'status'],
        ...     key_col='id'
        ... )
    """
    table: str
    time_col: Optional[str] = None  # None 表示自动检测
    cols: Optional[List[str]] = None
    key_col: Optional[str] = None
    time_unit: Optional[str] = None  # None/'ms'/'us'/'s'
