"""
时间对齐模块

使用 pandas.merge_asof 实现多表时间对齐。

核心概念：
- Anchor 表：以该表的时间轴为基准
- merge_asof：近似时间匹配，适用于不同采样率的数据
- tolerance：时间容差，超出范围的匹配填充 NaN
- direction：匹配方向（backward/forward/nearest）
"""
from typing import Dict, List, Union

import pandas as pd

from .spec import TableSpec


def align_asof(
    frames: Union[Dict[str, pd.DataFrame], List[pd.DataFrame]],
    anchor: Union[str, int],
    tolerance: Union[str, pd.Timedelta] = '200ms',
    direction: str = 'backward'
) -> pd.DataFrame:
    """
    使用 merge_asof 对齐多个表的数据

    以 anchor 表为时间基准，将其他表的数据按时间匹配合并。
    非 anchor 表的列自动添加 {table}__ 前缀避免冲突。

    Args:
        frames: {表名: DataFrame} 字典或 DataFrame 列表，DataFrame 应以时间为索引
        anchor: 锚表名（字符串）或索引（整数），以该表时间轴为基准
        tolerance: 时间容差，如 '200ms', '1s' 或 pd.Timedelta
        direction: 对齐方向
            - 'backward': 向后查找（默认，找历史最近的）
            - 'forward': 向前查找
            - 'nearest': 最近的（任意方向）

    Returns:
        合并后的宽表 DataFrame，索引为 anchor 表的时间

    Examples:
        >>> frames = {
        ...     'temp': df_temp,
        ...     'pressure': df_press,
        ... }
        >>> aligned = align_asof(frames, anchor='temp', tolerance='200ms')
    """
    # 处理列表输入：将列表转换为字典
    if isinstance(frames, list):
        # 如果 anchor 是整数索引，转换为对应的键
        if isinstance(anchor, int):
            if anchor < 0 or anchor >= len(frames):
                raise ValueError(f"anchor 索引 {anchor} 超出范围 [0, {len(frames)-1}]")
            # 创建临时字典，使用索引作为键名
            frames = {f'table_{i}': df for i, df in enumerate(frames)}
            anchor = f'table_{anchor}'
        else:
            # anchor 是字符串，但 frames 是列表，需要转换
            frames = {f'table_{i}': df for i, df in enumerate(frames)}
            # 如果 anchor 不在转换后的键中，尝试作为索引处理
            if anchor not in frames:
                try:
                    anchor_idx = int(anchor)
                    if 0 <= anchor_idx < len(frames):
                        anchor = f'table_{anchor_idx}'
                except ValueError:
                    pass
    
    if anchor not in frames:
        raise ValueError(f"anchor '{anchor}' 不在 frames 中: {list(frames.keys())}")

    anchor_df = frames[anchor]
    if anchor_df.empty:
        return pd.DataFrame()

    # 解析 tolerance
    if isinstance(tolerance, str):
        tolerance = pd.Timedelta(tolerance)

    # 准备 anchor 表
    result = anchor_df.reset_index()
    time_col = result.columns[0]
    
    # 检查是否是长表格式（有 timestamp, variable, value 列）
    if "timestamp" in result.columns and "variable" in result.columns and "value" in result.columns:
        # 长表格式：转换为宽表格式（pivot）
        time_col = "timestamp"
        result = result.pivot_table(
            index=time_col,
            columns="variable",
            values="value",
            aggfunc='first'  # 如果有重复，取第一个
        ).reset_index()
        time_col = result.columns[0]
    
    result[time_col] = pd.to_datetime(result[time_col])

    # 为 anchor 表列添加前缀
    rename_map = {col: f'{anchor}__{col}' for col in result.columns if col != time_col}
    result = result.rename(columns=rename_map)

    # 对齐其他表
    for table_name, df in frames.items():
        if table_name == anchor or df.empty:
            continue

        right = df.reset_index()
        right_time_col = right.columns[0]
        
        # 检查是否是长表格式
        if "timestamp" in right.columns and "variable" in right.columns and "value" in right.columns:
            # 转换为宽表格式
            right_time_col = "timestamp"
            right = right.pivot_table(
                index=right_time_col,
                columns="variable",
                values="value",
                aggfunc='first'
            ).reset_index()
            right_time_col = right.columns[0]
        
        right[right_time_col] = pd.to_datetime(right[right_time_col])

        # 添加前缀
        right_rename = {col: f'{table_name}__{col}' for col in right.columns if col != right_time_col}
        right = right.rename(columns=right_rename)

        # 排序（merge_asof 要求）
        result = result.sort_values(time_col)
        right = right.sort_values(right_time_col)

        # merge_asof
        result = pd.merge_asof(
            result,
            right,
            left_on=time_col,
            right_on=right_time_col,
            direction=direction,
            tolerance=tolerance
        )

        # 删除右表时间列
        if right_time_col in result.columns and right_time_col != time_col:
            result = result.drop(columns=[right_time_col])

    return result.set_index(time_col)


def collect_and_align(
    reader,  # SCReader 实例
    specs: List[TableSpec],
    anchor: str,
    tolerance: str = '200ms',
    direction: str = 'backward',
    lookback: str = '2s'
) -> pd.DataFrame:
    """
    一次性收集多表数据并对齐

    便捷函数，组合 reader.read_multiple 和 align_asof。

    Args:
        reader: SCReader 实例
        specs: 表规格列表
        anchor: 锚表名
        tolerance: 时间容差
        direction: 对齐方向
        lookback: 回看窗口

    Returns:
        对齐后的宽表 DataFrame

    Examples:
        >>> from sc_reader import SCReader, TableSpec, collect_and_align
        >>>
        >>> reader = SCReader(state_path='./watermark.json')
        >>> specs = [
        ...     TableSpec('temperature', 'Time(s)'),
        ...     TableSpec('pressure', 'Time(s)'),
        ... ]
        >>> df = collect_and_align(reader, specs, anchor='temperature')
    """
    frames = reader.read_multiple(specs, lookback=lookback)
    return align_asof(frames, anchor=anchor, tolerance=tolerance, direction=direction)
