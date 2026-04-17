"""
时间对齐模块

使用时间索引的近似匹配实现多表时间对齐。

核心概念：
- Anchor 表：以该表的时间轴为基准
- asof 匹配：近似时间匹配，适用于不同采样率的数据
- tolerance：时间容差，超出范围的匹配填充 NaN
- direction：匹配方向（backward/forward/nearest）
"""
from typing import Dict, List, Union

import numpy as np
import pandas as pd
from numba import njit

from .spec import TableSpec


def _is_long_format(df: pd.DataFrame) -> bool:
    return "timestamp" in df.columns and "variable" in df.columns and "value" in df.columns


def _pivot_long_format(df: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" not in df.columns:
        df = df.reset_index()
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.pivot_table(
        index="timestamp",
        columns="variable",
        values="value",
        aggfunc="first",
    )


def _ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.index, pd.DatetimeIndex):
        return df
    time_col = "timestamp" if "timestamp" in df.columns else df.columns[0]
    if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
        df = df.copy()
        df[time_col] = pd.to_datetime(df[time_col])
    return df.set_index(time_col)


def _prepare_frame(df: pd.DataFrame, table_name: str, assume_normalized: bool = False) -> pd.DataFrame:
    """
    准备 DataFrame 用于对齐。

    Args:
        df: 输入 DataFrame
        table_name: 表名，用于列重命名
        assume_normalized: 假定输入已规范化（DatetimeIndex、已排序、无重复），跳过检查

    Returns:
        规范化的 DataFrame
    """
    if df.empty:
        return df
    if _is_long_format(df):
        df = _pivot_long_format(df)
    else:
        if assume_normalized:
            # Fast path: 假定已经是 DatetimeIndex 且已排序
            pass
        else:
            df = _ensure_datetime_index(df)
            if not df.index.is_monotonic_increasing:
                df = df.sort_index()
    if df.columns.size:
        rename_map = {col: f"{table_name}__{col}" for col in df.columns}
        df = df.rename(columns=rename_map)
    return df


@njit(cache=True)
def _nb_backward_indexer(left_ns: np.ndarray, right_ns: np.ndarray) -> np.ndarray:
    """返回 backward 匹配位置（right 中 <= left 的最后一个）。"""
    out = np.empty(left_ns.size, dtype=np.int64)
    out.fill(-1)

    j = 0
    n = right_ns.size
    for i in range(left_ns.size):
        lv = left_ns[i]
        while j < n and right_ns[j] <= lv:
            j += 1
        out[i] = j - 1
    return out


@njit(cache=True)
def _nb_forward_indexer(left_ns: np.ndarray, right_ns: np.ndarray) -> np.ndarray:
    """返回 forward 匹配位置（right 中 >= left 的第一个）。"""
    out = np.empty(left_ns.size, dtype=np.int64)
    out.fill(-1)

    j = 0
    n = right_ns.size
    for i in range(left_ns.size):
        lv = left_ns[i]
        while j < n and right_ns[j] < lv:
            j += 1
        if j < n:
            out[i] = j
    return out


@njit(cache=True)
def _nb_nearest_indexer(left_ns: np.ndarray, right_ns: np.ndarray) -> np.ndarray:
    """
    返回 nearest 匹配位置。
    等距时优先 backward（与 pandas.merge_asof(nearest) 一致）。
    """
    back = _nb_backward_indexer(left_ns, right_ns)
    fwd = _nb_forward_indexer(left_ns, right_ns)

    out = np.empty(left_ns.size, dtype=np.int64)
    out.fill(-1)
    for i in range(left_ns.size):
        b = back[i]
        f = fwd[i]
        if b >= 0 and f >= 0:
            db = left_ns[i] - right_ns[b]
            if db < 0:
                db = -db
            df = right_ns[f] - left_ns[i]
            if df < 0:
                df = -df
            if df < db:
                out[i] = f
            else:
                out[i] = b
        elif b >= 0:
            out[i] = b
        elif f >= 0:
            out[i] = f
    return out


@njit(cache=True)
def _nb_apply_tolerance(
    left_ns: np.ndarray, right_ns: np.ndarray, pos: np.ndarray, tol_ns: int
) -> np.ndarray:
    out = pos.copy()
    for i in range(out.size):
        p = out[i]
        if p >= 0:
            d = left_ns[i] - right_ns[p]
            if d < 0:
                d = -d
            if d > tol_ns:
                out[i] = -1
    return out


def _asof_indexer(
    left_index: pd.DatetimeIndex,
    right_index: pd.DatetimeIndex,
    direction: str,
    tolerance: Union[None, pd.Timedelta],
) -> np.ndarray:
    """
    线性时间复杂度的 asof 索引匹配器。

    Returns:
        每个 left 行对应的 right 行位置，未匹配为 -1
    """
    left_ns = left_index.asi8
    right_ns = right_index.asi8

    if right_ns.size == 0:
        return np.full(left_ns.size, -1, dtype=np.int64)

    if direction == "backward":
        pos = _nb_backward_indexer(left_ns, right_ns)
    elif direction == "forward":
        pos = _nb_forward_indexer(left_ns, right_ns)
    elif direction == "nearest":
        pos = _nb_nearest_indexer(left_ns, right_ns)
    else:
        raise ValueError("direction 必须是 'backward'、'forward' 或 'nearest'")

    if tolerance is None:
        return pos

    tol_ns = int(tolerance.value)
    if tol_ns < 0:
        raise ValueError("tolerance 必须 >= 0")

    return _nb_apply_tolerance(left_ns, right_ns, pos, tol_ns)


def _merge_asof_linear(
    left: pd.DataFrame,
    right: pd.DataFrame,
    direction: str,
    tolerance: Union[None, pd.Timedelta],
) -> pd.DataFrame:
    """用双指针线性匹配实现 DataFrame 的 asof 合并。"""
    if right.empty:
        filler = right.iloc[:0].reindex(left.index)
        return pd.concat([left, filler], axis=1)

    pos = _asof_indexer(left.index, right.index, direction=direction, tolerance=tolerance)
    matched = pos >= 0

    safe_pos = pos.copy()
    safe_pos[~matched] = 0

    # iloc 取行后对齐到 left 索引，未匹配行置 NaN
    taken = right.iloc[safe_pos].copy()
    taken.index = left.index
    if not matched.all():
        taken.loc[~matched, :] = np.nan

    return pd.concat([left, taken], axis=1)


def align_asof(
    frames: Union[Dict[str, pd.DataFrame], List[pd.DataFrame]],
    anchor: Union[str, int],
    tolerance: Union[str, pd.Timedelta] = '200ms',
    direction: str = 'backward',
    assume_normalized: bool = False
) -> pd.DataFrame:
    """
    使用 asof 规则对齐多个表的数据

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
        assume_normalized: 假定输入已规范化（DatetimeIndex、已排序），跳过检查以提升性能

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

    if direction not in {"backward", "forward", "nearest"}:
        raise ValueError("direction 必须是 'backward'、'forward' 或 'nearest'")

    # 解析 tolerance（None 表示不限制）
    if isinstance(tolerance, str):
        tolerance = pd.Timedelta(tolerance)

    # 准备 anchor 表
    result = _prepare_frame(anchor_df, anchor, assume_normalized=assume_normalized)

    # 对齐其他表
    for table_name, df in frames.items():
        if table_name == anchor or df.empty:
            continue
        right = _prepare_frame(df, table_name, assume_normalized=assume_normalized)

        result = _merge_asof_linear(
            result,
            right,
            direction=direction,
            tolerance=tolerance,
        )
    return result


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
