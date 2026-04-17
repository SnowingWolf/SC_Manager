"""
Dashboard 工具函数

提供 Dashboard 使用的通用工具函数。
"""

from typing import List, Optional

import pandas as pd
import plotly.graph_objects as go


def find_columns(cols: List[str], keywords: List[str]) -> List[str]:
    """根据关键词查找匹配的列名

    Args:
        cols: 列名列表
        keywords: 关键词列表（不区分大小写）

    Returns:
        匹配的列名列表
    """
    lowered = [(c, c.lower()) for c in cols]
    matched = [c for c, cl in lowered if any(k in cl for k in keywords)]
    return matched


def column_options(cols: List[str]) -> List[dict]:
    """将列名列表转换为 Dropdown 选项格式

    Args:
        cols: 列名列表

    Returns:
        Dropdown 选项列表 [{"label": col, "value": col}, ...]
    """
    return [{"label": c, "value": c} for c in cols]


def empty_fig(message: str) -> go.Figure:
    """创建带有消息的空白图表

    Args:
        message: 显示的消息

    Returns:
        Plotly Figure 对象
    """
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
    )
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.update_layout(template="plotly_white")
    return fig


def select_or_fallback(current: Optional[str], candidates: List[str]) -> Optional[str]:
    """选择当前值或回退到候选列表的第一个

    Args:
        current: 当前选中的值
        candidates: 候选值列表

    Returns:
        选中的值或 None
    """
    if current and current in candidates:
        return current
    return candidates[0] if candidates else None


def parse_load_hours(value: Optional[str]) -> Optional[float]:
    """解析加载时间窗口值

    Args:
        value: 字符串值（"all", "custom", 或数字）

    Returns:
        小时数或 None（表示全量加载）
    """
    if value is None or value in {"all", "custom"}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_custom_hours(value: Optional[float]) -> Optional[float]:
    """解析自定义小时数

    Args:
        value: 输入值

    Returns:
        有效的小时数或 None
    """
    if value is None:
        return None
    try:
        hours = float(value)
    except (TypeError, ValueError):
        return None
    return hours if hours > 0 else None


def format_range_value(value) -> str:
    """格式化时间范围值为字符串

    Args:
        value: 时间值

    Returns:
        格式化的字符串
    """
    if value is None:
        return "n/a"
    if isinstance(value, str):
        return value[:19]
    try:
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return "n/a"
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


def parse_int(value: Optional[str], default: int) -> int:
    """解析整数值

    Args:
        value: 字符串值
        default: 默认值

    Returns:
        解析的整数或默认值
    """
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# 加载窗口下拉选项
LOAD_WINDOW_OPTIONS = [
    {"label": "All", "value": "all"},
    {"label": "1 h", "value": "1"},
    {"label": "3 h", "value": "3"},
    {"label": "6 h", "value": "6"},
    {"label": "12 h", "value": "12"},
    {"label": "24 h", "value": "24"},
    {"label": "3 d", "value": "72"},
    {"label": "7 d", "value": "168"},
    {"label": "Custom", "value": "custom"},
]

# 时间序列图降采样选项
TS_MAX_POINTS_OPTIONS = [
    {"label": "2k", "value": "2000"},
    {"label": "5k", "value": "5000"},
    {"label": "8k", "value": "8000"},
    {"label": "12k", "value": "12000"},
    {"label": "20k", "value": "20000"},
]

# 相图降采样选项
PHASE_MAX_POINTS_OPTIONS = [
    {"label": "2k", "value": "2000"},
    {"label": "5k", "value": "5000"},
    {"label": "10k", "value": "10000"},
    {"label": "20k", "value": "20000"},
    {"label": "50k", "value": "50000"},
]
