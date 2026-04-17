"""
通用气体 P-T 相图模块

提供氩气和氙气的物性常量、相边界计算函数，以及通用的 P-T 相图绘制功能。
支持 Plotly 和 Matplotlib 两种后端。
"""

from typing import List, Optional, Tuple, Union

import numpy as np

try:
    import plotly.graph_objects as go
    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False
    go = None

try:
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyArrowPatch
    _MATPLOTLIB_AVAILABLE = True
except ImportError:
    _MATPLOTLIB_AVAILABLE = False
    plt = None
    FancyArrowPatch = None

# 默认颜色方案
DEFAULT_COLORS = [
    'orange', 'dodgerblue', 'limegreen', 'crimson', 'purple',
    'gold', 'deepskyblue', 'coral', 'mediumseagreen', 'orchid'
]

# =============================================================================
# 气体物性常量
# =============================================================================

GAS_PROPERTIES = {
    'argon': {
        'name': 'Argon',
        'T_triple': 83.8058,      # K
        'P_triple_bar': 0.68891,  # bar
        'T_crit': 150.687,        # K
        'P_crit_bar': 48.63,      # bar
        # 升华曲线参数
        'sub_a1': -11.391604,
        'sub_a2': -0.39513431,
        # 饱和曲线参数
        'sat_a1': -5.9409785,
        'sat_a2': 1.3553888,
        'sat_a3': -0.46497607,
        'sat_a4': -1.5399043,
    },
    'xenon': {
        'name': 'Xenon',
        'T_triple': 161.4,        # K
        'P_triple_bar': 0.8166,   # bar
        'T_crit': 289.733,        # K
        'P_crit_bar': 58.42,      # bar
        # 简化参数（需要更精确的数据）
        'sub_a1': -11.5,
        'sub_a2': -0.4,
        'sat_a1': -6.0,
        'sat_a2': 1.4,
        'sat_a3': -0.5,
        'sat_a4': -1.6,
    }
}


def psub_bar(T_K: Union[float, np.ndarray], gas: str = 'argon') -> Union[float, np.ndarray]:
    """
    计算气体升华压力（固-气平衡线）

    Args:
        T_K: 温度 (K)
        gas: 气体类型 ('argon' 或 'xenon')

    Returns:
        升华压力 (bar)
    """
    props = GAS_PROPERTIES[gas]
    T_K = np.asarray(T_K)
    theta = T_K / props['T_triple']
    tau = 1 - theta

    exponent = (1 / theta) * (
        props['sub_a1'] * tau + props['sub_a2'] * tau**1.5
    )

    return props['P_triple_bar'] * np.exp(exponent)


def psat_bar(T_K: Union[float, np.ndarray], gas: str = 'argon') -> Union[float, np.ndarray]:
    """
    计算气体饱和蒸气压（液-气平衡线）

    Args:
        T_K: 温度 (K)
        gas: 气体类型 ('argon' 或 'xenon')

    Returns:
        饱和蒸气压 (bar)
    """
    props = GAS_PROPERTIES[gas]
    T_K = np.asarray(T_K)
    theta = 1 - T_K / props['T_crit']

    exponent = (props['T_crit'] / T_K) * (
        props['sat_a1'] * theta +
        props['sat_a2'] * theta**1.5 +
        props['sat_a3'] * theta**2.0 +
        props['sat_a4'] * theta**4.5
    )

    return props['P_crit_bar'] * np.exp(exponent)


def phase_boundary_bar(
    T_K: Union[float, np.ndarray],
    gas: str = 'argon'
) -> Union[float, np.ndarray]:
    """
    计算气体相边界压力（自动选择升华或饱和曲线）

    Args:
        T_K: 温度 (K)
        gas: 气体类型 ('argon' 或 'xenon')

    Returns:
        相边界压力 (bar)
    """
    props = GAS_PROPERTIES[gas]
    T_K = np.asarray(T_K)
    scalar_input = T_K.ndim == 0
    T_K = np.atleast_1d(T_K)

    result = np.full_like(T_K, np.nan, dtype=float)

    # 升华区域
    mask_sub = T_K <= props['T_triple']
    if np.any(mask_sub):
        result[mask_sub] = psub_bar(T_K[mask_sub], gas)

    # 饱和区域
    mask_sat = (T_K > props['T_triple']) & (T_K <= props['T_crit'])
    if np.any(mask_sat):
        result[mask_sat] = psat_bar(T_K[mask_sat], gas)

    if scalar_input:
        return float(result[0])
    return result


def get_phase(T_K: float, P_bar: float, gas: str = 'argon') -> str:
    """
    判断给定 (T, P) 点所处的相态

    Args:
        T_K: 温度 (K)
        P_bar: 压力 (bar)
        gas: 气体类型 ('argon' 或 'xenon')

    Returns:
        相态字符串: 'solid', 'liquid', 'gas', 或 'supercritical'
    """
    props = GAS_PROPERTIES[gas]

    if T_K > props['T_crit'] and P_bar > props['P_crit_bar']:
        return 'supercritical'

    if T_K <= props['T_triple']:
        P_boundary = psub_bar(T_K, gas)
        return 'solid' if P_bar > P_boundary else 'gas'
    elif T_K <= props['T_crit']:
        P_boundary = psat_bar(T_K, gas)
        return 'liquid' if P_bar > P_boundary else 'gas'
    else:
        return 'gas' if P_bar < props['P_crit_bar'] else 'supercritical'


def plot_pt_path(
    P_bar: Union[float, np.ndarray, List[np.ndarray]],
    T_K: Union[float, np.ndarray, List[np.ndarray]],
    *,
    gas: str = 'argon',
    kind: str = 'plotly',
    T_range: Optional[Tuple[float, float]] = None,
    P_range: Optional[Tuple[float, float]] = None,
    fill_regions: bool = True,
    draw_boundary: bool = True,
    arrow_every: int = 8,
    arrow_max: int = 12,
    arrow_min_dist: float = 0.015,
    downsample_max_points: Optional[int] = None,
    boundary_points: Optional[int] = None,
    title: Optional[str] = None,
    labels: Optional[List[str]] = None,
    colors: Optional[List[str]] = None,
    fig: Optional['go.Figure'] = None,
    ax: Optional['plt.Axes'] = None,
    show: bool = False,
) -> Union['go.Figure', 'plt.Axes']:
    """
    绘制 P-T 相图，支持单路径或多路径

    Args:
        P_bar: 压力数据 (bar)，可以是标量、1D 数组或数组列表
        T_K: 温度数据 (K)，结构需与 P_bar 匹配
        gas: 气体类型 ('argon' 或 'xenon')
        kind: 绘图后端 ('plotly' 或 'matplotlib')
        T_range: 温度显示范围 (K)
        P_range: 压力显示范围 (bar)
        fill_regions: 是否填充相区域
        draw_boundary: 是否绘制相边界线
        arrow_every: 每 N 个点绘制一个箭头
        arrow_max: 每条路径最大箭头数
        arrow_min_dist: 归一化位移阈值，低于此值不绘制箭头
        downsample_max_points: Plotly 降采样最大点数
        boundary_points: 相边界采样点数
        title: 图表标题
        labels: 路径标签列表
        colors: 路径颜色列表
        fig: 现有 Plotly Figure
        ax: 现有 Matplotlib Axes
        show: 是否自动显示图表

    Returns:
        kind='plotly': go.Figure
        kind='matplotlib': plt.Axes
    """
    # 导入对应后端的实现
    if kind == 'plotly':
        from .argon_phase import plot_argon_pt_path

        # 规范化为列表格式
        if not isinstance(P_bar, list):
            P_bar = [np.atleast_1d(P_bar)]
            T_K = [np.atleast_1d(T_K)]

        # 设置默认范围
        if T_range is None:
            props = GAS_PROPERTIES[gas]
            T_range = (props['T_triple'] - 5, props['T_triple'] + 20)
        if P_range is None:
            P_range = (0.0, 3.0)

        # 设置默认标签和颜色
        n_paths = len(P_bar)
        if labels is None:
            labels = [f'Path {i+1}' for i in range(n_paths)]
        if colors is None:
            colors = [DEFAULT_COLORS[i % len(DEFAULT_COLORS)] for i in range(n_paths)]

        # 调用 argon_phase 的实现（目前只支持 argon）
        if gas != 'argon':
            raise NotImplementedError(f"Plotly backend currently only supports argon, got {gas}")

        # 为多路径绘制创建图表
        if fig is None:
            fig = go.Figure()

        # 绘制每条路径
        for i, (P, T) in enumerate(zip(P_bar, T_K)):
            # 只在第一条路径时绘制相区域和边界
            _fill = fill_regions if i == 0 else False
            _boundary = draw_boundary if i == 0 else False

            fig = plot_argon_pt_path(
                P, T,
                T_range=T_range,
                P_range=P_range,
                fill_regions=_fill,
                draw_boundary=_boundary,
                arrow_every=arrow_every,
                arrow_max=arrow_max,
                arrow_min_dist=arrow_min_dist,
                downsample_max_points=downsample_max_points,
                boundary_points=boundary_points or 100,
                title=title or f"{gas.capitalize()} P-T Phase Diagram",
                path_color=colors[i],
                path_label=labels[i],
                fig=fig,
                show=False,
            )

        if show:
            fig.show()

        return fig

    elif kind == 'matplotlib':
        raise NotImplementedError("Matplotlib backend not yet implemented")

    else:
        raise ValueError(f"Unknown kind: {kind}. Must be 'plotly' or 'matplotlib'")
