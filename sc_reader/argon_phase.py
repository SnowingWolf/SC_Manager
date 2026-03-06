"""
氩气 P-T 相图绘制模块

提供氩气物性常量和相边界计算函数，以及 P-T 相图绘制功能。
使用 Plotly 作为绑图后端。

References:
    - NIST Chemistry WebBook: https://webbook.nist.gov/cgi/cbook.cgi?ID=C7440371
    - Tegeler, Ch., Span, R., and Wagner, W. (1999). A New Equation of State for Argon
"""

from typing import Optional, Union

import numpy as np

try:
    import plotly.graph_objects as go
    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False
    go = None

# =============================================================================
# 氩气物性常量
# =============================================================================

ARGON_T_TRIPLE = 83.8058  # K, 三相点温度
ARGON_P_TRIPLE_BAR = 0.68891  # bar, 三相点压力
ARGON_T_CRIT = 150.687  # K, 临界点温度
ARGON_P_CRIT_BAR = 48.63  # bar, 临界点压力

# 升华曲线拟合参数 (Tegeler et al., 1999)
_ARGON_SUBLIMATION_A1 = -11.391604
_ARGON_SUBLIMATION_A2 = -0.39513431

# 饱和蒸气压曲线拟合参数 (Tegeler et al., 1999)
_ARGON_SATURATION_A1 = -5.9409785
_ARGON_SATURATION_A2 = 1.3553888
_ARGON_SATURATION_A3 = -0.46497607
_ARGON_SATURATION_A4 = -1.5399043


# =============================================================================
# 相边界计算函数
# =============================================================================

def argon_psub_bar(T_K: Union[float, np.ndarray]) -> Union[float, np.ndarray]:
    """
    计算氩气升华压力（固-气平衡线）

    使用 Tegeler et al. (1999) 的拟合公式。
    仅在 T <= T_triple 时有效。

    Args:
        T_K: 温度 (K)，标量或数组

    Returns:
        升华压力 (bar)

    Examples:
        >>> argon_psub_bar(80.0)
        0.4299...
        >>> argon_psub_bar(np.array([75, 80, 83.8]))
        array([0.189..., 0.429..., 0.688...])
    """
    T_K = np.asarray(T_K)
    theta = T_K / ARGON_T_TRIPLE

    # ln(P/P_t) = (T_t/T) * [A1*(1 - T/T_t) + A2*(1 - T/T_t)^1.5]
    tau = 1 - theta  # 1 - T/T_triple
    exponent = (1 / theta) * (
        _ARGON_SUBLIMATION_A1 * tau
        + _ARGON_SUBLIMATION_A2 * tau**1.5
    )

    return ARGON_P_TRIPLE_BAR * np.exp(exponent)


def argon_psat_bar(T_K: Union[float, np.ndarray]) -> Union[float, np.ndarray]:
    """
    计算氩气饱和蒸气压（液-气平衡线）

    使用 Tegeler et al. (1999) 的拟合公式。
    仅在 T_triple <= T <= T_crit 时有效。

    Args:
        T_K: 温度 (K)，标量或数组

    Returns:
        饱和蒸气压 (bar)

    Examples:
        >>> argon_psat_bar(90.0)
        1.339...
        >>> argon_psat_bar(np.array([85, 90, 100]))
        array([0.877..., 1.339..., 3.238...])
    """
    T_K = np.asarray(T_K)
    theta = 1 - T_K / ARGON_T_CRIT

    exponent = (ARGON_T_CRIT / T_K) * (
        _ARGON_SATURATION_A1 * theta
        + _ARGON_SATURATION_A2 * theta**1.5
        + _ARGON_SATURATION_A3 * theta**2.0
        + _ARGON_SATURATION_A4 * theta**4.5
    )

    return ARGON_P_CRIT_BAR * np.exp(exponent)


def argon_phase_boundary_bar(
    T_K: Union[float, np.ndarray]
) -> Union[float, np.ndarray]:
    """
    计算氩气相边界压力（自动选择升华或饱和曲线）

    - T <= T_triple: 返回升华压力
    - T_triple < T <= T_crit: 返回饱和蒸气压
    - T > T_crit: 返回 NaN（超临界区域无相边界）

    Args:
        T_K: 温度 (K)，标量或数组

    Returns:
        相边界压力 (bar)

    Examples:
        >>> argon_phase_boundary_bar(80.0)  # 升华区
        0.4299...
        >>> argon_phase_boundary_bar(90.0)  # 饱和区
        1.339...
        >>> argon_phase_boundary_bar(np.array([80, 90, 160]))
        array([0.429..., 1.339..., nan])
    """
    T_K = np.asarray(T_K)
    scalar_input = T_K.ndim == 0
    T_K = np.atleast_1d(T_K)

    result = np.full_like(T_K, np.nan, dtype=float)

    # 升华区域
    mask_sub = T_K <= ARGON_T_TRIPLE
    if np.any(mask_sub):
        result[mask_sub] = argon_psub_bar(T_K[mask_sub])

    # 饱和区域
    mask_sat = (T_K > ARGON_T_TRIPLE) & (T_K <= ARGON_T_CRIT)
    if np.any(mask_sat):
        result[mask_sat] = argon_psat_bar(T_K[mask_sat])

    if scalar_input:
        return float(result[0])
    return result


def _get_phase(T_K: float, P_bar: float) -> str:
    """
    判断给定 (T, P) 点所处的相态

    Args:
        T_K: 温度 (K)
        P_bar: 压力 (bar)

    Returns:
        相态字符串: 'solid', 'liquid', 'gas', 或 'supercritical'
    """
    if T_K > ARGON_T_CRIT and P_bar > ARGON_P_CRIT_BAR:
        return 'supercritical'

    if T_K <= ARGON_T_TRIPLE:
        # 固-气区域
        P_boundary = argon_psub_bar(T_K)
        return 'solid' if P_bar > P_boundary else 'gas'
    elif T_K <= ARGON_T_CRIT:
        # 液-气区域
        P_boundary = argon_psat_bar(T_K)
        return 'liquid' if P_bar > P_boundary else 'gas'
    else:
        # T > T_crit
        return 'gas' if P_bar < ARGON_P_CRIT_BAR else 'supercritical'


# =============================================================================
# 绑图函数
# =============================================================================

def plot_argon_pt_path(
    P_bar: Union[float, np.ndarray],
    T_K: Union[float, np.ndarray],
    *,
    T_range: tuple = (80.0, 100.0),
    P_range: tuple = (0.0, 3.0),
    fill_regions: bool = True,
    draw_boundary: bool = True,
    arrow_every: int = 8,
    boundary_points: int = 500,
    title: Optional[str] = None,
    fig: Optional['go.Figure'] = None,
    show: bool = False,
) -> 'go.Figure':
    """
    绑制氩气 P-T 相图，并在图上标注给定的 (T, P) 路径

    Args:
        P_bar: 压力数据 (bar)，标量或数组
        T_K: 温度数据 (K)，标量或数组
        T_range: 温度显示范围 (K)，默认 (80, 100)
        P_range: 压力显示范围 (bar)，默认 (0, 3)
        fill_regions: 是否填充相区域颜色，默认 True
        draw_boundary: 是否绘制相边界线，默认 True
        arrow_every: 每隔多少个点绘制一个箭头，默认 8（设为 0 禁用箭头）
        boundary_points: 相边界线的采样点数，默认 500
        title: 图表标题，默认 "Argon P-T Phase Diagram"
        fig: 已有的 Plotly Figure 对象（可选）
        show: 是否自动显示图表，默认 False

    Returns:
        Plotly Figure 对象

    Raises:
        ImportError: 如果 Plotly 不可用

    Examples:
        >>> # 单点绘图
        >>> fig = plot_argon_pt_path(P_bar=1.0, T_K=90.0)
        >>> fig.show()

        >>> # 路径绘图
        >>> import numpy as np
        >>> T = np.linspace(80, 100, 60)
        >>> P = 0.4 + 0.02 * (T - 80) + 0.25 * np.sin(np.linspace(0, 2*np.pi, T.size))
        >>> fig = plot_argon_pt_path(P, T, arrow_every=8)
        >>> fig.show()
    """
    if not _PLOTLY_AVAILABLE:
        raise ImportError(
            "Plotly 不可用。请安装 plotly: pip install plotly"
        )

    # 转换输入为数组
    P_bar = np.atleast_1d(np.asarray(P_bar, dtype=float))
    T_K = np.atleast_1d(np.asarray(T_K, dtype=float))

    if len(P_bar) != len(T_K):
        raise ValueError(
            f"P_bar 和 T_K 长度不一致: {len(P_bar)} vs {len(T_K)}"
        )

    # 创建 Figure
    if fig is None:
        fig = go.Figure()

    T_min, T_max = T_range
    P_min, P_max = P_range

    # -------------------------------------------------------------------------
    # 1. 填充相区域
    # -------------------------------------------------------------------------
    if fill_regions:
        # 生成相边界数据
        T_sub = np.linspace(T_min, min(ARGON_T_TRIPLE, T_max), boundary_points // 2)
        P_sub = argon_psub_bar(T_sub)

        T_sat_start = max(ARGON_T_TRIPLE, T_min)
        T_sat_end = min(ARGON_T_CRIT, T_max)
        if T_sat_start < T_sat_end:
            T_sat = np.linspace(T_sat_start, T_sat_end, boundary_points // 2)
            P_sat = argon_psat_bar(T_sat)
        else:
            T_sat = np.array([])
            P_sat = np.array([])

        # 气相区域（边界线以下）
        # 构建气相区域的多边形
        gas_T = []
        gas_P = []

        # 从左下角开始，沿底边到右下角
        gas_T.extend([T_min, T_max])
        gas_P.extend([P_min, P_min])

        # 沿右边向上到边界线（如果在范围内）
        if T_max <= ARGON_T_TRIPLE:
            P_boundary_right = argon_psub_bar(T_max)
        elif T_max <= ARGON_T_CRIT:
            P_boundary_right = argon_psat_bar(T_max)
        else:
            P_boundary_right = P_max

        gas_T.append(T_max)
        gas_P.append(min(P_boundary_right, P_max))

        # 沿边界线向左
        if len(T_sat) > 0:
            for t, p in zip(reversed(T_sat), reversed(P_sat)):
                if p <= P_max:
                    gas_T.append(t)
                    gas_P.append(p)

        # 三相点
        if T_min <= ARGON_T_TRIPLE <= T_max and ARGON_P_TRIPLE_BAR <= P_max:
            gas_T.append(ARGON_T_TRIPLE)
            gas_P.append(ARGON_P_TRIPLE_BAR)

        # 沿升华线向左
        for t, p in zip(reversed(T_sub), reversed(P_sub)):
            if p <= P_max:
                gas_T.append(t)
                gas_P.append(p)

        # 回到左下角
        gas_T.append(T_min)
        gas_P.append(P_min)

        fig.add_trace(go.Scatter(
            x=gas_T,
            y=gas_P,
            fill='toself',
            fillcolor='rgba(173, 216, 230, 0.4)',  # 浅蓝色
            line=dict(width=0),
            name='Gas',
            hoverinfo='name',
            showlegend=True,
        ))

        # 固相区域（升华线以上，三相点温度以下）
        if T_min < ARGON_T_TRIPLE:
            solid_T = [T_min]
            solid_P = [P_max]

            # 沿顶边到三相点温度
            solid_T.append(min(ARGON_T_TRIPLE, T_max))
            solid_P.append(P_max)

            # 沿三相点温度向下到三相点
            if ARGON_P_TRIPLE_BAR <= P_max:
                solid_T.append(ARGON_T_TRIPLE)
                solid_P.append(ARGON_P_TRIPLE_BAR)

            # 沿升华线向左
            for t, p in zip(reversed(T_sub), reversed(P_sub)):
                if p <= P_max:
                    solid_T.append(t)
                    solid_P.append(p)

            # 回到左上角
            solid_T.append(T_min)
            solid_P.append(P_max)

            fig.add_trace(go.Scatter(
                x=solid_T,
                y=solid_P,
                fill='toself',
                fillcolor='rgba(144, 238, 144, 0.4)',  # 浅绿色
                line=dict(width=0),
                name='Solid',
                hoverinfo='name',
                showlegend=True,
            ))

        # 液相区域（饱和线以上，三相点温度以上，临界点以下）
        if ARGON_T_TRIPLE < T_max and len(T_sat) > 0:
            liquid_T = []
            liquid_P = []

            # 从三相点开始
            if ARGON_P_TRIPLE_BAR <= P_max:
                liquid_T.append(ARGON_T_TRIPLE)
                liquid_P.append(ARGON_P_TRIPLE_BAR)

            # 沿饱和线向右
            for t, p in zip(T_sat, P_sat):
                if p <= P_max:
                    liquid_T.append(t)
                    liquid_P.append(p)

            # 向上到顶边
            if len(liquid_T) > 0:
                liquid_T.append(liquid_T[-1])
                liquid_P.append(P_max)

            # 沿顶边向左到三相点温度
            liquid_T.append(max(ARGON_T_TRIPLE, T_min))
            liquid_P.append(P_max)

            # 回到三相点
            if ARGON_P_TRIPLE_BAR <= P_max:
                liquid_T.append(ARGON_T_TRIPLE)
                liquid_P.append(ARGON_P_TRIPLE_BAR)

            if len(liquid_T) >= 3:
                fig.add_trace(go.Scatter(
                    x=liquid_T,
                    y=liquid_P,
                    fill='toself',
                    fillcolor='rgba(255, 182, 193, 0.4)',  # 浅粉色
                    line=dict(width=0),
                    name='Liquid',
                    hoverinfo='name',
                    showlegend=True,
                ))

    # -------------------------------------------------------------------------
    # 2. 绘制相边界线
    # -------------------------------------------------------------------------
    if draw_boundary:
        # 升华线
        T_sub_line = np.linspace(T_min, min(ARGON_T_TRIPLE, T_max), boundary_points // 2)
        P_sub_line = argon_psub_bar(T_sub_line)
        mask_sub = P_sub_line <= P_max

        if np.any(mask_sub):
            fig.add_trace(go.Scatter(
                x=T_sub_line[mask_sub],
                y=P_sub_line[mask_sub],
                mode='lines',
                line=dict(color='darkgreen', width=2),
                name='Sublimation',
                hovertemplate='T: %{x:.2f} K<br>P: %{y:.4f} bar<extra>Sublimation</extra>',
            ))

        # 饱和线
        T_sat_start = max(ARGON_T_TRIPLE, T_min)
        T_sat_end = min(ARGON_T_CRIT, T_max)
        if T_sat_start < T_sat_end:
            T_sat_line = np.linspace(T_sat_start, T_sat_end, boundary_points // 2)
            P_sat_line = argon_psat_bar(T_sat_line)
            mask_sat = P_sat_line <= P_max

            if np.any(mask_sat):
                fig.add_trace(go.Scatter(
                    x=T_sat_line[mask_sat],
                    y=P_sat_line[mask_sat],
                    mode='lines',
                    line=dict(color='darkblue', width=2),
                    name='Saturation',
                    hovertemplate='T: %{x:.2f} K<br>P: %{y:.4f} bar<extra>Saturation</extra>',
                ))

        # 三相点标记
        if (T_min <= ARGON_T_TRIPLE <= T_max and
            P_min <= ARGON_P_TRIPLE_BAR <= P_max):
            fig.add_trace(go.Scatter(
                x=[ARGON_T_TRIPLE],
                y=[ARGON_P_TRIPLE_BAR],
                mode='markers',
                marker=dict(
                    size=12,
                    color='red',
                    symbol='star',
                    line=dict(width=1, color='darkred'),
                ),
                name='Triple Point',
                hovertemplate=(
                    f'Triple Point<br>'
                    f'T: {ARGON_T_TRIPLE:.4f} K<br>'
                    f'P: {ARGON_P_TRIPLE_BAR:.5f} bar<extra></extra>'
                ),
            ))

    # -------------------------------------------------------------------------
    # 3. 绘制数据路径
    # -------------------------------------------------------------------------
    n_points = len(T_K)

    if n_points == 1:
        # 单点：绘制标记
        phase = _get_phase(T_K[0], P_bar[0])
        fig.add_trace(go.Scatter(
            x=T_K,
            y=P_bar,
            mode='markers',
            marker=dict(
                size=14,
                color='orange',
                symbol='circle',
                line=dict(width=2, color='darkorange'),
            ),
            name=f'Data Point ({phase})',
            hovertemplate=(
                f'T: %{{x:.2f}} K<br>'
                f'P: %{{y:.4f}} bar<br>'
                f'Phase: {phase}<extra></extra>'
            ),
        ))
    else:
        # 多点：绘制路径
        fig.add_trace(go.Scatter(
            x=T_K,
            y=P_bar,
            mode='lines',
            line=dict(color='orange', width=2.5),
            name='Path',
            hovertemplate='T: %{x:.2f} K<br>P: %{y:.4f} bar<extra>Path</extra>',
        ))

        # 起点和终点标记
        fig.add_trace(go.Scatter(
            x=[T_K[0]],
            y=[P_bar[0]],
            mode='markers',
            marker=dict(size=10, color='green', symbol='circle'),
            name='Start',
            hovertemplate=f'Start<br>T: %{{x:.2f}} K<br>P: %{{y:.4f}} bar<extra></extra>',
        ))

        fig.add_trace(go.Scatter(
            x=[T_K[-1]],
            y=[P_bar[-1]],
            mode='markers',
            marker=dict(size=10, color='red', symbol='square'),
            name='End',
            hovertemplate=f'End<br>T: %{{x:.2f}} K<br>P: %{{y:.4f}} bar<extra></extra>',
        ))

        # 箭头标注（使用 annotation）
        if arrow_every > 0 and n_points > arrow_every:
            for i in range(arrow_every, n_points - 1, arrow_every):
                # 计算箭头方向
                dx = T_K[i] - T_K[i - 1]
                dy = P_bar[i] - P_bar[i - 1]

                # 添加箭头注释
                fig.add_annotation(
                    x=T_K[i],
                    y=P_bar[i],
                    ax=T_K[i] - dx * 0.3,
                    ay=P_bar[i] - dy * 0.3,
                    xref='x',
                    yref='y',
                    axref='x',
                    ayref='y',
                    showarrow=True,
                    arrowhead=2,
                    arrowsize=1.5,
                    arrowwidth=2,
                    arrowcolor='darkorange',
                )

    # -------------------------------------------------------------------------
    # 4. 布局设置
    # -------------------------------------------------------------------------
    fig.update_layout(
        title=title or 'Argon P-T Phase Diagram',
        xaxis=dict(
            title='Temperature (K)',
            range=[T_min, T_max],
            showgrid=True,
            gridcolor='lightgray',
        ),
        yaxis=dict(
            title='Pressure (bar)',
            range=[P_min, P_max],
            showgrid=True,
            gridcolor='lightgray',
        ),
        template='plotly_white',
        hovermode='closest',
        legend=dict(
            yanchor='top',
            y=0.99,
            xanchor='left',
            x=0.01,
            bgcolor='rgba(255, 255, 255, 0.8)',
        ),
        width=800,
        height=600,
    )

    if show:
        fig.show()

    return fig
