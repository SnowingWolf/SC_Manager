"""
P-T Phase Diagram Plotting Module

Supports phase properties and boundary calculations for multiple gases (argon, xenon),
and provides P-T phase diagram plotting. Supports both Plotly interactive plots and
Matplotlib static plots.

References:
    - NIST Chemistry WebBook
    - Tegeler, Ch., Span, R., and Wagner, W. (1999). A New Equation of State for Argon
    - Lemmon, E.W. and Span, R. (2006). Short Fundamental Equations of State for 20 Industrial Fluids
"""

from typing import List, Optional, Tuple, Union

import numpy as np

# Default color palette for multiple paths
DEFAULT_COLORS = [
    "orange",
    "dodgerblue",
    "limegreen",
    "crimson",
    "purple",
    "gold",
    "cyan",
    "magenta",
    "brown",
    "teal",
]

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

# =============================================================================
# Gas property constants
# =============================================================================

GAS_PROPERTIES = {
    "argon": {
        "name": "Argon",
        "T_triple": 83.8058,  # K
        "P_triple_bar": 0.68891,  # bar
        "T_crit": 150.687,  # K
        "P_crit_bar": 48.63,  # bar
        "sublimation": {"A1": -11.391604, "A2": -0.39513431},
        "saturation": {"A1": -5.9409785, "A2": 1.3553888, "A3": -0.46497607, "A4": -1.5399043},
    },
    "xenon": {
        "name": "Xenon",
        "T_triple": 161.405,  # K
        "P_triple_bar": 0.81600,  # bar
        "T_crit": 289.733,  # K
        "P_crit_bar": 58.40,  # bar
        "sublimation": {"A1": -11.866, "A2": -0.3878},  # Lemmon & Span (2006)
        "saturation": {"A1": -6.0177, "A2": 1.4053, "A3": -0.4684, "A4": -1.8570},
    },
}


# =============================================================================
# Phase boundary calculation functions
# =============================================================================


def psub_bar(T_K: Union[float, np.ndarray], gas: str = "argon") -> Union[float, np.ndarray]:
    """
    Compute sublimation pressure (solid-gas equilibrium line).

    Args:
        T_K: Temperature (K), scalar or array.
        gas: Gas type, 'argon' or 'xenon'.

    Returns:
        Sublimation pressure (bar).
    """
    props = GAS_PROPERTIES[gas]
    T_K = np.asarray(T_K)
    theta = T_K / props["T_triple"]

    sub = props["sublimation"]
    tau = 1 - theta
    exponent = (1 / theta) * (sub["A1"] * tau + sub["A2"] * tau**1.5)

    return props["P_triple_bar"] * np.exp(exponent)


def psat_bar(T_K: Union[float, np.ndarray], gas: str = "argon") -> Union[float, np.ndarray]:
    """
    Compute saturation vapor pressure (liquid-gas equilibrium line).

    Args:
        T_K: Temperature (K), scalar or array.
        gas: Gas type, 'argon' or 'xenon'.

    Returns:
        Saturation vapor pressure (bar).
    """

    GAS_PROPERTIES = {
        "argon": {
            "name": "Argon",
            "T_triple": 83.8058,  # K
            "P_triple_bar": 0.68891,  # bar
            "T_crit": 150.687,  # K
            "P_crit_bar": 48.63,  # bar
            "sublimation": {"A1": -11.391604, "A2": -0.39513431},
            "saturation": {"A1": -5.9409785, "A2": 1.3553888, "A3": -0.46497607, "A4": -1.5399043},
        },
        "xenon": {
            "name": "Xenon",
            "T_triple": 161.405,  # K
            "P_triple_bar": 0.81600,  # bar
            "T_crit": 289.733,  # K
            "P_crit_bar": 58.40,  # bar
            "sublimation": {"A1": -11.866, "A2": -0.3878},  # Lemmon & Span (2006)
            "saturation": {"A1": -6.0177, "A2": 1.4053, "A3": -0.4684, "A4": -1.8570},
        },
    }
    props = GAS_PROPERTIES[gas]
    T_K = np.asarray(T_K)
    theta = 1 - T_K / props["T_crit"]

    sat = props["saturation"]
    exponent = (props["T_crit"] / T_K) * (
        sat["A1"] * theta + sat["A2"] * theta**1.5 + sat["A3"] * theta**2.0 + sat["A4"] * theta**4.5
    )

    return props["P_crit_bar"] * np.exp(exponent)


def phase_boundary_bar(T_K: Union[float, np.ndarray], gas: str = "argon") -> Union[float, np.ndarray]:
    """
    Compute phase boundary pressure (auto-select sublimation or saturation curve).

    Args:
        T_K: Temperature (K), scalar or array.
        gas: Gas type, 'argon' or 'xenon'.

    Returns:
        Phase boundary pressure (bar).
    """
    props = GAS_PROPERTIES[gas]
    T_K = np.asarray(T_K)
    scalar_input = T_K.ndim == 0
    T_K = np.atleast_1d(T_K)

    result = np.full_like(T_K, np.nan, dtype=float)

    mask_sub = T_K <= props["T_triple"]
    if np.any(mask_sub):
        result[mask_sub] = psub_bar(T_K[mask_sub], gas)

    mask_sat = (T_K > props["T_triple"]) & (T_K <= props["T_crit"])
    if np.any(mask_sat):
        result[mask_sat] = psat_bar(T_K[mask_sat], gas)

    if scalar_input:
        return float(result[0])
    return result


def get_phase(T_K: float, P_bar: float, gas: str = "argon") -> str:
    """
    Determine the phase for a given (T, P) point.

    Args:
        T_K: Temperature (K).
        P_bar: Pressure (bar).
        gas: Gas type.

    Returns:
        Phase label: 'solid', 'liquid', 'gas', or 'supercritical'.
    """
    props = GAS_PROPERTIES[gas]

    if T_K > props["T_crit"] and P_bar > props["P_crit_bar"]:
        return "supercritical"

    if T_K <= props["T_triple"]:
        P_boundary = psub_bar(T_K, gas)
        return "solid" if P_bar > P_boundary else "gas"
    elif T_K <= props["T_crit"]:
        P_boundary = psat_bar(T_K, gas)
        return "liquid" if P_bar > P_boundary else "gas"
    else:
        return "gas" if P_bar < props["P_crit_bar"] else "supercritical"


# =============================================================================
# Matplotlib static plotting (multi-path support)
# =============================================================================


def _add_path_arrows_mpl(
    ax, x, y, every=8, mutation_scale=12, lw=1.3, alpha=0.9, max_arrows=12, min_dist=0.015, color="darkorange"
):
    """Add arrows along the path using FancyArrowPatch."""
    if not _MATPLOTLIB_AVAILABLE:
        return

    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    n = x.size
    if n < 2:
        return

    seg_idx = list(range(0, n - 1, max(1, int(every))))
    if seg_idx and seg_idx[-1] != n - 2:
        seg_idx.append(n - 2)

    if max_arrows is not None and max_arrows > 0 and len(seg_idx) > max_arrows:
        step = int(np.ceil(len(seg_idx) / max_arrows))
        seg_idx = seg_idx[::step]

    x_span = np.nanmax(x) - np.nanmin(x)
    y_span = np.nanmax(y) - np.nanmin(y)
    x_span = x_span if x_span > 0 else 1.0
    y_span = y_span if y_span > 0 else 1.0

    for i in seg_idx:
        x0, y0 = x[i], y[i]
        x1, y1 = x[i + 1], y[i + 1]
        if np.isclose(x0, x1) and np.isclose(y0, y1):
            continue
        dx_norm = (x1 - x0) / x_span
        dy_norm = (y1 - y0) / y_span
        if np.hypot(dx_norm, dy_norm) < min_dist:
            continue
        ax.add_patch(
            FancyArrowPatch(
                (x0, y0),
                (x1, y1),
                arrowstyle="-|>",
                mutation_scale=mutation_scale,
                lw=lw,
                alpha=alpha,
                color=color,
            )
        )


def _plot_pt_paths_matplotlib(
    paths_P,
    paths_T,
    *,
    gas,
    T_range,
    P_range,
    fill_regions,
    draw_boundary,
    arrow_every,
    arrow_max,
    arrow_min_dist,
    boundary_points,
    title,
    labels,
    colors,
    ax,
    show,
):
    """Matplotlib static plotting implementation with multi-path support."""
    if not _MATPLOTLIB_AVAILABLE:
        raise ImportError("Matplotlib is not available. Please install: pip install matplotlib")

    props = GAS_PROPERTIES[gas]
    T_min, T_max = T_range
    P_min, P_max = P_range

    if ax is None:
        _, ax = plt.subplots(figsize=(7.6, 5.0))

    # Phase boundary line data
    Tg = np.linspace(T_min, T_max, boundary_points)
    Pb = phase_boundary_bar(Tg, gas=gas)
    Pb_clip = np.clip(Pb, P_min, P_max)

    # Region fills
    if fill_regions:
        ax.fill_between(
            Tg, P_min, Pb_clip, alpha=0.10, hatch="..", edgecolor=(0, 0, 0, 0.4), linewidth=0.0, label="_nolegend_"
        )
        left = Tg < props["T_triple"]
        if np.any(left):
            ax.fill_between(
                Tg[left],
                Pb_clip[left],
                P_max,
                alpha=0.10,
                hatch="///",
                edgecolor=(0, 0, 0, 0.4),
                linewidth=0.0,
                label="_nolegend_",
            )
        right = ~left
        if np.any(right):
            ax.fill_between(
                Tg[right],
                Pb_clip[right],
                P_max,
                alpha=0.10,
                hatch="\\\\",
                edgecolor=(0, 0, 0, 0.4),
                linewidth=0.0,
                label="_nolegend_",
            )

    # Phase boundary line
    if draw_boundary:
        ax.plot(Tg, Pb, lw=1.6, alpha=0.9, color="darkblue", label="_nolegend_")

    # Triple point
    T_triple, P_triple = props["T_triple"], props["P_triple_bar"]
    if T_min <= T_triple <= T_max and P_min <= P_triple <= P_max:
        ax.scatter([T_triple], [P_triple], s=35, zorder=4, color="red", marker="*")
        ax.annotate("Triple point", (T_triple, P_triple), textcoords="offset points", xytext=(8, 6), fontsize=9)

    # Phase region labels
    ax.text(T_min + 0.62 * (T_max - T_min), P_min + 0.15 * (P_max - P_min), "Vapor", fontsize=12, color=(0, 0, 0, 0.6))
    if T_min < T_triple:
        ax.text(
            T_min + 0.18 * (min(T_triple, T_max) - T_min),
            P_min + 0.78 * (P_max - P_min),
            "Solid",
            fontsize=12,
            color=(0, 0, 0, 0.6),
        )
    if T_max > T_triple:
        ax.text(
            max(T_triple, T_min) + 0.35 * (T_max - max(T_triple, T_min)),
            P_min + 0.78 * (P_max - P_min),
            "Liquid",
            fontsize=12,
            color=(0, 0, 0, 0.6),
        )

    # Draw each path
    for idx, (P, T) in enumerate(zip(paths_P, paths_T)):
        color = colors[idx]
        label = labels[idx]

        if P.size == 1:
            ax.scatter([float(T)], [float(P)], marker="x", s=90, zorder=5, color=color, label=label)
        else:
            ax.plot(T, P, lw=1.8, alpha=0.85, color=color, label=label)
            _add_path_arrows_mpl(
                ax,
                T,
                P,
                every=arrow_every,
                mutation_scale=12,
                max_arrows=arrow_max,
                min_dist=arrow_min_dist,
                color=color,
            )
            # Start marker
            ax.scatter(
                [T[0]],
                [P[0]],
                s=80,
                zorder=5,
                color=color,
                edgecolors="darkgreen",
                linewidths=2,
                marker="o",
                label="_nolegend_",
            )
            # End marker
            ax.scatter(
                [T[-1]],
                [P[-1]],
                s=80,
                zorder=5,
                color=color,
                edgecolors="darkred",
                linewidths=2,
                marker="s",
                label="_nolegend_",
            )

    # Styling
    ax.set_xlim(T_range)
    ax.set_ylim(P_range)
    ax.set_xlabel("Temperature (K)")
    ax.set_ylabel("Pressure (bar)")
    ax.set_title(title or f"{props['name']} phase diagram ({T_min:.0f}–{T_max:.0f} K)")
    ax.grid(True, alpha=0.18, linewidth=0.8)
    if ax.get_legend_handles_labels()[0]:
        ax.legend(loc="upper left")

    plt.tight_layout()
    if show:
        plt.show()
    return ax


# =============================================================================
# Plotly interactive plotting (multi-path support)
# =============================================================================


def _plot_pt_paths_plotly(
    paths_P,
    paths_T,
    *,
    gas,
    T_range,
    P_range,
    fill_regions,
    draw_boundary,
    arrow_every,
    arrow_max,
    arrow_min_dist,
    downsample_max_points,
    boundary_points,
    title,
    labels,
    colors,
    fig,
    show,
):
    """Plotly interactive plotting implementation with multi-path support."""
    if not _PLOTLY_AVAILABLE:
        raise ImportError("Plotly is not available. Please install: pip install plotly")

    props = GAS_PROPERTIES[gas]
    T_min, T_max = T_range
    P_min, P_max = P_range

    if fig is None:
        fig = go.Figure()

    # -------------------------------------------------------------------------
    # 1. Fill phase regions
    # -------------------------------------------------------------------------
    if fill_regions:
        T_sub = np.linspace(T_min, min(props["T_triple"], T_max), boundary_points // 2)
        P_sub = psub_bar(T_sub, gas)

        T_sat_start = max(props["T_triple"], T_min)
        T_sat_end = min(props["T_crit"], T_max)
        if T_sat_start < T_sat_end:
            T_sat = np.linspace(T_sat_start, T_sat_end, boundary_points // 2)
            P_sat = psat_bar(T_sat, gas)
        else:
            T_sat = np.array([])
            P_sat = np.array([])

        # Gas region
        gas_T = [T_min, T_max]
        gas_P = [P_min, P_min]

        if T_max <= props["T_triple"]:
            P_boundary_right = psub_bar(T_max, gas)
        elif T_max <= props["T_crit"]:
            P_boundary_right = psat_bar(T_max, gas)
        else:
            P_boundary_right = P_max

        gas_T.append(T_max)
        gas_P.append(min(float(P_boundary_right), P_max))

        if len(T_sat) > 0:
            for t, p in zip(reversed(T_sat.tolist()), reversed(P_sat.tolist())):
                if p <= P_max:
                    gas_T.append(t)
                    gas_P.append(p)

        if T_min <= props["T_triple"] <= T_max and props["P_triple_bar"] <= P_max:
            gas_T.append(props["T_triple"])
            gas_P.append(props["P_triple_bar"])

        for t, p in zip(reversed(T_sub.tolist()), reversed(P_sub.tolist())):
            if p <= P_max:
                gas_T.append(t)
                gas_P.append(p)

        gas_T.append(T_min)
        gas_P.append(P_min)

        fig.add_trace(
            go.Scatter(
                x=gas_T,
                y=gas_P,
                fill="toself",
                fillcolor="rgba(173, 216, 230, 0.4)",
                line=dict(width=0),
                name="Gas",
                hoverinfo="name",
                showlegend=True,
            )
        )

        # Solid region
        if T_min < props["T_triple"]:
            solid_T = [T_min, min(props["T_triple"], T_max)]
            solid_P = [P_max, P_max]

            if props["P_triple_bar"] <= P_max:
                solid_T.append(props["T_triple"])
                solid_P.append(props["P_triple_bar"])

            for t, p in zip(reversed(T_sub.tolist()), reversed(P_sub.tolist())):
                if p <= P_max:
                    solid_T.append(t)
                    solid_P.append(p)

            solid_T.append(T_min)
            solid_P.append(P_max)

            fig.add_trace(
                go.Scatter(
                    x=solid_T,
                    y=solid_P,
                    fill="toself",
                    fillcolor="rgba(144, 238, 144, 0.4)",
                    line=dict(width=0),
                    name="Solid",
                    hoverinfo="name",
                    showlegend=True,
                )
            )

        # Liquid region
        if props["T_triple"] < T_max and len(T_sat) > 0:
            liquid_T = []
            liquid_P = []

            if props["P_triple_bar"] <= P_max:
                liquid_T.append(props["T_triple"])
                liquid_P.append(props["P_triple_bar"])

            for t, p in zip(T_sat.tolist(), P_sat.tolist()):
                if p <= P_max:
                    liquid_T.append(t)
                    liquid_P.append(p)

            if len(liquid_T) > 0:
                liquid_T.append(liquid_T[-1])
                liquid_P.append(P_max)

            liquid_T.append(max(props["T_triple"], T_min))
            liquid_P.append(P_max)

            if props["P_triple_bar"] <= P_max:
                liquid_T.append(props["T_triple"])
                liquid_P.append(props["P_triple_bar"])

            if len(liquid_T) >= 3:
                fig.add_trace(
                    go.Scatter(
                        x=liquid_T,
                        y=liquid_P,
                        fill="toself",
                        fillcolor="rgba(255, 182, 193, 0.4)",
                        line=dict(width=0),
                        name="Liquid",
                        hoverinfo="name",
                        showlegend=True,
                    )
                )

    # -------------------------------------------------------------------------
    # 2. Draw phase boundary lines
    # -------------------------------------------------------------------------
    if draw_boundary:
        T_sub_line = np.linspace(T_min, min(props["T_triple"], T_max), boundary_points // 2)
        P_sub_line = psub_bar(T_sub_line, gas)
        mask_sub = P_sub_line <= P_max

        if np.any(mask_sub):
            fig.add_trace(
                go.Scatter(
                    x=T_sub_line[mask_sub],
                    y=P_sub_line[mask_sub],
                    mode="lines",
                    line=dict(color="darkgreen", width=2),
                    name="Sublimation",
                    showlegend=False,
                    hovertemplate="T: %{x:.2f} K<br>P: %{y:.4f} bar<extra>Sublimation</extra>",
                )
            )

        T_sat_start = max(props["T_triple"], T_min)
        T_sat_end = min(props["T_crit"], T_max)
        if T_sat_start < T_sat_end:
            T_sat_line = np.linspace(T_sat_start, T_sat_end, boundary_points // 2)
            P_sat_line = psat_bar(T_sat_line, gas)
            mask_sat = P_sat_line <= P_max

            if np.any(mask_sat):
                fig.add_trace(
                    go.Scatter(
                        x=T_sat_line[mask_sat],
                        y=P_sat_line[mask_sat],
                        mode="lines",
                        line=dict(color="darkblue", width=2),
                        name="Saturation",
                        showlegend=False,
                        hovertemplate="T: %{x:.2f} K<br>P: %{y:.4f} bar<extra>Saturation</extra>",
                    )
                )

        # Triple point marker
        if T_min <= props["T_triple"] <= T_max and P_min <= props["P_triple_bar"] <= P_max:
            fig.add_trace(
                go.Scatter(
                    x=[props["T_triple"]],
                    y=[props["P_triple_bar"]],
                    mode="markers",
                    marker=dict(
                        size=12,
                        color="red",
                        symbol="star",
                        line=dict(width=1, color="darkred"),
                    ),
                    name="Triple Point",
                    hovertemplate=(
                        f"Triple Point<br>"
                        f"T: {props['T_triple']:.4f} K<br>"
                        f"P: {props['P_triple_bar']:.5f} bar<extra></extra>"
                    ),
                )
            )

    # -------------------------------------------------------------------------
    # 3. Draw data paths
    # -------------------------------------------------------------------------
    for idx, (P_bar, T_K) in enumerate(zip(paths_P, paths_T)):
        color = colors[idx]
        label = labels[idx]

        n_points = len(T_K)
        if downsample_max_points is not None and downsample_max_points > 1 and n_points > downsample_max_points:
            ds_idx = np.linspace(0, n_points - 1, downsample_max_points)
            ds_idx = np.unique(np.round(ds_idx).astype(int))
            if ds_idx[0] != 0:
                ds_idx = np.insert(ds_idx, 0, 0)
            if ds_idx[-1] != n_points - 1:
                ds_idx = np.append(ds_idx, n_points - 1)
            T_K = T_K[ds_idx]
            P_bar = P_bar[ds_idx]
            n_points = len(T_K)

        if n_points == 1:
            phase = get_phase(T_K[0], P_bar[0], gas)
            fig.add_trace(
                go.Scatter(
                    x=T_K,
                    y=P_bar,
                    mode="markers",
                    marker=dict(
                        size=14,
                        color=color,
                        symbol="circle",
                        line=dict(width=2, color="black"),
                    ),
                    name=f"{label} ({phase})",
                    hovertemplate=(f"T: %{{x:.2f}} K<br>P: %{{y:.4f}} bar<br>Phase: {phase}<extra>{label}</extra>"),
                )
            )
        else:
            fig.add_trace(
                go.Scatter(
                    x=T_K,
                    y=P_bar,
                    mode="lines",
                    line=dict(color=color, width=2.5),
                    name=label,
                    hovertemplate=f"T: %{{x:.2f}} K<br>P: %{{y:.4f}} bar<extra>{label}</extra>",
                )
            )

            # Start marker
            fig.add_trace(
                go.Scatter(
                    x=[T_K[0]],
                    y=[P_bar[0]],
                    mode="markers",
                    marker=dict(size=12, color=color, symbol="circle", line=dict(width=2, color="darkgreen")),
                    name=f"{label} Start",
                    showlegend=False,
                    hovertemplate=f"{label} Start<br>T: %{{x:.2f}} K<br>P: %{{y:.4f}} bar<extra></extra>",
                )
            )

            # End marker
            fig.add_trace(
                go.Scatter(
                    x=[T_K[-1]],
                    y=[P_bar[-1]],
                    mode="markers",
                    marker=dict(size=10, color=color, symbol="square", line=dict(width=2, color="darkred")),
                    name=f"{label} End",
                    showlegend=False,
                    hovertemplate=f"{label} End<br>T: %{{x:.2f}} K<br>P: %{{y:.4f}} bar<extra></extra>",
                )
            )

            # Arrow annotations - use central difference for direction
            if arrow_every > 0 and n_points > arrow_every:
                arrow_idx = list(range(arrow_every, n_points - 1, arrow_every))
                if arrow_max is not None and arrow_max > 0 and len(arrow_idx) > arrow_max:
                    step = int(np.ceil(len(arrow_idx) / arrow_max))
                    arrow_idx = arrow_idx[::step]
                for i in arrow_idx:
                    # Central difference for direction
                    i_prev = max(i - 1, 0)
                    i_next = min(i + 1, n_points - 1)
                    dx = T_K[i_next] - T_K[i_prev]
                    dy = P_bar[i_next] - P_bar[i_prev]

                    # Normalize
                    dx_norm = dx / (T_max - T_min) if (T_max - T_min) > 0 else 0
                    dy_norm = dy / (P_max - P_min) if (P_max - P_min) > 0 else 0
                    norm = np.sqrt(dx_norm**2 + dy_norm**2)

                    if norm > 1e-10 and norm >= arrow_min_dist:
                        scale = 0.02
                        ax_off = dx_norm / norm * scale * (T_max - T_min)
                        ay_off = dy_norm / norm * scale * (P_max - P_min)

                        fig.add_annotation(
                            x=T_K[i],
                            y=P_bar[i],
                            ax=T_K[i] - ax_off,
                            ay=P_bar[i] - ay_off,
                            xref="x",
                            yref="y",
                            axref="x",
                            ayref="y",
                            showarrow=True,
                            arrowhead=2,
                            arrowsize=1.5,
                            arrowwidth=2,
                            arrowcolor=color,
                        )

    # -------------------------------------------------------------------------
    # 4. Layout settings
    # -------------------------------------------------------------------------
    fig.update_layout(
        title=title or f"{props['name']} P-T Phase Diagram",
        xaxis=dict(
            title="Temperature (K)",
            range=[T_min, T_max],
            showgrid=True,
            gridcolor="lightgray",
        ),
        yaxis=dict(
            title="Pressure (bar)",
            range=[P_min, P_max],
            showgrid=True,
            gridcolor="lightgray",
        ),
        template="plotly_white",
        hovermode="closest",
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255, 255, 255, 0.8)",
        ),
        width=800,
        height=600,
    )

    if show:
        fig.show()

    return fig


# =============================================================================
# Main plotting function
# =============================================================================


def plot_pt_path(
    P_bar: Union[float, np.ndarray, List[np.ndarray]],
    T_K: Union[float, np.ndarray, List[np.ndarray]],
    *,
    gas: str = "argon",
    kind: str = "plotly",
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
    fig: Optional["go.Figure"] = None,
    ax: Optional["plt.Axes"] = None,
    show: bool = False,
) -> Union["go.Figure", "plt.Axes"]:
    """
    Plot a P-T phase diagram with one or multiple paths.

    Args:
        P_bar: Pressure data (bar). Can be:
            - scalar or 1D array for single path
            - list of arrays for multiple paths
        T_K: Temperature data (K). Must match P_bar structure.
        gas: Gas type, 'argon' or 'xenon'.
        kind: Plotting backend, 'plotly' (interactive) or 'matplotlib' (static).
        T_range: Temperature display range (K), default from gas properties.
        P_range: Pressure display range (bar), default (0, 3).
        fill_regions: Whether to fill phase regions.
        draw_boundary: Whether to draw phase boundary lines.
        arrow_every: Draw one arrow every N points (0 disables).
        arrow_max: Maximum number of arrows per path (None or <=0 means no limit).
        arrow_min_dist: Normalized displacement threshold; below this no arrow is drawn.
        downsample_max_points: Plotly downsample max points (None disables).
        boundary_points: Sampling points for boundary lines; default plotly 100, matplotlib 500.
        title: Plot title.
        labels: List of labels for each path (for legend). Default: "Path 1", "Path 2", ...
        colors: List of colors for each path. Default: uses DEFAULT_COLORS palette.
        fig: Existing Plotly Figure (when kind='plotly').
        ax: Existing Matplotlib Axes (when kind='matplotlib').
        show: Whether to display the plot automatically.

    Returns:
        kind='plotly': go.Figure
        kind='matplotlib': plt.Axes

    Examples:
        # Single path
        plot_pt_path(P, T, gas='argon')

        # Multiple paths
        plot_pt_path([P1, P2, P3], [T1, T2, T3], labels=['Run 1', 'Run 2', 'Run 3'])
    """
    if gas not in GAS_PROPERTIES:
        raise ValueError(f"Unknown gas type: {gas!r}. Supported: {list(GAS_PROPERTIES.keys())}")

    # Normalize inputs to list of arrays
    # Check if P_bar is a list of array-like objects (multiple paths)
    def _is_array_like(obj):
        """Check if object is array-like (numpy array, pandas Series, or list)."""
        return isinstance(obj, (list, np.ndarray)) or hasattr(obj, "values")  # pandas Series has .values

    if isinstance(P_bar, list) and len(P_bar) > 0 and _is_array_like(P_bar[0]):
        # Multiple paths
        paths_P = [np.atleast_1d(np.asarray(p, dtype=float)) for p in P_bar]
        paths_T = [np.atleast_1d(np.asarray(t, dtype=float)) for t in T_K]
    else:
        # Single path
        paths_P = [np.atleast_1d(np.asarray(P_bar, dtype=float))]
        paths_T = [np.atleast_1d(np.asarray(T_K, dtype=float))]

    n_paths = len(paths_P)
    if len(paths_T) != n_paths:
        raise ValueError(f"P_bar and T_K must have same number of paths: {n_paths} vs {len(paths_T)}")

    for i, (p, t) in enumerate(zip(paths_P, paths_T)):
        if len(p) != len(t):
            raise ValueError(f"Path {i}: P_bar and T_K length mismatch: {len(p)} vs {len(t)}")

    # Default labels and colors
    if labels is None:
        labels = [f"Path {i + 1}" for i in range(n_paths)] if n_paths > 1 else ["Path"]
    if colors is None:
        colors = [DEFAULT_COLORS[i % len(DEFAULT_COLORS)] for i in range(n_paths)]

    # Default ranges
    props = GAS_PROPERTIES[gas]
    if T_range is None:
        T_range = (props["T_triple"] - 5, props["T_triple"] + 20)
    if P_range is None:
        P_range = (0.0, 3.0)

    # Default boundary sampling points per backend
    if boundary_points is None:
        boundary_points = 100 if kind == "plotly" else 500

    if kind == "plotly":
        return _plot_pt_paths_plotly(
            paths_P,
            paths_T,
            gas=gas,
            T_range=T_range,
            P_range=P_range,
            fill_regions=fill_regions,
            draw_boundary=draw_boundary,
            arrow_every=arrow_every,
            arrow_max=arrow_max,
            arrow_min_dist=arrow_min_dist,
            downsample_max_points=downsample_max_points,
            boundary_points=boundary_points,
            title=title,
            labels=labels,
            colors=colors,
            fig=fig,
            show=show,
        )
    elif kind == "matplotlib":
        return _plot_pt_paths_matplotlib(
            paths_P,
            paths_T,
            gas=gas,
            T_range=T_range,
            P_range=P_range,
            fill_regions=fill_regions,
            draw_boundary=draw_boundary,
            arrow_every=arrow_every,
            arrow_max=arrow_max,
            arrow_min_dist=arrow_min_dist,
            boundary_points=boundary_points,
            title=title,
            labels=labels,
            colors=colors,
            ax=ax,
            show=show,
        )
    else:
        raise ValueError(f"kind must be 'plotly' or 'matplotlib', got {kind!r}")


def plot_argon_pt_path(P_bar: Union[float, np.ndarray], T_K: Union[float, np.ndarray], **kwargs) -> "go.Figure":
    """Backward compatibility: plot argon P-T diagram (defaults to plotly)."""
    kwargs.setdefault("kind", "plotly")
    kwargs.setdefault("T_range", (80.0, 100.0))
    return plot_pt_path(P_bar, T_K, gas="argon", **kwargs)
