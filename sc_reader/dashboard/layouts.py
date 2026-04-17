"""
Dashboard 布局组件

提供 Dashboard 的 UI 布局组件。
"""

from typing import Optional

from dash import dcc, html

from .config import DashboardConfig
from .helpers import LOAD_WINDOW_OPTIONS, PHASE_MAX_POINTS_OPTIONS, TS_MAX_POINTS_OPTIONS


def create_layout(config: DashboardConfig, poll_interval_ms: int) -> html.Div:
    """创建 Dashboard 布局

    Args:
        config: Dashboard 配置
        poll_interval_ms: 数据刷新间隔（毫秒）

    Returns:
        Dash HTML 布局组件
    """
    load_window_default = (
        "all" if config.initial_load_hours is None
        else str(config.initial_load_hours)
    )
    ts_max_points_default = str(config.ts_max_points)
    phase_max_points_default = str(config.phase_max_points)

    return html.Div(
        [
            html.H2("SC Temperature/Pressure Dashboard"),
            # 控制面板 (全宽)
            _create_control_panel(
                load_window_default,
                ts_max_points_default,
                phase_max_points_default,
            ),
            # 主内容区 (左右并排)
            html.Div(
                [
                    # 左列: 时间序列图
                    html.Div(
                        [
                            # 主时间序列图
                            dcc.Graph(id="ts-graph"),
                            # 概览图 (并排)
                            html.Div(
                                [
                                    dcc.Graph(id="press-overview"),
                                    dcc.Graph(id="temp-overview"),
                                ],
                                style={
                                    "display": "flex",
                                    "gap": "12px",
                                    "flexWrap": "wrap",
                                },
                            ),
                            # 范围控制
                            html.Div(
                                [
                                    html.Button(
                                        "Reset Range",
                                        id="reset-range-btn",
                                        n_clicks=0,
                                        style={"marginRight": "12px"},
                                    ),
                                    html.Span(
                                        id="range-display",
                                        style={"fontSize": "12px", "color": "#666"},
                                    ),
                                ],
                                style={"marginTop": "8px", "marginBottom": "8px"},
                            ),
                        ],
                        style={"flex": "3", "minWidth": "500px"},
                    ),
                    # 右列: P-T 相图
                    html.Div(
                        [
                            dcc.Graph(
                                id="pt-graph",
                                style={"height": "600px"},
                            ),
                        ],
                        style={
                            "flex": "2",
                            "minWidth": "400px",
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "gap": "16px",
                    "flexWrap": "wrap",
                    "alignItems": "flex-start",
                },
            ),
            # 状态栏 (全宽)
            html.Div(id="status", style={"marginTop": "8px", "fontSize": "12px"}),
            # 定时器
            dcc.Interval(id="tick", interval=poll_interval_ms, n_intervals=0),
            # 隐藏存储
            dcc.Store(id="selected-range-store", data=None),
        ],
        style={"maxWidth": "1600px", "margin": "0 auto", "padding": "16px"},
    )


def _create_control_panel(
    load_window_default: str,
    ts_max_points_default: str,
    phase_max_points_default: str,
) -> html.Div:
    """创建控制面板

    Args:
        load_window_default: 默认加载窗口
        ts_max_points_default: 默认时间序列最大点数
        phase_max_points_default: 默认相图最大点数

    Returns:
        控制面板 HTML 组件
    """
    return html.Div(
        [
            # 温度列选择
            html.Div(
                [
                    html.Label("Temperature column"),
                    dcc.Dropdown(
                        id="temp-col",
                        options=[],
                        value=None,
                        clearable=False,
                    ),
                ],
                style={"flex": "1"},
            ),
            # 压力列选择
            html.Div(
                [
                    html.Label("Pressure column"),
                    dcc.Dropdown(
                        id="press-col",
                        options=[],
                        value=None,
                        clearable=False,
                    ),
                ],
                style={"flex": "1"},
            ),
            # 加载窗口选择
            html.Div(
                [
                    html.Label("Load window"),
                    dcc.Dropdown(
                        id="load-window",
                        options=LOAD_WINDOW_OPTIONS,
                        value=load_window_default,
                        clearable=False,
                    ),
                ],
                style={"flex": "1", "minWidth": "180px"},
            ),
            # 自定义小时数输入
            html.Div(
                [
                    html.Label("Custom hours"),
                    dcc.Input(
                        id="custom-hours",
                        type="number",
                        min=0.1,
                        step=0.1,
                        placeholder="e.g. 0.5",
                        debounce=True,
                        style={"width": "100%"},
                    ),
                ],
                id="custom-hours-container",
                style={"flex": "1", "minWidth": "140px", "display": "none"},
            ),
            # 时间序列最大点数
            html.Div(
                [
                    html.Label("TS max points"),
                    dcc.Dropdown(
                        id="ts-max-points",
                        options=TS_MAX_POINTS_OPTIONS,
                        value=ts_max_points_default,
                        clearable=False,
                    ),
                ],
                style={"flex": "1", "minWidth": "140px"},
            ),
            # 相图最大点数
            html.Div(
                [
                    html.Label("Phase max points"),
                    dcc.Dropdown(
                        id="phase-max-points",
                        options=PHASE_MAX_POINTS_OPTIONS,
                        value=phase_max_points_default,
                        clearable=False,
                    ),
                ],
                style={"flex": "1", "minWidth": "150px"},
            ),
        ],
        style={
            "display": "flex",
            "gap": "12px",
            "marginBottom": "12px",
            "flexWrap": "wrap",
        },
    )
