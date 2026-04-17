"""
Dashboard 回调函数

提供 Dashboard 的 Dash 回调函数。
"""

from typing import TYPE_CHECKING, Optional, Tuple

import pandas as pd
from dash import Input, Output, State, callback_context, no_update

from .config import DashboardConfig
from .helpers import (
    column_options,
    empty_fig,
    find_columns,
    format_range_value,
    parse_custom_hours,
    parse_int,
    parse_load_hours,
    select_or_fallback,
)

if TYPE_CHECKING:
    from dash import Dash

    from ..cache import AlignedData
    from ..reader import SCReader
    from ..spec import TableSpec


def register_callbacks(
    app: "Dash",
    cache: "AlignedData",
    reader: "SCReader",
    specs: list,
    config: DashboardConfig,
) -> None:
    """注册所有回调函数

    Args:
        app: Dash 应用实例
        cache: 数据缓存
        reader: 数据读取器
        specs: 表规格列表
        config: Dashboard 配置
    """
    # 使用闭包保存状态
    state = {
        "first_load": True,
        "current_load_hours": config.initial_load_hours,
    }

    @app.callback(
        Output("custom-hours-container", "style"),
        Input("load-window", "value"),
    )
    def toggle_custom_hours(load_window):
        """切换自定义小时数输入框的显示"""
        if load_window == "custom":
            return {"flex": "1", "minWidth": "140px"}
        return {"flex": "1", "minWidth": "140px", "display": "none"}

    @app.callback(
        Output("ts-graph", "figure"),
        Output("press-overview", "figure"),
        Output("temp-overview", "figure"),
        Output("status", "children"),
        Output("temp-col", "options"),
        Output("press-col", "options"),
        Output("temp-col", "value"),
        Output("press-col", "value"),
        Input("tick", "n_intervals"),
        Input("load-window", "value"),
        Input("custom-hours", "value"),
        Input("ts-max-points", "value"),
        State("temp-col", "value"),
        State("press-col", "value"),
    )
    def refresh_timeseries(
        _n: int,
        load_window: Optional[str],
        custom_hours: Optional[float],
        ts_max_points: Optional[str],
        temp_col: Optional[str],
        press_col: Optional[str],
    ):
        """刷新时间序列图并从数据库更新数据"""
        # 导入可视化函数（延迟导入避免循环依赖）
        from ..visualizer import plot_dual_axis, plot_timeseries

        custom_hours_val = (
            parse_custom_hours(custom_hours)
            if load_window == "custom"
            else None
        )
        load_hours = (
            custom_hours_val
            if custom_hours_val is not None
            else parse_load_hours(load_window)
        )
        ts_max_points_val = parse_int(ts_max_points, config.ts_max_points)

        # 检查是否需要重置缓存
        if load_hours != state["current_load_hours"]:
            cache.reset(reset_watermark=True)
            state["first_load"] = True
            state["current_load_hours"] = load_hours

        status_note = ""
        try:
            force_full = state["first_load"] and load_hours is None

            if state["first_load"] and load_hours is not None:
                # 设置初始加载窗口
                end_time = None
                try:
                    tr = reader.get_time_range(config.anchor_table)
                    end_time = tr.get("max_time")
                except Exception:
                    end_time = None

                if end_time is None:
                    tz = getattr(reader, "_time_zone", None)
                    now = (
                        pd.Timestamp.now(tz=tz)
                        if tz
                        else pd.Timestamp.now()
                    )
                    end_time = now

                cutoff = pd.Timestamp(end_time) - pd.Timedelta(hours=load_hours)
                cutoff_dt = cutoff.to_pydatetime()
                for spec in specs:
                    reader._watermarks[spec.table] = {
                        "last_ts": cutoff_dt,
                        "last_id": None,
                    }
                status_note = f"load_last_{load_hours}h"
            elif state["first_load"] and load_hours is None:
                status_note = "load_all"

            added = cache.update(force_full=force_full)
            state["first_load"] = False
            stats = cache.stats
            time_range = stats.get("time_range")
            range_text = (
                f"{time_range[0]} ~ {time_range[1]}"
                if time_range
                else "n/a"
            )
            status = (
                f"rows={stats['total_rows']} | cols={stats['total_columns']} | "
                f"added={added} | last_update={stats['last_update']} | range={range_text}"
            )
            if status_note:
                status = f"{status} | {status_note}"
        except Exception as exc:
            status = f"update failed: {exc}"

        df = cache.data
        if df.empty:
            empty = empty_fig("no data yet")
            return empty, empty, empty, status, [], [], None, None

        all_cols = list(df.columns)
        temp_candidates = find_columns(all_cols, ["temp", "temperature"])
        press_candidates = find_columns(all_cols, ["press", "pressure"])

        temp_options = column_options(temp_candidates or all_cols)
        press_options = column_options(press_candidates or all_cols)

        temp_col = select_or_fallback(temp_col, temp_candidates or all_cols)
        press_col = select_or_fallback(press_col, press_candidates or all_cols)

        if not temp_col or not press_col:
            empty = empty_fig("missing temperature/pressure columns")
            return (
                empty,
                empty,
                empty,
                status,
                temp_options,
                press_options,
                temp_col,
                press_col,
            )

        # 创建时间序列图
        ts_fig = plot_dual_axis(cache, temp_col, press_col, max_points=ts_max_points_val)
        ts_fig.update_layout(
            xaxis=dict(
                rangeslider=dict(visible=True, thickness=0.08),
                type="date",
            ),
            title="Time Series (Drag to Select Range for Phase Diagram)",
        )

        # 创建压力概览图
        if not press_candidates:
            press_fig = empty_fig("no pressure columns")
        else:
            press_fig = plot_timeseries(
                cache,
                press_candidates,
                max_points=ts_max_points_val,
                title="All Pressure",
                ylabel="Pressure",
            )

        # 创建温度概览图
        if not temp_candidates:
            temp_fig = empty_fig("no temperature columns")
        else:
            temp_fig = plot_timeseries(
                cache,
                temp_candidates,
                max_points=ts_max_points_val,
                title="All Temperature",
                ylabel="Temperature",
            )

        return (
            ts_fig,
            press_fig,
            temp_fig,
            status,
            temp_options,
            press_options,
            temp_col,
            press_col,
        )

    @app.callback(
        Output("selected-range-store", "data"),
        Output("range-display", "children"),
        Input("ts-graph", "relayoutData"),
        Input("reset-range-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def update_selected_range(relayout_data, reset_clicks):
        """根据用户交互更新选中的时间范围"""
        ctx = callback_context
        if not ctx.triggered:
            return no_update, no_update

        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if trigger_id == "reset-range-btn":
            return None, "Range: Full data"

        if trigger_id == "ts-graph" and relayout_data:
            if (
                "xaxis.range[0]" in relayout_data
                and "xaxis.range[1]" in relayout_data
            ):
                start = relayout_data["xaxis.range[0]"]
                end = relayout_data["xaxis.range[1]"]
                return (
                    {"start": start, "end": end},
                    f"Range: {format_range_value(start)} ~ {format_range_value(end)}",
                )
            elif "xaxis.range" in relayout_data:
                range_val = relayout_data["xaxis.range"]
                if isinstance(range_val, list) and len(range_val) == 2:
                    start, end = range_val
                    return (
                        {"start": start, "end": end},
                        f"Range: {format_range_value(start)} ~ {format_range_value(end)}",
                    )
            elif (
                "xaxis.autorange" in relayout_data
                and relayout_data["xaxis.autorange"]
            ):
                return None, "Range: Full data"

        return no_update, no_update

    @app.callback(
        Output("pt-graph", "figure"),
        Input("selected-range-store", "data"),
        Input("phase-max-points", "value"),
        Input("tick", "n_intervals"),
    )
    def update_phase_diagram(selected_range, phase_max_points: Optional[str], _n):
        """根据选中的时间范围更新相图"""
        from ..phase_diagram import plot_pt_path

        phase_max_points_val = parse_int(phase_max_points, config.phase_max_points)
        df = cache.data
        if df.empty:
            return empty_fig("no data yet")

        # 确定相图数据范围
        if selected_range and "start" in selected_range and "end" in selected_range:
            try:
                df_phase = cache[selected_range["start"]:selected_range["end"]]
                if df_phase.empty:
                    return empty_fig("No data in selected range")
            except Exception as e:
                return empty_fig(f"Error selecting range: {e}")
        else:
            df_phase = df

        # 检查必需的列
        required_cols = [config.phase_pressure_primary, *config.phase_temperatures]
        if config.phase_pressure_secondary:
            required_cols.append(config.phase_pressure_secondary)
        missing = [c for c in required_cols if c not in df_phase.columns]

        if missing:
            return empty_fig(f"missing columns: {', '.join(missing)}")

        # 提取数据
        p_primary = df_phase[config.phase_pressure_primary].to_numpy()
        p_secondary = (
            df_phase[config.phase_pressure_secondary].to_numpy()
            if config.phase_pressure_secondary
            else p_primary
        )
        temps = [df_phase[c].to_numpy() for c in config.phase_temperatures]

        # 单位转换
        p_primary = p_primary * config.press_scale + config.press_offset
        p_secondary = p_secondary * config.press_scale + config.press_offset
        temps = [t * config.temp_scale + config.temp_offset for t in temps]

        # 构建压力路径
        p_paths = [p_primary, p_secondary, p_secondary, p_secondary][: len(temps)]

        # 绘制相图
        pt_fig = plot_pt_path(
            p_paths,
            temps,
            gas=config.gas,
            kind="plotly",
            arrow_max=12,
            arrow_min_dist=0.02,
            downsample_max_points=phase_max_points_val,
            T_range=config.T_range,
            P_range=config.P_range,
            title=f"P-T Phase Diagram ({config.gas})",
            labels=["cold finger", "Point B", "Point C", "Point D"][: len(temps)],
            colors=["red", "orange", "blue", "green"][: len(temps)],
        )

        return pt_fig
