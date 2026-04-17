#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dash app for temperature/pressure and P-T phase diagram with interactive range selection.

Features:
  - Time series plot with RangeSlider for selecting time range
  - P-T phase diagram that updates based on selected time range
  - Auto-refresh with configurable interval

Usage:
  1) Copy sc_config.example.json -> sc_config.json (or set SC_CONFIG_PATH)
  2) Adjust TABLE/column settings below
  3) python example/dash_phase_app.py
"""

import atexit
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, callback_context, dcc, html, no_update

from sc_reader import AlignConfig, AlignedData, MySQLConfig, SCReader, TableSpec, plot_dual_axis, plot_timeseries
from sc_reader.phase_diagram import plot_pt_path

# ------------------------------
# User settings
# ------------------------------

# Tables and time column (None = auto-detect)
# Matches phase_diagram.ipynb
ANCHOR_TABLE = "piddata"
TIME_COL = "timestamp"  # set to None to auto-detect
TABLE_SPECS = [
    ("tempdata", TIME_COL),
    ("runlidata", TIME_COL),
    ("statedata", TIME_COL),
    ("piddata", TIME_COL),
]

# If you want to read only a subset of columns, set lists below per table.
# Example: {"piddata": ["A_Temperature", "B_Temperature"], "runlidata": ["Pressure5"]}
TABLE_COLUMNS: Optional[dict] = None

# Gas type for phase diagram: "argon" or "xenon"
GAS = "argon"

# Phase diagram defaults (from phase_diagram.ipynb)
PHASE_WINDOW: Optional[Tuple[str, str]] = None
PHASE_FALLBACK_HOURS = 6
PHASE_PRESSURE_PRIMARY = "runlidata__Pressure5"
PHASE_PRESSURE_SECONDARY = "runlidata__Pressure6"
PHASE_TEMPERATURES = [
    "piddata__A_Temperature",
    "piddata__B_Temperature",
    "piddata__C_Temperature",
    "piddata__D_Temperature",
]

# Unit conversions for phase diagram
# plot_pt_path expects Temperature in K and Pressure in bar.
TEMP_SCALE = 1.0
TEMP_OFFSET = 0.0  # set to 273.15 if your temperature is in Celsius
PRESS_SCALE = 1.0  # set to 0.01 if your pressure is in mbar
PRESS_OFFSET = 0.0

# Initial load window for faster startup (set None for full history)
INITIAL_LOAD_HOURS: Optional[int] = 6

# Load window dropdown options (hours)
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
LOAD_WINDOW_DEFAULT = "all" if INITIAL_LOAD_HOURS is None else str(INITIAL_LOAD_HOURS)

# Downsample options for plotting
TS_MAX_POINTS_OPTIONS = [
    {"label": "2k", "value": "2000"},
    {"label": "5k", "value": "5000"},
    {"label": "8k", "value": "8000"},
    {"label": "12k", "value": "12000"},
    {"label": "20k", "value": "20000"},
]
PHASE_MAX_POINTS_OPTIONS = [
    {"label": "2k", "value": "2000"},
    {"label": "5k", "value": "5000"},
    {"label": "10k", "value": "10000"},
    {"label": "20k", "value": "20000"},
    {"label": "50k", "value": "50000"},
]
TS_MAX_POINTS_DEFAULT = "8000"
PHASE_MAX_POINTS_DEFAULT = "10000"

# ------------------------------
# Init reader/cache
# ------------------------------

CONFIG_PATH = Path(__file__).resolve().parent.parent / "sc_config.json"

mysql_cfg = MySQLConfig.from_json(str(CONFIG_PATH))
align_cfg = AlignConfig.from_json(str(CONFIG_PATH))

reader = SCReader(config=mysql_cfg, state_path="./dash_watermark.json")

specs = [TableSpec(table, time_col, cols=(TABLE_COLUMNS or {}).get(table)) for table, time_col in TABLE_SPECS]

cache = AlignedData(
    reader,
    specs,
    anchor=ANCHOR_TABLE,
    tolerance="20s",
    direction=align_cfg.direction,
    lookback="20s",
)


@atexit.register
def _cleanup():
    try:
        reader.close()
    except Exception:
        pass


# ------------------------------
# Helpers
# ------------------------------


def _find_columns(cols: List[str], keywords: List[str]) -> List[str]:
    lowered = [(c, c.lower()) for c in cols]
    matched = [c for c, cl in lowered if any(k in cl for k in keywords)]
    return matched


def _column_options(cols: List[str]) -> List[dict]:
    return [{"label": c, "value": c} for c in cols]


def _empty_fig(message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=message, x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False)
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.update_layout(template="plotly_white")
    return fig


def _select_or_fallback(current: Optional[str], candidates: List[str]) -> Optional[str]:
    if current and current in candidates:
        return current
    return candidates[0] if candidates else None


def _parse_load_hours(value: Optional[str]) -> Optional[float]:
    if value is None or value in {"all", "custom"}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_custom_hours(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        hours = float(value)
    except (TypeError, ValueError):
        return None
    return hours if hours > 0 else None


def _format_range_value(value) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, str):
        return value[:19]
    try:
        return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


def _parse_int(value: Optional[str], default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ------------------------------
# Dash app
# ------------------------------

app = Dash(__name__)
_FIRST_LOAD = True
_CURRENT_LOAD_HOURS = INITIAL_LOAD_HOURS

app.layout = html.Div(
    [
        html.H2("SC Temperature/Pressure Dashboard"),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Temperature column"),
                        dcc.Dropdown(id="temp-col", options=[], value=None, clearable=False),
                    ],
                    style={"flex": "1"},
                ),
                html.Div(
                    [
                        html.Label("Pressure column"),
                        dcc.Dropdown(id="press-col", options=[], value=None, clearable=False),
                    ],
                    style={"flex": "1"},
                ),
                html.Div(
                    [
                        html.Label("Load window"),
                        dcc.Dropdown(
                            id="load-window",
                            options=LOAD_WINDOW_OPTIONS,
                            value=LOAD_WINDOW_DEFAULT,
                            clearable=False,
                        ),
                    ],
                    style={"flex": "1", "minWidth": "180px"},
                ),
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
                html.Div(
                    [
                        html.Label("TS max points"),
                        dcc.Dropdown(
                            id="ts-max-points",
                            options=TS_MAX_POINTS_OPTIONS,
                            value=TS_MAX_POINTS_DEFAULT,
                            clearable=False,
                        ),
                    ],
                    style={"flex": "1", "minWidth": "140px"},
                ),
                html.Div(
                    [
                        html.Label("Phase max points"),
                        dcc.Dropdown(
                            id="phase-max-points",
                            options=PHASE_MAX_POINTS_OPTIONS,
                            value=PHASE_MAX_POINTS_DEFAULT,
                            clearable=False,
                        ),
                    ],
                    style={"flex": "1", "minWidth": "150px"},
                ),
            ],
            style={"display": "flex", "gap": "12px", "marginBottom": "12px", "flexWrap": "wrap"},
        ),
        dcc.Graph(id="ts-graph"),
        html.Div(
            [
                dcc.Graph(id="press-overview"),
                dcc.Graph(id="temp-overview"),
            ],
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
        ),
        html.Div(
            [
                html.Button("Reset Range", id="reset-range-btn", n_clicks=0, style={"marginRight": "12px"}),
                html.Span(id="range-display", style={"fontSize": "12px", "color": "#666"}),
            ],
            style={"marginTop": "8px", "marginBottom": "8px"},
        ),
        dcc.Graph(id="pt-graph"),
        html.Div(id="status", style={"marginTop": "8px", "fontSize": "12px"}),
        dcc.Interval(id="tick", interval=int(align_cfg.poll_interval * 1000), n_intervals=0),
        # Hidden store for selected range
        dcc.Store(id="selected-range-store", data=None),
    ],
    style={"maxWidth": "1200px", "margin": "0 auto", "padding": "16px"},
)

@app.callback(
    Output("custom-hours-container", "style"),
    Input("load-window", "value"),
)
def _toggle_custom_hours(load_window):
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
def _refresh_timeseries(
    _n: int,
    load_window: Optional[str],
    custom_hours: Optional[float],
    ts_max_points: Optional[str],
    temp_col: Optional[str],
    press_col: Optional[str],
):
    """Refresh time series plot and update data from database."""
    global _FIRST_LOAD, _CURRENT_LOAD_HOURS
    custom_hours_val = _parse_custom_hours(custom_hours) if load_window == "custom" else None
    load_hours = custom_hours_val if custom_hours_val is not None else _parse_load_hours(load_window)
    ts_max_points_val = _parse_int(ts_max_points, int(TS_MAX_POINTS_DEFAULT))
    if load_hours != _CURRENT_LOAD_HOURS:
        cache.reset(reset_watermark=True)
        _FIRST_LOAD = True
        _CURRENT_LOAD_HOURS = load_hours
    status_note = ""
    try:
        force_full = _FIRST_LOAD and load_hours is None
        if _FIRST_LOAD and load_hours is not None:
            end_time = None
            try:
                tr = reader.get_time_range(ANCHOR_TABLE)
                end_time = tr.get("max_time")
            except Exception:
                end_time = None

            if end_time is None:
                now = pd.Timestamp.now(tz=reader._time_zone) if reader._time_zone else pd.Timestamp.now()
                end_time = now

            cutoff = pd.Timestamp(end_time) - pd.Timedelta(hours=load_hours)
            cutoff_dt = cutoff.to_pydatetime()
            for spec in specs:
                reader._watermarks[spec.table] = {"last_ts": cutoff_dt, "last_id": None}
            status_note = f"load_last_{load_hours}h"
        elif _FIRST_LOAD and load_hours is None:
            status_note = "load_all"

        added = cache.update(force_full=force_full)
        _FIRST_LOAD = False
        stats = cache.stats
        time_range = stats.get("time_range")
        range_text = f"{time_range[0]} ~ {time_range[1]}" if time_range else "n/a"
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
        empty = _empty_fig("no data yet")
        return empty, empty, empty, status, [], [], None, None

    all_cols = list(df.columns)
    temp_candidates = _find_columns(all_cols, ["temp", "temperature"])
    press_candidates = _find_columns(all_cols, ["press", "pressure"])

    temp_options = _column_options(temp_candidates or all_cols)
    press_options = _column_options(press_candidates or all_cols)

    temp_col = _select_or_fallback(temp_col, temp_candidates or all_cols)
    press_col = _select_or_fallback(press_col, press_candidates or all_cols)

    if not temp_col or not press_col:
        empty = _empty_fig("missing temperature/pressure columns")
        return empty, empty, empty, status, temp_options, press_options, temp_col, press_col

    # Create time series figure with RangeSlider
    ts_fig = plot_dual_axis(cache, temp_col, press_col, max_points=ts_max_points_val)

    # Add RangeSlider to the time series figure
    ts_fig.update_layout(
        xaxis=dict(
            rangeslider=dict(visible=True, thickness=0.08),
            type="date",
        ),
        title="Time Series (Drag to Select Range for Phase Diagram)",
    )

    if not press_candidates:
        press_fig = _empty_fig("no pressure columns")
    else:
        press_fig = plot_timeseries(
            cache,
            press_candidates,
            max_points=ts_max_points_val,
            title="All Pressure",
            ylabel="Pressure",
        )

    if not temp_candidates:
        temp_fig = _empty_fig("no temperature columns")
    else:
        temp_fig = plot_timeseries(
            cache,
            temp_candidates,
            max_points=ts_max_points_val,
            title="All Temperature",
            ylabel="Temperature",
        )

    return ts_fig, press_fig, temp_fig, status, temp_options, press_options, temp_col, press_col


@app.callback(
    Output("selected-range-store", "data"),
    Output("range-display", "children"),
    Input("ts-graph", "relayoutData"),
    Input("reset-range-btn", "n_clicks"),
    prevent_initial_call=True,
)
def _update_selected_range(relayout_data, reset_clicks):
    """Update selected range based on user interaction."""
    ctx = callback_context
    if not ctx.triggered:
        return no_update, no_update

    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if trigger_id == "reset-range-btn":
        return None, "Range: Full data"

    if trigger_id == "ts-graph" and relayout_data:
        # Check for range selection from RangeSlider or zoom
        if "xaxis.range[0]" in relayout_data and "xaxis.range[1]" in relayout_data:
            start = relayout_data["xaxis.range[0]"]
            end = relayout_data["xaxis.range[1]"]
            return {"start": start, "end": end}, f"Range: {_format_range_value(start)} ~ {_format_range_value(end)}"
        elif "xaxis.range" in relayout_data:
            range_val = relayout_data["xaxis.range"]
            if isinstance(range_val, list) and len(range_val) == 2:
                start, end = range_val
                return {"start": start, "end": end}, f"Range: {_format_range_value(start)} ~ {_format_range_value(end)}"
        elif "xaxis.autorange" in relayout_data and relayout_data["xaxis.autorange"]:
            return None, "Range: Full data"

    return no_update, no_update


@app.callback(
    Output("pt-graph", "figure"),
    Input("selected-range-store", "data"),
    Input("phase-max-points", "value"),
    Input("tick", "n_intervals"),
)
def _update_phase_diagram(selected_range, phase_max_points: Optional[str], _n):
    """Update phase diagram based on selected time range."""
    phase_max_points_val = _parse_int(phase_max_points, int(PHASE_MAX_POINTS_DEFAULT))
    df = cache.data
    if df.empty:
        return _empty_fig("no data yet")

    # Determine data range for phase diagram
    if selected_range and "start" in selected_range and "end" in selected_range:
        try:
            df_phase = cache[selected_range["start"] : selected_range["end"]]
            if df_phase.empty:
                return _empty_fig("No data in selected range")
        except Exception as e:
            return _empty_fig(f"Error selecting range: {e}")
    elif PHASE_WINDOW:
        df_phase = cache[PHASE_WINDOW[0] : PHASE_WINDOW[1]]
        if df_phase.empty and not df.empty:
            end = df.index.max()
            start = end - pd.Timedelta(hours=PHASE_FALLBACK_HOURS)
            df_phase = df.loc[start:end]
    else:
        df_phase = df

    required_cols = [PHASE_PRESSURE_PRIMARY, *PHASE_TEMPERATURES]
    if PHASE_PRESSURE_SECONDARY:
        required_cols.append(PHASE_PRESSURE_SECONDARY)
    missing = [c for c in required_cols if c not in df_phase.columns]

    if missing:
        return _empty_fig(f"missing columns: {', '.join(missing)}")

    p_primary = df_phase[PHASE_PRESSURE_PRIMARY].to_numpy()
    p_secondary = df_phase[PHASE_PRESSURE_SECONDARY].to_numpy() if PHASE_PRESSURE_SECONDARY else p_primary
    temps = [df_phase[c].to_numpy() for c in PHASE_TEMPERATURES]

    # Convert units
    p_primary = p_primary * PRESS_SCALE + PRESS_OFFSET
    p_secondary = p_secondary * PRESS_SCALE + PRESS_OFFSET
    temps = [t * TEMP_SCALE + TEMP_OFFSET for t in temps]

    p_paths = [p_primary, p_secondary, p_secondary, p_secondary][: len(temps)]

    pt_fig = plot_pt_path(
        p_paths,
        temps,
        gas=GAS,
        kind="plotly",
        arrow_max=12,
        arrow_min_dist=0.02,
        downsample_max_points=phase_max_points_val,
        T_range=(80, 110),
        P_range=(0.5, 3.5),
        title=f"P-T Phase Diagram ({GAS})",
        labels=["cold finger", "Point B", "Point C", "Point D"][: len(temps)],
        colors=["red", "orange", "blue", "green"][: len(temps)],
    )

    return pt_fig


if __name__ == "__main__":
    app.run(debug=True, port=8051)
