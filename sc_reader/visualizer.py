"""
慢控数据可视化模块

提供多种可视化函数用于分析慢控数据
"""

import functools
import warnings
from typing import TYPE_CHECKING, Callable, List, Optional, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

if TYPE_CHECKING:
    from .cache import AlignedData

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False
    go = None
    make_subplots = None


# 设置默认样式
plt.style.use("seaborn-v0_8-darkgrid")
sns.set_palette("husl")


def _downsample_for_plotly(
    data: Union[pd.DataFrame, pd.Series], max_points: int = 10000
) -> Union[pd.DataFrame, pd.Series]:
    """
    自动降采样数据以优化 Plotly 渲染性能

    Args:
        data: DataFrame 或 Series
        max_points: 最大数据点数，超过此值将降采样

    Returns:
        降采样后的数据，如果未超过阈值则返回原始数据
    """
    if not _PLOTLY_AVAILABLE:
        return data

    if isinstance(data, pd.Series):
        n_points = len(data)
        if n_points <= max_points:
            return data

        # 计算采样频率
        if isinstance(data.index, pd.DatetimeIndex):
            time_span = (data.index[-1] - data.index[0]).total_seconds()
            target_interval = time_span / max_points
            # 选择合适的重采样频率
            if target_interval < 1:
                freq = "1s"  # 1秒
            elif target_interval < 60:
                freq = f"{int(target_interval)}s"
            elif target_interval < 3600:
                freq = f"{int(target_interval / 60)}T"  # 分钟
            else:
                freq = f"{int(target_interval / 3600)}H"  # 小时

            downsampled = data.resample(freq).mean()
            warnings.warn(
                f"数据点数量 ({n_points}) 超过阈值 ({max_points})，已自动降采样到 {len(downsampled)} 点",
                UserWarning,
                stacklevel=3,
            )
            return downsampled
        else:
            # 非时间索引，使用简单的线性降采样
            step = max(1, n_points // max_points)
            downsampled = data.iloc[::step]
            warnings.warn(
                f"数据点数量 ({n_points}) 超过阈值 ({max_points})，已自动降采样到 {len(downsampled)} 点",
                UserWarning,
                stacklevel=3,
            )
            return downsampled

    elif isinstance(data, pd.DataFrame):
        n_points = len(data)
        if n_points <= max_points:
            return data

        # 计算采样频率
        if isinstance(data.index, pd.DatetimeIndex):
            time_span = (data.index[-1] - data.index[0]).total_seconds()
            target_interval = time_span / max_points
            # 选择合适的重采样频率
            if target_interval < 1:
                freq = "1s"  # 1秒
            elif target_interval < 60:
                freq = f"{int(target_interval)}s"
            elif target_interval < 3600:
                freq = f"{int(target_interval / 60)}T"  # 分钟
            else:
                freq = f"{int(target_interval / 3600)}H"  # 小时

            downsampled = data.resample(freq).mean()
            warnings.warn(
                f"数据点数量 ({n_points}) 超过阈值 ({max_points})，已自动降采样到 {len(downsampled)} 点",
                UserWarning,
                stacklevel=3,
            )
            return downsampled
        else:
            # 非时间索引，使用简单的线性降采样
            step = max(1, n_points // max_points)
            downsampled = data.iloc[::step]
            warnings.warn(
                f"数据点数量 ({n_points}) 超过阈值 ({max_points})，已自动降采样到 {len(downsampled)} 点",
                UserWarning,
                stacklevel=3,
            )
            return downsampled

    return data


def downsample_if_needed(max_points: int = 10000):
    """
    装饰器：自动对 Plotly 绘图函数的数据进行降采样

    用法:
        @downsample_if_needed(max_points=10000)
        def plot_function(data, backend='plotly', max_points=10000, ...):
            # data 已经被降采样（如果需要）
            ...

    Args:
        max_points: 最大数据点数，超过此值将降采样（默认值，会被函数参数覆盖）
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 只对 Plotly 后端进行降采样
            backend = kwargs.get("backend", "plotly")
            if backend == "plotly" and _PLOTLY_AVAILABLE:
                # 获取 max_points 参数（优先使用函数参数，否则使用装饰器默认值）
                func_max_points = kwargs.get("max_points", max_points)

                # 查找数据参数（第一个位置参数通常是 data）
                if args and isinstance(args[0], (pd.DataFrame, pd.Series)):
                    data = args[0]
                    # 检查是否需要降采样
                    n_points = len(data)
                    if n_points > func_max_points:
                        # 进行降采样
                        downsampled = _downsample_for_plotly(data, max_points=func_max_points)
                        # 替换第一个参数
                        args = (downsampled,) + args[1:]
                # 也检查 kwargs 中的 data 参数（某些函数可能使用关键字参数）
                elif "data" in kwargs and isinstance(kwargs["data"], (pd.DataFrame, pd.Series)):
                    data = kwargs["data"]
                    n_points = len(data)
                    if n_points > func_max_points:
                        kwargs["data"] = _downsample_for_plotly(data, max_points=func_max_points)

            return func(*args, **kwargs)

        return wrapper

    return decorator


def _extract_data_from_cache(
    cache_or_data: Union[pd.DataFrame, pd.Series, "AlignedData"],
    time_range: Optional[Union[str, Tuple[str, str]]] = None,
) -> Union[pd.DataFrame, pd.Series]:
    """
    从 cache 或直接数据中提取 DataFrame/Series

    供 AlignedDataCache 的绘图方法内部使用

    Args:
        cache_or_data: AlignedDataCache 对象、DataFrame 或 Series
        time_range: 时间范围（可选）
            - str: 单个时间点，例如 '2025-12-15'
            - Tuple[str, str]: 时间范围，例如 ('2025-12-15', '2025-12-16')
            - None: 使用全部数据

    Returns:
        DataFrame 或 Series
    """
    # 检查是否为 AlignedDataCache
    if hasattr(cache_or_data, "data") and hasattr(cache_or_data, "__getitem__"):
        try:
            # 尝试访问 _reader 属性（AlignedDataCache 特有属性）
            if hasattr(cache_or_data, "_reader"):
                if time_range is None:
                    return cache_or_data.data
                elif isinstance(time_range, str):
                    return cache_or_data.loc[time_range]
                elif isinstance(time_range, tuple) and len(time_range) == 2:
                    return cache_or_data[time_range[0] : time_range[1]]
        except (AttributeError, TypeError, KeyError):
            pass

    # 不是 cache，直接返回
    return cache_or_data


def _auto_plot_config(data: Union[pd.DataFrame, pd.Series]) -> dict:
    """
    自动识别数据类型并返回配置信息

    Args:
        data: DataFrame 或 Series

    Returns:
        包含配置信息的字典
    """
    config = {
        "is_series": isinstance(data, pd.Series),
        "is_dataframe": isinstance(data, pd.DataFrame),
        "has_datetime_index": isinstance(data.index, pd.DatetimeIndex),
        "numeric_columns": [],
        "is_time_series": False,
    }

    if isinstance(data, pd.Series):
        config["numeric_columns"] = [data.name or "value"]
        config["is_time_series"] = isinstance(data.index, pd.DatetimeIndex)
    elif isinstance(data, pd.DataFrame):
        config["numeric_columns"] = data.select_dtypes(include=[np.number]).columns.tolist()
        config["is_time_series"] = isinstance(data.index, pd.DatetimeIndex)

    return config


@downsample_if_needed(max_points=10000)
def plot_timeseries(
    data: Union[pd.DataFrame, pd.Series, "AlignedData"],
    column: Optional[Union[str, List[str]]] = None,
    time_range: Optional[Union[str, Tuple[str, str]]] = None,
    figsize: Tuple[int, int] = (12, 5),
    title: Optional[str] = None,
    ylabel: Optional[str] = None,
    color: str = "steelblue",
    linewidth: float = 1.5,
    alpha: float = 0.8,
    grid: bool = True,
    legend_loc: str = "best",
    fig=None,
    ax: Optional[plt.Axes] = None,
    backend: str = "plotly",
    max_points: int = 10000,
    return_ax: bool = False,  # 用于保持 plot_data 的向后兼容性
    **kwargs,
) -> Union["go.Figure", Tuple[plt.Figure, plt.Axes], plt.Axes]:
    """
    绘制时间序列图（支持交互式 Plotly）

    合并了原 plot_data、plot 和 plot_multi_variables 的功能，支持：
    - Series 输入：直接绘制 Series
    - DataFrame + column=None：自动选择所有数值列（单列或多列绘制）
    - DataFrame + column=str：绘制指定列
    - DataFrame + column=List[str]：在同一图表中绘制多个列
    - AlignedDataCache：直接从 cache 读取数据，支持 time_range 参数

    Args:
        data: DataFrame、Series 或 AlignedDataCache 对象，索引为时间
        time_range: 时间范围（仅当 data 是 AlignedDataCache 时有效）
            - None: 使用全部数据
            - Tuple[str, str]: 时间范围，例如 ('2025-12-15', '2025-12-16')
        column: 要绘制的列名（可选）
            - None: 自动选择所有数值列（Series 时忽略）
            - str: 单个列名
            - List[str]: 多个列名，在同一图表中绘制
        figsize: 图表大小（matplotlib 使用）
        title: 图表标题，默认为列名或多变量时间序列
        ylabel: Y轴标签，默认为列名或 'Value'
        color: 线条颜色（单列时使用，多列时忽略）
        linewidth: 线条宽度
        alpha: 透明度
        grid: 是否显示网格
        legend_loc: 图例位置（仅用于 matplotlib，多列时有效）
        fig: 已有的 Figure 对象（Plotly figure 或 matplotlib Figure，可选）
        ax: 已有的 Axes 对象（仅用于 matplotlib，可选）
        backend: 'plotly' 或 'matplotlib'，默认 'plotly'
        max_points: 自动降采样阈值，默认 10000（仅用于 Plotly）
        return_ax: 是否只返回 ax（用于保持 plot_data 的向后兼容性）
        **kwargs: 其他参数（传递给 matplotlib）

    Returns:
        - Plotly: go.Figure 对象
        - Matplotlib: (fig, ax) 元组，或仅 ax（当 return_ax=True 时）

    Examples:
        >>> # Series 输入
        >>> fig = plot_timeseries(series_data)
        >>> fig.show()

        >>> # DataFrame + 单列
        >>> fig = plot_timeseries(data, 'temperature')
        >>> fig.show()

        >>> # DataFrame + 自动选择列
        >>> fig = plot_timeseries(data)  # 自动选择所有数值列
        >>> fig.show()

        >>> # DataFrame + 多列（在同一图表中绘制）
        >>> fig = plot_timeseries(data, ['temp', 'pressure'])
        >>> fig.show()

        >>> # 使用 AlignedDataCache
        >>> fig = plot_timeseries(cache, 'tempdata__Temperature')
        >>> fig.show()

        >>> # 指定时间范围
        >>> fig = plot_timeseries(
        ...     cache,
        ...     'tempdata__Temperature',
        ...     time_range=('2025-12-15', '2025-12-16')
        ... )
        >>> fig.show()
    """
    # 提取数据（如果是 cache）
    data = _extract_data_from_cache(data, time_range)

    # 处理 Series 输入
    if isinstance(data, pd.Series):
        y = data
        ylabel_name = ylabel or (y.name if y.name is not None else "value")

        if backend == "plotly" and _PLOTLY_AVAILABLE:
            if fig is None:
                fig = go.Figure()

            fig.add_trace(
                go.Scatter(
                    x=y.index,
                    y=y.values,
                    mode="lines",
                    name=ylabel_name,
                    line=dict(color=color, width=linewidth),
                    opacity=alpha,
                )
            )

            fig.update_layout(
                title=title or ylabel_name,
                xaxis_title="Time",
                yaxis_title=ylabel_name,
                template="plotly_white",
                hovermode="x unified",
                width=figsize[0] * 100,
                height=figsize[1] * 100,
                showlegend=True,
            )

            if not grid:
                fig.update_xaxis(showgrid=False)
                fig.update_yaxis(showgrid=False)

            return fig
        else:
            # matplotlib 后端
            if backend == "plotly" and not _PLOTLY_AVAILABLE:
                warnings.warn("Plotly 不可用，已切换到 matplotlib 后端", UserWarning, stacklevel=2)

            if fig is None or ax is None:
                fig, ax = plt.subplots(figsize=figsize)

            plot_kwargs = {"alpha": alpha, "linewidth": linewidth, "color": color}
            plot_kwargs.update(kwargs)
            ax.plot(y.index, y.values, **plot_kwargs)

            ax.set_title(title or ylabel_name, fontsize=14, fontweight="bold")
            ax.set_xlabel("Time", fontsize=12)
            ax.set_ylabel(ylabel_name, fontsize=12)

            if grid:
                ax.grid(True, alpha=0.3)

            fig.autofmt_xdate()
            plt.tight_layout()

            if return_ax:
                return ax
            return fig, ax

    # 处理 DataFrame 输入
    elif isinstance(data, pd.DataFrame):
        # 处理 column 参数
        if column is None:
            # 自动选择所有数值列
            numeric_cols = data.select_dtypes(include=[np.number]).columns.tolist()
            if len(numeric_cols) == 0:
                raise ValueError("DataFrame 中没有数值列可绘制")
            elif len(numeric_cols) == 1:
                # 单列：继续绘制
                column = numeric_cols[0]
            else:
                # 多列：直接绘制
                columns = numeric_cols
                # 多列绘制逻辑
                if backend == "plotly" and _PLOTLY_AVAILABLE:
                    # Plotly 后端（数据已由装饰器降采样）
                    data_selected = data[columns]

                    if fig is None:
                        fig = go.Figure()

                    # 为每列添加轨迹
                    for col in columns:
                        fig.add_trace(
                            go.Scatter(
                                x=data_selected.index,
                                y=data_selected[col],
                                mode="lines",
                                name=col,
                                line=dict(width=linewidth),
                                opacity=alpha,
                            )
                        )

                    fig.update_layout(
                        title=title or "Multi-Variable Time Series",
                        xaxis_title="Time",
                        yaxis_title=ylabel or "Value",
                        template="plotly_white",
                        hovermode="x unified",
                        width=figsize[0] * 100,
                        height=figsize[1] * 100,
                        showlegend=True,
                    )

                    if not grid:
                        fig.update_xaxis(showgrid=False)
                        fig.update_yaxis(showgrid=False)

                    return fig
                else:
                    # matplotlib 后端
                    if backend == "plotly" and not _PLOTLY_AVAILABLE:
                        warnings.warn("Plotly 不可用，已切换到 matplotlib 后端", UserWarning, stacklevel=2)

                    if fig is None or ax is None:
                        fig, ax = plt.subplots(figsize=figsize)

                    for col in columns:
                        ax.plot(data.index, data[col], label=col, linewidth=linewidth, alpha=alpha)

                    ax.set_title(title or "Multi-Variable Time Series", fontsize=14, fontweight="bold")
                    ax.set_xlabel("Time", fontsize=12)
                    ax.set_ylabel(ylabel or "Value", fontsize=12)
                    ax.legend(loc=legend_loc, fontsize=10)

                    if grid:
                        ax.grid(True, alpha=0.3)

                    fig.autofmt_xdate()
                    plt.tight_layout()

                    if return_ax:
                        return ax
                    return fig, ax
        elif isinstance(column, list):
            if len(column) == 1:
                # 单列列表：提取列名
                column = column[0]
            else:
                # 多列列表：直接绘制
                columns = column
                # 多列绘制逻辑
                if backend == "plotly" and _PLOTLY_AVAILABLE:
                    # Plotly 后端（数据已由装饰器降采样）
                    data_selected = data[columns]

                    if fig is None:
                        fig = go.Figure()

                    # 为每列添加轨迹
                    for col in columns:
                        fig.add_trace(
                            go.Scatter(
                                x=data_selected.index,
                                y=data_selected[col],
                                mode="lines",
                                name=col,
                                line=dict(width=linewidth),
                                opacity=alpha,
                            )
                        )

                    fig.update_layout(
                        title=title or "Multi-Variable Time Series",
                        xaxis_title="Time",
                        yaxis_title=ylabel or "Value",
                        template="plotly_white",
                        hovermode="x unified",
                        width=figsize[0] * 100,
                        height=figsize[1] * 100,
                        showlegend=True,
                    )

                    if not grid:
                        fig.update_xaxis(showgrid=False)
                        fig.update_yaxis(showgrid=False)

                    return fig
                else:
                    # matplotlib 后端
                    if backend == "plotly" and not _PLOTLY_AVAILABLE:
                        warnings.warn("Plotly 不可用，已切换到 matplotlib 后端", UserWarning, stacklevel=2)

                    if fig is None or ax is None:
                        fig, ax = plt.subplots(figsize=figsize)

                    for col in columns:
                        ax.plot(data.index, data[col], label=col, linewidth=linewidth, alpha=alpha)

                    ax.set_title(title or "Multi-Variable Time Series", fontsize=14, fontweight="bold")
                    ax.set_xlabel("Time", fontsize=12)
                    ax.set_ylabel(ylabel or "Value", fontsize=12)
                    ax.legend(loc=legend_loc, fontsize=10)

                    if grid:
                        ax.grid(True, alpha=0.3)

                    fig.autofmt_xdate()
                    plt.tight_layout()

                    if return_ax:
                        return ax
                    return fig, ax

        # 单列绘制
        if isinstance(column, str):
            if column not in data.columns:
                raise KeyError(f"列不存在: {column!r}，可用列: {list(data.columns)}")

            y = data[column]
            ylabel_name = ylabel or column

            if backend == "plotly" and _PLOTLY_AVAILABLE:
                if fig is None:
                    fig = go.Figure()

                fig.add_trace(
                    go.Scatter(
                        x=y.index,
                        y=y.values,
                        mode="lines",
                        name=column,
                        line=dict(color=color, width=linewidth),
                        opacity=alpha,
                    )
                )

                fig.update_layout(
                    title=title or f"{column} vs Time",
                    xaxis_title="Time",
                    yaxis_title=ylabel_name,
                    template="plotly_white",
                    hovermode="x unified",
                    width=figsize[0] * 100,
                    height=figsize[1] * 100,
                    showlegend=True,
                )

                if not grid:
                    fig.update_xaxis(showgrid=False)
                    fig.update_yaxis(showgrid=False)

                return fig
            else:
                # matplotlib 后端
                if backend == "plotly" and not _PLOTLY_AVAILABLE:
                    warnings.warn("Plotly 不可用，已切换到 matplotlib 后端", UserWarning, stacklevel=2)

                if fig is None or ax is None:
                    fig, ax = plt.subplots(figsize=figsize)

                ax.plot(data.index, data[column], color=color, linewidth=linewidth, alpha=alpha)

                ax.set_title(title or f"{column} vs Time", fontsize=14, fontweight="bold")
                ax.set_xlabel("Time", fontsize=12)
                ax.set_ylabel(ylabel_name, fontsize=12)

                if grid:
                    ax.grid(True, alpha=0.3)

                fig.autofmt_xdate()
                plt.tight_layout()

                if return_ax:
                    return ax
                return fig, ax
        else:
            raise TypeError(f"不支持的 column 类型: {type(column)}")
    else:
        raise TypeError(f"不支持的数据类型: {type(data)}")


@downsample_if_needed(max_points=10000)
def plot_dual_axis(
    data: Union[pd.DataFrame, "AlignedData"],
    left_column: str,
    right_column: str,
    time_range: Optional[Union[str, Tuple[str, str]]] = None,
    figsize: Tuple[int, int] = (12, 6),
    title: str = "Dual Axis Time Series",
    left_ylabel: Optional[str] = None,
    right_ylabel: Optional[str] = None,
    left_color: str = "steelblue",
    right_color: str = "coral",
    linewidth: float = 1.5,
    alpha: float = 0.8,
    grid: bool = True,
    fig=None,
    backend: str = "plotly",
    max_points: int = 10000,
) -> Union["go.Figure", Tuple[plt.Figure, plt.Axes, plt.Axes]]:
    """
    绘制双Y轴图表（用于不同量级或单位的参数，支持交互式 Plotly）

    Args:
        data: 包含时间序列数据的 DataFrame 或 AlignedDataCache 对象，索引为时间
        time_range: 时间范围（仅当 data 是 AlignedDataCache 时有效）
        left_column: 左Y轴的列名
        right_column: 右Y轴的列名
        figsize: 图表大小（matplotlib 使用）
        title: 图表标题
        left_ylabel: 左Y轴标签
        right_ylabel: 右Y轴标签
        left_color: 左轴线条颜色
        right_color: 右轴线条颜色
        linewidth: 线条宽度
        alpha: 透明度
        grid: 是否显示网格
        fig: 已有的 Figure 对象（Plotly figure 或 matplotlib Figure，可选）
        backend: 'plotly' 或 'matplotlib'，默认 'plotly'
        max_points: 自动降采样阈值，默认 10000（仅用于 Plotly）

    Returns:
        Plotly figure 对象（backend='plotly'）或 (fig, ax1, ax2) 元组（backend='matplotlib'）

    Examples:
        >>> fig = plot_dual_axis(data, 'temperature', 'heater_power')  # Plotly
        >>> fig.show()
        >>> fig, ax1, ax2 = plot_dual_axis(data, 'temperature', 'heater_power', backend='matplotlib')  # Matplotlib
        >>> plt.show()

        >>> # 使用 AlignedDataCache
        >>> fig = plot_dual_axis(
        ...     cache,
        ...     'tempdata__Temperature',
        ...     'runlidata__Pressure1',
        ...     time_range=('2025-12-15', '2025-12-16')
        ... )
        >>> fig.show()
    """
    # 提取数据（如果是 cache）
    data = _extract_data_from_cache(data, time_range)

    if backend == "plotly" and _PLOTLY_AVAILABLE:
        # Plotly 后端（原生支持双Y轴，数据已由装饰器降采样）
        data_downsampled = data[[left_column, right_column]]

        if fig is None:
            fig = go.Figure()

        # 左Y轴
        fig.add_trace(
            go.Scatter(
                x=data_downsampled.index,
                y=data_downsampled[left_column],
                mode="lines",
                name=left_column,
                line=dict(color=left_color, width=linewidth),
                opacity=alpha,
                yaxis="y",
            )
        )

        # 右Y轴
        fig.add_trace(
            go.Scatter(
                x=data_downsampled.index,
                y=data_downsampled[right_column],
                mode="lines",
                name=right_column,
                line=dict(color=right_color, width=linewidth),
                opacity=alpha,
                yaxis="y2",
            )
        )

        # 配置布局，添加第二个Y轴
        layout_config = {
            "title": title,
            "xaxis_title": "Time",
            "yaxis": dict(
                title=dict(text=left_ylabel or left_column, font=dict(color=left_color)),
                tickfont=dict(color=left_color),
                side="left",
                showgrid=grid,
            ),
            "yaxis2": dict(
                title=dict(text=right_ylabel or right_column, font=dict(color=right_color)),
                tickfont=dict(color=right_color),
                anchor="x",
                overlaying="y",
                side="right",
                showgrid=grid,
            ),
            "template": "plotly_white",
            "hovermode": "x unified",
            "width": figsize[0] * 100,
            "height": figsize[1] * 100,
            "showlegend": True,
        }

        if not grid:
            layout_config["xaxis"] = dict(showgrid=False)

        fig.update_layout(**layout_config)

        return fig
    else:
        # matplotlib 后端（保持向后兼容）
        if backend == "plotly" and not _PLOTLY_AVAILABLE:
            warnings.warn("Plotly 不可用，已切换到 matplotlib 后端", UserWarning, stacklevel=2)

        fig, ax1 = plt.subplots(figsize=figsize)

        # 左Y轴
        ax1.plot(data.index, data[left_column], color=left_color, linewidth=linewidth, alpha=alpha, label=left_column)
        ax1.set_xlabel("Time", fontsize=12)
        ax1.set_ylabel(left_ylabel or left_column, fontsize=12, color=left_color)
        ax1.tick_params(axis="y", labelcolor=left_color)

        # 右Y轴
        ax2 = ax1.twinx()
        ax2.plot(
            data.index, data[right_column], color=right_color, linewidth=linewidth, alpha=alpha, label=right_column
        )
        ax2.set_ylabel(right_ylabel or right_column, fontsize=12, color=right_color)
        ax2.tick_params(axis="y", labelcolor=right_color)

        # 标题和图例
        ax1.set_title(title, fontsize=14, fontweight="bold")

        # 合并图例
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=10)

        if grid:
            ax1.grid(True, alpha=0.3)

        fig.autofmt_xdate()
        plt.tight_layout()

        return fig, ax1, ax2


@downsample_if_needed(max_points=10000)
def plot_subplots(
    data: Union[pd.DataFrame, "AlignedData"],
    columns: Optional[List[str]] = None,
    column_groups: Optional[List[List[str]]] = None,
    time_range: Optional[Union[str, Tuple[str, str]]] = None,
    nrows: Optional[int] = None,
    ncols: int = 2,
    figsize: Tuple[int, int] = (14, 10),
    suptitle: str = "Multi-Parameter Subplots",
    subplot_titles: Optional[List[str]] = None,
    linewidth: float = 1.5,
    alpha: float = 0.8,
    grid: bool = True,
    shared_xaxes: bool = False,
    colors: Optional[Union[str, List[str]]] = None,
    fig=None,
    backend: str = "plotly",
    max_points: int = 10000,
) -> Union["go.Figure", Tuple[plt.Figure, np.ndarray]]:
    """
    创建子图布局（多个参数分别显示，支持交互式 Plotly）

    支持两种模式：
    1. 单列模式（columns）：每个子图显示一个列，支持多列布局
    2. 列组模式（column_groups）：每个子图显示一个列组的所有列，自动同步 X 轴

    Args:
        data: 包含时间序列数据的 DataFrame 或 AlignedDataCache 对象，索引为时间
        time_range: 时间范围（仅当 data 是 AlignedDataCache 时有效）
        columns: 要绘制的列名列表（单列模式，与 column_groups 互斥）
        column_groups: 列组列表，每个元素是一组要绘制的列名列表（列组模式，与 columns 互斥）
            例如：`[['Temperature1', 'Temperature2'], ['Pressure1', 'Pressure2']]`
        nrows: 子图行数，默认自动计算（单列模式）或列组数量（列组模式）
        ncols: 子图列数，默认 2（单列模式）或 1（列组模式）
        figsize: 图表大小（matplotlib 使用）
        suptitle: 总标题
        subplot_titles: 每个子图的标题列表（可选，列组模式时推荐使用）
        linewidth: 线条宽度
        alpha: 透明度
        grid: 是否显示网格
        shared_xaxes: 是否同步所有子图的 X 轴（默认 False，列组模式时自动设为 True）
        colors: 颜色配置（仅列组模式）
            - None: 自动为每个列组内的列生成不同颜色
            - str: 单个颜色字符串，所有列使用相同颜色
            - List[str]: 颜色列表，为每个列组内的列指定颜色
        fig: 已有的 Figure 对象（Plotly figure 或 matplotlib Figure，可选）
        backend: 'plotly' 或 'matplotlib'，默认 'plotly'
        max_points: 自动降采样阈值，默认 10000（仅用于 Plotly）

    Returns:
        Plotly figure 对象（backend='plotly'）或 (fig, axes) 元组（backend='matplotlib'）

    Examples:
        >>> # 单列模式：每个子图显示一个列
        >>> fig = plot_subplots(data, columns=['temperature', 'pressure', 'flow', 'heater_power'])
        >>> fig.show()

        >>> # 列组模式：每个子图显示一个列组，X 轴同步
        >>> fig = plot_subplots(
        ...     data,
        ...     column_groups=[
        ...         ['Temperature1', 'Temperature2'],
        ...         ['Pressure1', 'Pressure2']
        ...     ],
        ...     subplot_titles=['Temperature', 'Pressure']
        ... )
        >>> fig.show()

        >>> # 使用 AlignedDataCache
        >>> fig = plot_subplots(
        ...     cache,
        ...     column_groups=[
        ...         ['tempdata__Temperature1', 'tempdata__Temperature2'],
        ...         ['runlidata__Pressure1', 'runlidata__Pressure2']
        ...     ],
        ...     time_range=('2025-12-15', '2025-12-16')
        ... )
        >>> fig.show()
    """
    # 提取数据（如果是 cache）
    data = _extract_data_from_cache(data, time_range)

    # 参数验证
    if column_groups is not None and columns is not None:
        raise ValueError("不能同时指定 columns 和 column_groups")
    if column_groups is None and columns is None:
        raise ValueError("必须指定 columns 或 column_groups 之一")

    # 处理颜色（列组模式使用）
    if colors is None:
        try:
            import plotly.colors as pc

            default_colors = pc.qualitative.Plotly
        except ImportError:
            default_colors = [
                "steelblue",
                "coral",
                "green",
                "orange",
                "purple",
                "brown",
                "pink",
                "gray",
                "olive",
                "cyan",
            ]
    elif isinstance(colors, str):
        default_colors = [colors]
    else:
        default_colors = colors

    # 列组模式
    if column_groups is not None:
        if not column_groups:
            raise ValueError("column_groups 不能为空")

        # 验证所有列都存在
        for group_idx, group_columns in enumerate(column_groups):
            if not group_columns:
                raise ValueError(f"列组 {group_idx + 1} 为空")
            for col in group_columns:
                if col not in data.columns:
                    raise KeyError(f"列不存在: {col!r}，可用列: {list(data.columns)}")

        nrows = len(column_groups)
        ncols = 1
        if not shared_xaxes:
            shared_xaxes = True  # 列组模式默认同步 X 轴

        # 处理子图标题
        if subplot_titles is None:
            subplot_titles = [f"Group {i + 1}" for i in range(nrows)]
        elif len(subplot_titles) != nrows:
            raise ValueError(f"subplot_titles 长度 ({len(subplot_titles)}) 应与 column_groups 长度 ({nrows}) 一致")

        if backend == "plotly" and _PLOTLY_AVAILABLE:
            # Plotly 后端（数据已由装饰器降采样）
            # 动态调整高度：每行约 350 像素
            height = max(400, nrows * 350)

            fig = make_subplots(
                rows=nrows,
                cols=ncols,
                shared_xaxes=shared_xaxes,
                vertical_spacing=0.08,
                subplot_titles=subplot_titles,
            )

            # 为每个列组添加轨迹
            for row_idx, group_columns in enumerate(column_groups, start=1):
                for col_idx, col_name in enumerate(group_columns):
                    color = default_colors[col_idx % len(default_colors)]
                    fig.add_trace(
                        go.Scatter(
                            x=data.index,
                            y=data[col_name],
                            mode="lines",
                            name=col_name,
                            line=dict(width=linewidth, color=color),
                            opacity=alpha,
                            showlegend=True,
                        ),
                        row=row_idx,
                        col=1,
                    )

                # 更新 Y 轴标签
                fig.update_yaxes(title_text=subplot_titles[row_idx - 1], row=row_idx, col=1)

                if not grid:
                    fig.update_xaxes(showgrid=False, row=row_idx, col=1)
                    fig.update_yaxes(showgrid=False, row=row_idx, col=1)

            # 只在最后一个子图显示 X 轴标题
            fig.update_xaxes(title_text="Time", row=nrows, col=1)

            fig.update_layout(
                title=suptitle,
                template="plotly_white",
                hovermode="x unified",
                width=figsize[0] * 100,
                height=height,
                showlegend=True,
            )

            return fig
        else:
            # matplotlib 后端
            if backend == "plotly" and not _PLOTLY_AVAILABLE:
                warnings.warn("Plotly 不可用，已切换到 matplotlib 后端", UserWarning, stacklevel=2)

            # 动态调整高度：每行约 3 英寸
            height = max(6, nrows * 3)
            fig, axes = plt.subplots(nrows, 1, figsize=(figsize[0], height), sharex=shared_xaxes)

            # 确保 axes 是一维数组
            if nrows == 1:
                axes = [axes]

            # 为每个列组绘制数据
            for ax_idx, (group_columns, subplot_title) in enumerate(zip(column_groups, subplot_titles)):
                ax = axes[ax_idx]

                for col_idx, col_name in enumerate(group_columns):
                    color = default_colors[col_idx % len(default_colors)]
                    ax.plot(
                        data.index,
                        data[col_name],
                        label=col_name,
                        linewidth=linewidth,
                        alpha=alpha,
                        color=color,
                    )

                ax.set_title(subplot_title, fontsize=12, fontweight="bold")
                ax.set_ylabel(subplot_title, fontsize=10)

                if grid:
                    ax.grid(True, alpha=0.3)

                if len(group_columns) > 1:
                    ax.legend(loc="best", fontsize=8)

                # 旋转x轴标签
                for label in ax.get_xticklabels():
                    label.set_rotation(30)
                    label.set_ha("right")

            # 只在最后一个子图显示 X 轴标签
            axes[-1].set_xlabel("Time", fontsize=12)

            fig.suptitle(suptitle, fontsize=16, fontweight="bold", y=0.995)
            plt.tight_layout()

            return fig, np.array(axes)

    # 单列模式（原有逻辑）
    n_plots = len(columns)
    if nrows is None:
        nrows = (n_plots + ncols - 1) // ncols

    if backend == "plotly" and _PLOTLY_AVAILABLE:
        # Plotly 后端（数据已由装饰器降采样）
        data_downsampled = data[columns]

        if fig is None:
            fig = make_subplots(
                rows=nrows,
                cols=ncols,
                shared_xaxes=shared_xaxes,
                subplot_titles=columns,
                vertical_spacing=0.08,
                horizontal_spacing=0.08,
            )

        for idx, column in enumerate(columns):
            row = (idx // ncols) + 1
            col = (idx % ncols) + 1

            fig.add_trace(
                go.Scatter(
                    x=data_downsampled.index,
                    y=data_downsampled[column],
                    mode="lines",
                    name=column,
                    line=dict(width=linewidth),
                    opacity=alpha,
                    showlegend=False,
                ),
                row=row,
                col=col,
            )

            fig.update_xaxes(title_text="Time", row=row, col=col)
            fig.update_yaxes(title_text=column, row=row, col=col)

            if not grid:
                fig.update_xaxes(showgrid=False, row=row, col=col)
                fig.update_yaxes(showgrid=False, row=row, col=col)

        fig.update_layout(
            title=suptitle,
            template="plotly_white",
            hovermode="x unified",
            width=figsize[0] * 100,
            height=figsize[1] * 100,
        )

        return fig
    else:
        # matplotlib 后端（保持向后兼容）
        if backend == "plotly" and not _PLOTLY_AVAILABLE:
            warnings.warn("Plotly 不可用，已切换到 matplotlib 后端", UserWarning, stacklevel=2)

        fig, axes = plt.subplots(nrows, ncols, figsize=figsize, sharex=shared_xaxes)

        # 确保 axes 是二维数组
        if nrows == 1 and ncols == 1:
            axes = np.array([[axes]])
        elif nrows == 1 or ncols == 1:
            axes = axes.reshape(nrows, ncols)

        for idx, column in enumerate(columns):
            row = idx // ncols
            col = idx % ncols
            ax = axes[row, col]

            ax.plot(data.index, data[column], linewidth=linewidth, alpha=alpha)
            ax.set_title(column, fontsize=12, fontweight="bold")
            ax.set_xlabel("Time", fontsize=10)
            ax.set_ylabel(column, fontsize=10)

            if grid:
                ax.grid(True, alpha=0.3)

            # 旋转x轴标签
            for label in ax.get_xticklabels():
                label.set_rotation(30)
                label.set_ha("right")

        # 隐藏多余的子图
        for idx in range(n_plots, nrows * ncols):
            row = idx // ncols
            col = idx % ncols
            axes[row, col].axis("off")

        fig.suptitle(suptitle, fontsize=16, fontweight="bold", y=0.995)
        plt.tight_layout()

        return fig, axes


def plot_temp_pressure_sync(
    data: Union[pd.DataFrame, "AlignedData"],
    temp_columns: Optional[List[str]] = None,
    pressure_columns: Optional[List[str]] = None,
    time_range: Optional[Union[str, Tuple[str, str]]] = None,
    return_overview: bool = False,
    **kwargs,
) -> Union["go.Figure", Tuple[plt.Figure, Tuple[plt.Axes, plt.Axes]]]:
    """
    绘制同步的温度和压强子图（便捷函数）

    自动识别温度和压强列，然后调用 plot_subplots 创建同步子图。

    Args:
        data: 包含时间序列数据的 DataFrame 或 AlignedDataCache 对象，索引为时间
        temp_columns: 温度列名列表（可选，如果为 None 则自动识别）
        pressure_columns: 压强列名列表（可选，如果为 None 则自动识别）
        time_range: 时间范围（仅当 data 是 AlignedDataCache 时有效）
        return_overview: 是否返回额外的总览图（温度总览 + 压强总览）
        **kwargs: 其他参数传递给 plot_subplots

    Returns:
        - return_overview=False: Plotly figure 或 (fig, (ax1, ax2))
        - return_overview=True: (sync_fig, pressure_fig, temperature_fig)

    Examples:
        >>> # 自动识别温度和压强
        >>> fig = plot_temp_pressure_sync(data)
        >>> fig.show()

        >>> # 手动指定列
        >>> fig = plot_temp_pressure_sync(
        ...     data,
        ...     temp_columns=['A_Temperature', 'B_Temperature'],
        ...     pressure_columns=['Pressure1', 'Pressure2']
        ... )
        >>> fig.show()

        >>> # 使用 AlignedDataCache
        >>> fig = plot_temp_pressure_sync(
        ...     cache,
        ...     time_range=('2025-12-15', '2025-12-16')
        ... )
        >>> fig.show()

        >>> # 返回额外总览图（压强 + 温度）
        >>> sync_fig, p_fig, t_fig = plot_temp_pressure_sync(
        ...     cache,
        ...     time_range=('2025-12-15', '2025-12-16'),
        ...     return_overview=True
        ... )
        >>> p_fig.show(); t_fig.show()
    """
    # 提取数据（如果是 cache，但先不提取，因为需要识别列）
    # 先识别列，再提取数据
    if hasattr(data, "_reader"):  # AlignedDataCache
        cache = data
        # 使用全部数据识别列
        data_for_columns = cache.data
    else:
        cache = None
        data_for_columns = data

    # 自动识别温度列
    if temp_columns is None:
        temp_columns = [
            col
            for col in data_for_columns.columns
            if any(keyword in col.lower() for keyword in ["temp", "temperature"])
        ]
        if not temp_columns:
            raise ValueError("未找到温度列，请手动指定 temp_columns 参数")

    # 自动识别压强列
    if pressure_columns is None:
        pressure_columns = [
            col for col in data_for_columns.columns if any(keyword in col.lower() for keyword in ["pressure", "press"])
        ]
        if not pressure_columns:
            raise ValueError("未找到压强列，请手动指定 pressure_columns 参数")

    # 提取数据（如果是 cache）
    if cache is not None:
        data = _extract_data_from_cache(cache, time_range)

    # 设置默认标题
    if "subplot_titles" not in kwargs:
        kwargs["subplot_titles"] = ["Temperature", "Pressure"]
    if "suptitle" not in kwargs:
        kwargs["suptitle"] = "Temperature and Pressure (Synchronized)"

    # 调用合并后的函数
    sync_fig = plot_subplots(data, column_groups=[temp_columns, pressure_columns], shared_xaxes=True, **kwargs)

    if not return_overview:
        return sync_fig

    backend = kwargs.get("backend", "plotly")
    max_points = kwargs.get("max_points", 10000)

    pressure_fig = plot_timeseries(
        data,
        pressure_columns,
        backend=backend,
        max_points=max_points,
        title="All Pressure",
        ylabel="Pressure",
    )
    temperature_fig = plot_timeseries(
        data,
        temp_columns,
        backend=backend,
        max_points=max_points,
        title="All Temperature",
        ylabel="Temperature",
    )

    return sync_fig, pressure_fig, temperature_fig


def plot_distribution(
    data: pd.DataFrame,
    column: str,
    bins: int = 30,
    figsize: Tuple[int, int] = (10, 6),
    title: Optional[str] = None,
    xlabel: Optional[str] = None,
    color: str = "steelblue",
    alpha: float = 0.7,
    kde: bool = True,
    fig: Optional[plt.Figure] = None,
    ax: Optional[plt.Axes] = None,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    绘制数据分布直方图

    Args:
        data: 包含数据的 DataFrame
        column: 要分析的列名
        bins: 直方图的柱数
        figsize: 图表大小
        title: 图表标题
        xlabel: X轴标签
        color: 颜色
        alpha: 透明度
        kde: 是否显示核密度估计曲线
        fig: 已有的 Figure 对象（可选）
        ax: 已有的 Axes 对象（可选）

    Returns:
        fig, ax: matplotlib 的 Figure 和 Axes 对象

    Examples:
        >>> fig, ax = plot_distribution(data, 'temperature', bins=50)
        >>> plt.show()
    """
    if fig is None or ax is None:
        fig, ax = plt.subplots(figsize=figsize)

    # 绘制直方图
    ax.hist(data[column].dropna(), bins=bins, color=color, alpha=alpha, edgecolor="black")

    # 添加 KDE 曲线
    if kde:
        ax2 = ax.twinx()
        data[column].dropna().plot.kde(ax=ax2, color="red", linewidth=2)
        ax2.set_ylabel("Density", fontsize=12)

    ax.set_title(title or f"Distribution of {column}", fontsize=14, fontweight="bold")
    ax.set_xlabel(xlabel or column, fontsize=12)
    ax.set_ylabel("Frequency", fontsize=12)
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()

    return fig, ax


def plot_boxplot(
    data: pd.DataFrame,
    columns: Union[str, List[str]],
    figsize: Tuple[int, int] = (10, 6),
    title: str = "Box Plot",
    ylabel: str = "Value",
    showfliers: bool = True,
    fig: Optional[plt.Figure] = None,
    ax: Optional[plt.Axes] = None,
) -> Union["go.Figure", Tuple[plt.Figure, plt.Axes]]:
    """
    绘制箱线图（用于异常值检测）

    Args:
        data: 包含数据的 DataFrame
        columns: 要分析的列名或列名列表
        figsize: 图表大小
        title: 图表标题
        ylabel: Y轴标签
        showfliers: 是否显示异常值点
        fig: 已有的 Figure 对象（可选）
        ax: 已有的 Axes 对象（可选）

    Returns:
        fig, ax: matplotlib 的 Figure 和 Axes 对象

    Examples:
        >>> fig, ax = plot_boxplot(data, ['temperature', 'pressure', 'flow'])
        >>> plt.show()
    """
    if fig is None or ax is None:
        fig, ax = plt.subplots(figsize=figsize)

    if isinstance(columns, str):
        columns = [columns]

    data[columns].boxplot(ax=ax, showfliers=showfliers, patch_artist=True)

    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_ylabel(ylabel, fontsize=12)
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()

    return fig, ax


def plot_correlation(
    data: pd.DataFrame,
    columns: Optional[List[str]] = None,
    figsize: Tuple[int, int] = (10, 8),
    title: str = "Correlation Heatmap",
    cmap: str = "coolwarm",
    annot: bool = True,
    fmt: str = ".2f",
    fig=None,
    ax: Optional[plt.Axes] = None,
    backend: str = "plotly",
) -> Union["go.Figure", Tuple[plt.Figure, plt.Axes]]:
    """
    绘制相关性热力图（支持交互式 Plotly）

    Args:
        data: 包含数据的 DataFrame
        columns: 要分析的列名列表，默认使用所有数值列
        figsize: 图表大小（matplotlib 使用）
        title: 图表标题
        cmap: 颜色映射（matplotlib）或颜色标度（plotly）
        annot: 是否在每个单元格中显示数值
        fmt: 数值格式
        fig: 已有的 Figure 对象（Plotly figure 或 matplotlib Figure，可选）
        ax: 已有的 Axes 对象（仅用于 matplotlib，可选）
        backend: 'plotly' 或 'matplotlib'，默认 'plotly'

    Returns:
        Plotly figure 对象（backend='plotly'）或 (fig, ax) 元组（backend='matplotlib'）

    Examples:
        >>> fig = plot_correlation(data, ['temperature', 'pressure', 'flow', 'heater_power'])  # Plotly
        >>> fig.show()
        >>> fig, ax = plot_correlation(data, columns=['temp', 'pressure'], backend='matplotlib')  # Matplotlib
        >>> plt.show()
    """
    if columns is None:
        # 使用所有数值列
        corr_data = data.select_dtypes(include=[np.number])
    else:
        corr_data = data[columns]

    corr_matrix = corr_data.corr()

    if backend == "plotly" and _PLOTLY_AVAILABLE:
        # Plotly 后端
        # 映射颜色标度
        color_scale_map = {
            "coolwarm": "RdBu",
            "viridis": "Viridis",
            "plasma": "Plasma",
            "RdYlBu": "RdYlBu",
        }
        colorscale = color_scale_map.get(cmap, "RdBu")

        if fig is None:
            fig = go.Figure()

        # 创建热力图
        fig.add_trace(
            go.Heatmap(
                z=corr_matrix.values,
                x=corr_matrix.columns,
                y=corr_matrix.columns,
                colorscale=colorscale,
                text=corr_matrix.values if annot else None,
                texttemplate=f"%.{fmt[1:]}f" if annot and fmt.startswith(".") else None,
                textfont={"size": 10},
                colorbar=dict(title="Correlation"),
                zmid=0,
            )
        )

        fig.update_layout(
            title=title,
            template="plotly_white",
            width=figsize[0] * 100,
            height=figsize[1] * 100,
            xaxis=dict(side="bottom"),
            yaxis=dict(autorange="reversed"),
        )

        return fig
    else:
        # matplotlib 后端（保持向后兼容）
        if backend == "plotly" and not _PLOTLY_AVAILABLE:
            warnings.warn("Plotly 不可用，已切换到 matplotlib 后端", UserWarning, stacklevel=2)

        if fig is None or ax is None:
            fig, ax = plt.subplots(figsize=figsize)

        sns.heatmap(
            corr_matrix,
            annot=annot,
            fmt=fmt,
            cmap=cmap,
            center=0,
            square=True,
            linewidths=1,
            cbar_kws={"shrink": 0.8},
            ax=ax,
        )

        ax.set_title(title, fontsize=14, fontweight="bold", pad=20)

        plt.tight_layout()

        return fig, ax


@downsample_if_needed(max_points=10000)
def plot_rolling_stats(
    data: Union[pd.DataFrame, "AlignedData"],
    column: str,
    time_range: Optional[Union[str, Tuple[str, str]]] = None,
    window: int = 24,
    figsize: Tuple[int, int] = (12, 6),
    title: Optional[str] = None,
    ylabel: Optional[str] = None,
    plot_std: bool = True,
    alpha: float = 0.7,
    grid: bool = True,
    fig=None,
    ax: Optional[plt.Axes] = None,
    backend: str = "plotly",
    max_points: int = 10000,
) -> Union["go.Figure", Tuple[plt.Figure, plt.Axes]]:
    """
    绘制滚动统计图（移动平均和标准差，支持交互式 Plotly）

    Args:
        data: 包含时间序列数据的 DataFrame 或 AlignedDataCache 对象，索引为时间
        column: 要分析的列名
        time_range: 时间范围（仅当 data 是 AlignedDataCache 时有效）
        window: 滚动窗口大小
        figsize: 图表大小（matplotlib 使用）
        title: 图表标题
        ylabel: Y轴标签
        plot_std: 是否绘制标准差带
        alpha: 透明度
        grid: 是否显示网格
        fig: 已有的 Figure 对象（Plotly figure 或 matplotlib Figure，可选）
        ax: 已有的 Axes 对象（仅用于 matplotlib，可选）
        backend: 'plotly' 或 'matplotlib'，默认 'plotly'
        max_points: 自动降采样阈值，默认 10000（仅用于 Plotly）

    Returns:
        Plotly figure 对象（backend='plotly'）或 (fig, ax) 元组（backend='matplotlib'）

    Examples:
        >>> fig = plot_rolling_stats(data, 'temperature', window=24)  # Plotly
        >>> fig.show()
        >>> fig, ax = plot_rolling_stats(data, 'temperature', window=24, backend='matplotlib')  # Matplotlib
        >>> plt.show()

        >>> # 使用 AlignedDataCache
        >>> fig = plot_rolling_stats(
        ...     cache,
        ...     'tempdata__Temperature',
        ...     time_range=('2025-12-15', '2025-12-16')
        ... )
        >>> fig.show()
    """
    # 提取数据（如果是 cache）
    data = _extract_data_from_cache(data, time_range)

    if backend == "plotly" and _PLOTLY_AVAILABLE:
        # Plotly 后端（数据已由装饰器降采样）
        y = data[column]

        # 计算滚动统计（在降采样后的数据上）
        rolling_mean = y.rolling(window=window, center=True).mean()

        if fig is None:
            fig = go.Figure()

        # 原始数据
        fig.add_trace(
            go.Scatter(
                x=y.index,
                y=y.values,
                mode="lines",
                name="Original",
                line=dict(width=1, color="lightgray"),
                opacity=0.3,
            )
        )

        # 移动平均
        fig.add_trace(
            go.Scatter(
                x=rolling_mean.index,
                y=rolling_mean.values,
                mode="lines",
                name=f"Rolling Mean (window={window})",
                line=dict(width=2),
                opacity=alpha,
            )
        )

        # 标准差带
        if plot_std:
            rolling_std = y.rolling(window=window, center=True).std()
            fig.add_trace(
                go.Scatter(
                    x=rolling_mean.index,
                    y=(rolling_mean + rolling_std).values,
                    mode="lines",
                    name="+1 Std Dev",
                    line=dict(width=0),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=rolling_mean.index,
                    y=(rolling_mean - rolling_std).values,
                    mode="lines",
                    name="±1 Std Dev",
                    line=dict(width=0),
                    fill="tonexty",
                    fillcolor="rgba(128, 128, 128, 0.2)",
                    opacity=0.2,
                )
            )

        fig.update_layout(
            title=title or f"Rolling Statistics of {column}",
            xaxis_title="Time",
            yaxis_title=ylabel or column,
            template="plotly_white",
            hovermode="x unified",
            width=figsize[0] * 100,
            height=figsize[1] * 100,
            showlegend=True,
        )

        if not grid:
            fig.update_xaxis(showgrid=False)
            fig.update_yaxis(showgrid=False)

        return fig
    else:
        # matplotlib 后端（保持向后兼容）
        if backend == "plotly" and not _PLOTLY_AVAILABLE:
            warnings.warn("Plotly 不可用，已切换到 matplotlib 后端", UserWarning, stacklevel=2)

        if fig is None or ax is None:
            fig, ax = plt.subplots(figsize=figsize)

        # 原始数据
        ax.plot(data.index, data[column], label="Original", alpha=0.3, linewidth=1)

        # 移动平均
        rolling_mean = data[column].rolling(window=window, center=True).mean()
        ax.plot(data.index, rolling_mean, label=f"Rolling Mean (window={window})", linewidth=2, alpha=alpha)

        # 标准差带
        if plot_std:
            rolling_std = data[column].rolling(window=window, center=True).std()
            ax.fill_between(
                data.index, rolling_mean - rolling_std, rolling_mean + rolling_std, alpha=0.2, label="±1 Std Dev"
            )

        ax.set_title(title or f"Rolling Statistics of {column}", fontsize=14, fontweight="bold")
        ax.set_xlabel("Time", fontsize=12)
        ax.set_ylabel(ylabel or column, fontsize=12)
        ax.legend(loc="best", fontsize=10)

        if grid:
            ax.grid(True, alpha=0.3)

        fig.autofmt_xdate()
        plt.tight_layout()

        return fig, ax


def interactive_pt_diagram(
    cache: "AlignedData",
    pressure_col: str,
    temperature_cols: Union[str, List[str]],
    *,
    gas: str = "argon",
    pressure_secondary_col: Optional[str] = None,
    T_range: Tuple[float, float] = (80, 110),
    P_range: Tuple[float, float] = (0.5, 3.5),
    labels: Optional[List[str]] = None,
    colors: Optional[List[str]] = None,
    temp_scale: float = 1.0,
    temp_offset: float = 0.0,
    press_scale: float = 1.0,
    press_offset: float = 0.0,
    ts_height: int = 300,
    pt_height: int = 500,
    width: int = 900,
    arrow_max: int = 12,
    arrow_min_dist: float = 0.02,
    downsample_max_points: int = 50000,
) -> "widgets.VBox":
    """
    创建交互式 P-T 相图，支持通过 RangeSlider 选择时间范围

    在 Jupyter Notebook 中使用，通过拖动时间序列图下方的 RangeSlider
    选择时间范围，相图会自动更新显示选定范围内的数据路径。

    Args:
        cache: AlignedData 缓存对象，包含已对齐的数据
        pressure_col: 压力列名（主压力）
        temperature_cols: 温度列名，可以是单个字符串或字符串列表
        gas: 气体类型，'argon' 或 'xenon'
        pressure_secondary_col: 次要压力列名（可选，用于多路径绘制）
        T_range: 温度显示范围 (K)
        P_range: 压力显示范围 (bar)
        labels: 路径标签列表
        colors: 路径颜色列表
        temp_scale: 温度缩放因子（如果数据是摄氏度，设为 1.0）
        temp_offset: 温度偏移量（如果数据是摄氏度，设为 273.15 转换为 K）
        press_scale: 压力缩放因子（如果数据是 mbar，设为 0.01 转换为 bar）
        press_offset: 压力偏移量
        ts_height: 时间序列图高度（像素）
        pt_height: 相图高度（像素）
        width: 图表宽度（像素）
        arrow_max: 相图中最大箭头数量
        arrow_min_dist: 箭头最小间距（归一化）
        downsample_max_points: 相图降采样最大点数

    Returns:
        ipywidgets.VBox 包含时间序列图和相图的交互式组件

    Examples:
        >>> from sc_reader import AlignedData, SCReader, TableSpec, interactive_pt_diagram
        >>> reader = SCReader()
        >>> cache = AlignedData(reader, specs, anchor='piddata')
        >>> cache.update()
        >>>
        >>> # 创建交互式相图
        >>> widget = interactive_pt_diagram(
        ...     cache,
        ...     pressure_col='runlidata__Pressure5',
        ...     temperature_cols=['piddata__A_Temperature', 'piddata__B_Temperature'],
        ...     gas='argon',
        ...     T_range=(80, 110),
        ...     P_range=(0.5, 3.5),
        ... )
        >>> display(widget)

    Note:
        需要安装 ipywidgets: pip install ipywidgets
        在 JupyterLab 中可能需要安装扩展: jupyter labextension install @jupyter-widgets/jupyterlab-manager
    """
    # 检查依赖
    if not _PLOTLY_AVAILABLE:
        raise ImportError("Plotly is required. Install with: pip install plotly")

    try:
        import ipywidgets as widgets
    except ImportError:
        raise ImportError("ipywidgets is required. Install with: pip install ipywidgets")

    from .phase_diagram import plot_pt_path

    # 规范化温度列为列表
    if isinstance(temperature_cols, str):
        temperature_cols = [temperature_cols]

    # 验证列存在
    df = cache.data
    required_cols = [pressure_col] + list(temperature_cols)
    if pressure_secondary_col:
        required_cols.append(pressure_secondary_col)

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"缺少列: {missing}，可用列: {list(df.columns)}")

    # 默认标签和颜色
    n_temps = len(temperature_cols)
    if labels is None:
        labels = [col.split("__")[-1] if "__" in col else col for col in temperature_cols]
    if colors is None:
        default_colors = ["red", "orange", "blue", "green", "purple", "cyan", "magenta", "brown"]
        colors = [default_colors[i % len(default_colors)] for i in range(n_temps)]

    # 创建 FigureWidget
    ts_fig = go.FigureWidget()
    pt_fig = go.FigureWidget()

    # 初始化时间序列图
    ts_fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df[pressure_col],
            mode="lines",
            name=pressure_col.split("__")[-1] if "__" in pressure_col else pressure_col,
            line=dict(color="steelblue", width=1.5),
            yaxis="y",
        )
    )

    # 添加第一个温度列到时间序列图（右 Y 轴）
    ts_fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df[temperature_cols[0]],
            mode="lines",
            name=temperature_cols[0].split("__")[-1] if "__" in temperature_cols[0] else temperature_cols[0],
            line=dict(color="coral", width=1.5),
            yaxis="y2",
        )
    )

    ts_fig.update_layout(
        title="Time Series (Drag RangeSlider to Select Range)",
        xaxis=dict(
            title="Time",
            rangeslider=dict(visible=True, thickness=0.1),
            type="date",
        ),
        yaxis=dict(
            title=pressure_col.split("__")[-1] if "__" in pressure_col else pressure_col,
            titlefont=dict(color="steelblue"),
            tickfont=dict(color="steelblue"),
            side="left",
        ),
        yaxis2=dict(
            title=temperature_cols[0].split("__")[-1] if "__" in temperature_cols[0] else temperature_cols[0],
            titlefont=dict(color="coral"),
            tickfont=dict(color="coral"),
            anchor="x",
            overlaying="y",
            side="right",
        ),
        template="plotly_white",
        hovermode="x unified",
        height=ts_height,
        width=width,
        margin=dict(l=60, r=60, t=40, b=40),
    )

    def _update_pt_diagram(df_range: pd.DataFrame):
        """更新相图"""
        pt_fig.data = []  # 清除现有数据

        if df_range.empty:
            pt_fig.add_annotation(
                text="No data in selected range",
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=16),
            )
            return

        # 准备压力数据
        p_primary = df_range[pressure_col].dropna().to_numpy() * press_scale + press_offset
        if pressure_secondary_col:
            p_secondary = df_range[pressure_secondary_col].dropna().to_numpy() * press_scale + press_offset
        else:
            p_secondary = p_primary

        # 准备温度数据
        temps = []
        p_paths = []
        valid_labels = []
        valid_colors = []

        for i, temp_col in enumerate(temperature_cols):
            temp_data = df_range[temp_col].dropna()
            if len(temp_data) > 0:
                t_values = temp_data.to_numpy() * temp_scale + temp_offset
                temps.append(t_values)
                # 第一个温度用主压力，其他用次要压力
                if i == 0:
                    p_paths.append(p_primary[: len(t_values)])
                else:
                    p_paths.append(p_secondary[: len(t_values)])
                valid_labels.append(labels[i])
                valid_colors.append(colors[i])

        if not temps:
            pt_fig.add_annotation(
                text="No valid data in selected range",
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=16),
            )
            return

        # 使用 plot_pt_path 生成相图
        new_fig = plot_pt_path(
            p_paths,
            temps,
            gas=gas,
            kind="plotly",
            T_range=T_range,
            P_range=P_range,
            labels=valid_labels,
            colors=valid_colors,
            arrow_max=arrow_max,
            arrow_min_dist=arrow_min_dist,
            downsample_max_points=downsample_max_points,
            show=False,
        )

        # 复制 traces 到 pt_fig
        for trace in new_fig.data:
            pt_fig.add_trace(trace)

        # 复制 annotations
        if new_fig.layout.annotations:
            pt_fig.layout.annotations = new_fig.layout.annotations

    # 初始绘制相图（使用全部数据）
    _update_pt_diagram(df)

    pt_fig.update_layout(
        title=f"{gas.capitalize()} P-T Phase Diagram",
        xaxis=dict(title="Temperature (K)", range=list(T_range)),
        yaxis=dict(title="Pressure (bar)", range=list(P_range)),
        template="plotly_white",
        hovermode="closest",
        height=pt_height,
        width=width,
        margin=dict(l=60, r=60, t=40, b=40),
    )

    # 范围变化回调
    def on_xaxis_range_change(layout, xaxis_range):
        """当 X 轴范围变化时更新相图"""
        if xaxis_range is None:
            # 使用全部数据
            _update_pt_diagram(df)
        else:
            try:
                start, end = xaxis_range
                # 转换为 pandas 可识别的时间格式
                df_range = cache[start:end]
                _update_pt_diagram(df_range)
            except Exception as e:
                warnings.warn(f"Failed to update phase diagram: {e}", UserWarning, stacklevel=2)

    # 注册回调
    ts_fig.layout.on_change(on_xaxis_range_change, "xaxis.range")

    # 创建状态显示
    status_label = widgets.HTML(
        value=f"<b>Data range:</b> {df.index.min()} ~ {df.index.max()} | "
        f"<b>Points:</b> {len(df)} | "
        f"<b>Columns:</b> {pressure_col}, {', '.join(temperature_cols)}"
    )

    # 组合组件
    return widgets.VBox(
        [
            status_label,
            ts_fig,
            pt_fig,
        ],
        layout=widgets.Layout(width=f"{width}px"),
    )
