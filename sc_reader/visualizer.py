"""
慢控数据可视化模块

提供多种可视化函数用于分析慢控数据
"""

from typing import List, Optional, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


# 设置默认样式
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


def plot_timeseries(
    data: pd.DataFrame,
    column: str,
    figsize: Tuple[int, int] = (12, 5),
    title: Optional[str] = None,
    ylabel: Optional[str] = None,
    color: str = 'steelblue',
    linewidth: float = 1.5,
    alpha: float = 0.8,
    grid: bool = True,
    fig: Optional[plt.Figure] = None,
    ax: Optional[plt.Axes] = None
) -> Tuple[plt.Figure, plt.Axes]:
    """
    绘制单变量时间序列图

    Args:
        data: 包含时间序列数据的 DataFrame，索引为时间
        column: 要绘制的列名
        figsize: 图表大小
        title: 图表标题，默认为列名
        ylabel: Y轴标签，默认为列名
        color: 线条颜色
        linewidth: 线条宽度
        alpha: 透明度
        grid: 是否显示网格
        fig: 已有的 Figure 对象（可选）
        ax: 已有的 Axes 对象（可选）

    Returns:
        fig, ax: matplotlib 的 Figure 和 Axes 对象

    Examples:
        >>> fig, ax = plot_timeseries(data, 'temperature')
        >>> plt.show()
    """
    if fig is None or ax is None:
        fig, ax = plt.subplots(figsize=figsize)

    ax.plot(data.index, data[column], color=color, linewidth=linewidth, alpha=alpha)

    ax.set_title(title or f'{column} vs Time', fontsize=14, fontweight='bold')
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel(ylabel or column, fontsize=12)

    if grid:
        ax.grid(True, alpha=0.3)

    fig.autofmt_xdate()
    plt.tight_layout()

    return fig, ax


def plot_data(df_date=None, char: str = "B_Temperature", fig=None, ax=None, **kwargs):
    """绘制单个慢控通道的时间序列（遗留兼容函数）。

    说明：
    - 早期版本里这个函数位于 `connect_mysql.py`；为了避免 DB 模块在 import 阶段依赖 matplotlib，
      现在把绘图逻辑统一放到 `sc_reader.visualizer` 中。
    - 返回值保持旧行为：**返回 ax**（而不是 (fig, ax)）。

    Args:
        df_date: DataFrame（索引为时间）或 Series
        char: DataFrame 时要绘制的列名
        fig/ax: 可选复用已有画布
        **kwargs: 透传到 matplotlib `ax.plot(...)`
    """
    if df_date is None:
        raise ValueError("plot_data(df_date=...) 需要显式传入 df_date（DataFrame/Series），不再提供隐式默认值")

    if fig is None or ax is None:
        fig, ax = plt.subplots(figsize=(12, 5))

    plot_kwargs = {"alpha": 0.7, "linewidth": 1}
    plot_kwargs.update(kwargs)

    # 支持 DataFrame / Series
    if isinstance(df_date, pd.Series):
        y = df_date
        ylabel = char or (y.name if y.name is not None else "value")
    else:
        if char not in df_date.columns:
            raise KeyError(f"列不存在: {char!r}，可用列: {list(df_date.columns)}")
        y = df_date[char]
        ylabel = char

    ax.plot(y.index, y.values, **plot_kwargs)
    fig.autofmt_xdate()  # 自动格式化 x 轴日期标签

    ax.set_ylabel(ylabel, fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="x", rotation=30)
    return ax


def plot_multi_variables(
    data: pd.DataFrame,
    columns: List[str],
    figsize: Tuple[int, int] = (12, 6),
    title: str = 'Multi-Variable Time Series',
    ylabel: str = 'Value',
    linewidth: float = 1.5,
    alpha: float = 0.7,
    grid: bool = True,
    legend_loc: str = 'best',
    fig: Optional[plt.Figure] = None,
    ax: Optional[plt.Axes] = None
) -> Tuple[plt.Figure, plt.Axes]:
    """
    在同一图表中绘制多个变量的时间序列

    Args:
        data: 包含时间序列数据的 DataFrame，索引为时间
        columns: 要绘制的列名列表
        figsize: 图表大小
        title: 图表标题
        ylabel: Y轴标签
        linewidth: 线条宽度
        alpha: 透明度
        grid: 是否显示网格
        legend_loc: 图例位置
        fig: 已有的 Figure 对象（可选）
        ax: 已有的 Axes 对象（可选）

    Returns:
        fig, ax: matplotlib 的 Figure 和 Axes 对象

    Examples:
        >>> fig, ax = plot_multi_variables(data, ['temperature', 'pressure', 'flow'])
        >>> plt.show()
    """
    if fig is None or ax is None:
        fig, ax = plt.subplots(figsize=figsize)

    for column in columns:
        ax.plot(data.index, data[column], label=column, linewidth=linewidth, alpha=alpha)

    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.legend(loc=legend_loc, fontsize=10)

    if grid:
        ax.grid(True, alpha=0.3)

    fig.autofmt_xdate()
    plt.tight_layout()

    return fig, ax


def plot_dual_axis(
    data: pd.DataFrame,
    left_column: str,
    right_column: str,
    figsize: Tuple[int, int] = (12, 6),
    title: str = 'Dual Axis Time Series',
    left_ylabel: Optional[str] = None,
    right_ylabel: Optional[str] = None,
    left_color: str = 'steelblue',
    right_color: str = 'coral',
    linewidth: float = 1.5,
    alpha: float = 0.8,
    grid: bool = True
) -> Tuple[plt.Figure, plt.Axes, plt.Axes]:
    """
    绘制双Y轴图表（用于不同量级或单位的参数）

    Args:
        data: 包含时间序列数据的 DataFrame，索引为时间
        left_column: 左Y轴的列名
        right_column: 右Y轴的列名
        figsize: 图表大小
        title: 图表标题
        left_ylabel: 左Y轴标签
        right_ylabel: 右Y轴标签
        left_color: 左轴线条颜色
        right_color: 右轴线条颜色
        linewidth: 线条宽度
        alpha: 透明度
        grid: 是否显示网格

    Returns:
        fig, ax1, ax2: matplotlib 的 Figure 和两个 Axes 对象

    Examples:
        >>> fig, ax1, ax2 = plot_dual_axis(data, 'temperature', 'heater_power')
        >>> plt.show()
    """
    fig, ax1 = plt.subplots(figsize=figsize)

    # 左Y轴
    ax1.plot(data.index, data[left_column], color=left_color,
             linewidth=linewidth, alpha=alpha, label=left_column)
    ax1.set_xlabel('Time', fontsize=12)
    ax1.set_ylabel(left_ylabel or left_column, fontsize=12, color=left_color)
    ax1.tick_params(axis='y', labelcolor=left_color)

    # 右Y轴
    ax2 = ax1.twinx()
    ax2.plot(data.index, data[right_column], color=right_color,
             linewidth=linewidth, alpha=alpha, label=right_column)
    ax2.set_ylabel(right_ylabel or right_column, fontsize=12, color=right_color)
    ax2.tick_params(axis='y', labelcolor=right_color)

    # 标题和图例
    ax1.set_title(title, fontsize=14, fontweight='bold')

    # 合并图例
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)

    if grid:
        ax1.grid(True, alpha=0.3)

    fig.autofmt_xdate()
    plt.tight_layout()

    return fig, ax1, ax2


def plot_subplots(
    data: pd.DataFrame,
    columns: List[str],
    nrows: Optional[int] = None,
    ncols: int = 2,
    figsize: Tuple[int, int] = (14, 10),
    suptitle: str = 'Multi-Parameter Subplots',
    linewidth: float = 1.5,
    alpha: float = 0.8,
    grid: bool = True
) -> Tuple[plt.Figure, np.ndarray]:
    """
    创建子图布局（多个参数分别显示）

    Args:
        data: 包含时间序列数据的 DataFrame，索引为时间
        columns: 要绘制的列名列表
        nrows: 子图行数，默认自动计算
        ncols: 子图列数，默认 2
        figsize: 图表大小
        suptitle: 总标题
        linewidth: 线条宽度
        alpha: 透明度
        grid: 是否显示网格

    Returns:
        fig, axes: matplotlib 的 Figure 和 Axes 数组

    Examples:
        >>> fig, axes = plot_subplots(data, ['temperature', 'pressure', 'flow', 'heater_power'])
        >>> plt.show()
    """
    n_plots = len(columns)
    if nrows is None:
        nrows = (n_plots + ncols - 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)

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
        ax.set_title(column, fontsize=12, fontweight='bold')
        ax.set_xlabel('Time', fontsize=10)
        ax.set_ylabel(column, fontsize=10)

        if grid:
            ax.grid(True, alpha=0.3)

        # 旋转x轴标签
        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_ha('right')

    # 隐藏多余的子图
    for idx in range(n_plots, nrows * ncols):
        row = idx // ncols
        col = idx % ncols
        axes[row, col].axis('off')

    fig.suptitle(suptitle, fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()

    return fig, axes


def plot_distribution(
    data: pd.DataFrame,
    column: str,
    bins: int = 30,
    figsize: Tuple[int, int] = (10, 6),
    title: Optional[str] = None,
    xlabel: Optional[str] = None,
    color: str = 'steelblue',
    alpha: float = 0.7,
    kde: bool = True,
    fig: Optional[plt.Figure] = None,
    ax: Optional[plt.Axes] = None
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
    ax.hist(data[column].dropna(), bins=bins, color=color, alpha=alpha, edgecolor='black')

    # 添加 KDE 曲线
    if kde:
        ax2 = ax.twinx()
        data[column].dropna().plot.kde(ax=ax2, color='red', linewidth=2)
        ax2.set_ylabel('Density', fontsize=12)

    ax.set_title(title or f'Distribution of {column}', fontsize=14, fontweight='bold')
    ax.set_xlabel(xlabel or column, fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()

    return fig, ax


def plot_boxplot(
    data: pd.DataFrame,
    columns: Union[str, List[str]],
    figsize: Tuple[int, int] = (10, 6),
    title: str = 'Box Plot',
    ylabel: str = 'Value',
    showfliers: bool = True,
    fig: Optional[plt.Figure] = None,
    ax: Optional[plt.Axes] = None
) -> Tuple[plt.Figure, plt.Axes]:
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

    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=12)
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()

    return fig, ax


def plot_correlation(
    data: pd.DataFrame,
    columns: Optional[List[str]] = None,
    figsize: Tuple[int, int] = (10, 8),
    title: str = 'Correlation Heatmap',
    cmap: str = 'coolwarm',
    annot: bool = True,
    fmt: str = '.2f',
    fig: Optional[plt.Figure] = None,
    ax: Optional[plt.Axes] = None
) -> Tuple[plt.Figure, plt.Axes]:
    """
    绘制相关性热力图

    Args:
        data: 包含数据的 DataFrame
        columns: 要分析的列名列表，默认使用所有数值列
        figsize: 图表大小
        title: 图表标题
        cmap: 颜色映射
        annot: 是否在每个单元格中显示数值
        fmt: 数值格式
        fig: 已有的 Figure 对象（可选）
        ax: 已有的 Axes 对象（可选）

    Returns:
        fig, ax: matplotlib 的 Figure 和 Axes 对象

    Examples:
        >>> fig, ax = plot_correlation(data, ['temperature', 'pressure', 'flow', 'heater_power'])
        >>> plt.show()
    """
    if fig is None or ax is None:
        fig, ax = plt.subplots(figsize=figsize)

    if columns is None:
        # 使用所有数值列
        corr_data = data.select_dtypes(include=[np.number])
    else:
        corr_data = data[columns]

    corr_matrix = corr_data.corr()

    sns.heatmap(corr_matrix, annot=annot, fmt=fmt, cmap=cmap,
                center=0, square=True, linewidths=1, cbar_kws={"shrink": 0.8}, ax=ax)

    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)

    plt.tight_layout()

    return fig, ax


def plot_rolling_stats(
    data: pd.DataFrame,
    column: str,
    window: int = 24,
    figsize: Tuple[int, int] = (12, 6),
    title: Optional[str] = None,
    ylabel: Optional[str] = None,
    plot_std: bool = True,
    alpha: float = 0.7,
    grid: bool = True,
    fig: Optional[plt.Figure] = None,
    ax: Optional[plt.Axes] = None
) -> Tuple[plt.Figure, plt.Axes]:
    """
    绘制滚动统计图（移动平均和标准差）

    Args:
        data: 包含时间序列数据的 DataFrame，索引为时间
        column: 要分析的列名
        window: 滚动窗口大小
        figsize: 图表大小
        title: 图表标题
        ylabel: Y轴标签
        plot_std: 是否绘制标准差带
        alpha: 透明度
        grid: 是否显示网格
        fig: 已有的 Figure 对象（可选）
        ax: 已有的 Axes 对象（可选）

    Returns:
        fig, ax: matplotlib 的 Figure 和 Axes 对象

    Examples:
        >>> fig, ax = plot_rolling_stats(data, 'temperature', window=24)
        >>> plt.show()
    """
    if fig is None or ax is None:
        fig, ax = plt.subplots(figsize=figsize)

    # 原始数据
    ax.plot(data.index, data[column], label='Original', alpha=0.3, linewidth=1)

    # 移动平均
    rolling_mean = data[column].rolling(window=window, center=True).mean()
    ax.plot(data.index, rolling_mean, label=f'Rolling Mean (window={window})',
            linewidth=2, alpha=alpha)

    # 标准差带
    if plot_std:
        rolling_std = data[column].rolling(window=window, center=True).std()
        ax.fill_between(data.index,
                        rolling_mean - rolling_std,
                        rolling_mean + rolling_std,
                        alpha=0.2, label='±1 Std Dev')

    ax.set_title(title or f'Rolling Statistics of {column}', fontsize=14, fontweight='bold')
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel(ylabel or column, fontsize=12)
    ax.legend(loc='best', fontsize=10)

    if grid:
        ax.grid(True, alpha=0.3)

    fig.autofmt_xdate()
    plt.tight_layout()

    return fig, ax
