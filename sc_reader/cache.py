"""
数据缓存管理模块

提供时间索引数据缓存，支持增量更新和 pandas 风格访问。
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
import warnings

import pandas as pd

from .reader import SCReader
from .spec import TableSpec
from .align import align_asof

try:
    import plotly.graph_objects as go

    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False
    go = None


class AlignedDataCache:
    """
    时间索引数据缓存管理器

    在 SCReader 基础上提供：
    1. 累积历史数据到内存 DataFrame
    2. 支持时间索引切片和 loc 访问
    3. 增量更新时自动去重和排序
    4. 可选的内存管理（时间窗口/行数限制）
    5. 数据持久化（Parquet 格式）

    Examples:
        >>> from sc_reader import SCReader, TableSpec, AlignedDataCache
        >>>
        >>> reader = SCReader(state_path='./watermark.json')
        >>> specs = [
        ...     TableSpec('tempdata', 'timestamp'),
        ...     TableSpec('runlidata', 'timestamp'),
        ... ]
        >>>
        >>> # 创建缓存
        >>> cache = AlignedDataCache(reader, specs, anchor='tempdata')
        >>>
        >>> # 首次加载
        >>> cache.update()  # 读取所有历史数据
        >>>
        >>> # 后续增量更新
        >>> new_rows = cache.update()  # 只拉取新数据并合并
        >>>
        >>> # 时间索引查询
        >>> df = cache['2025-12-15':'2025-12-16']
        >>> point = cache.loc['2025-12-15 10:00:00']
        >>>
        >>> # pandas 操作
        >>> resampled = cache.data.resample('1min').mean()
        >>>
        >>> # 持久化
        >>> cache.save('./cache.parquet')
        >>> cache.load('./cache.parquet')
    """

    def __init__(
        self,
        reader: SCReader,
        specs: List[TableSpec],
        anchor: str,
        tolerance: str = "5s",
        direction: str = "backward",
        lookback: str = "2s",
        max_memory_mb: Optional[float] = None,
        max_rows: Optional[int] = None,
        time_window_days: Optional[float] = None,
    ):
        """
        初始化缓存管理器

        Args:
            reader: SCReader 实例
            specs: TableSpec 列表，要读取的表
            anchor: 锚表名，以该表时间轴为基准
            tolerance: 时间对齐容差，默认 '200ms'
            direction: 对齐方向，默认 'backward'
            lookback: 回看窗口，默认 '2s'
            max_memory_mb: 最大内存占用（MB），超出则触发清理
            max_rows: 最大行数，超出则删除旧数据
            time_window_days: 时间窗口（天），只保留最近 N 天数据

        Raises:
            ValueError: anchor 不在 specs 中
        """
        # 验证 anchor
        table_names = [spec.table for spec in specs]
        if anchor not in table_names:
            raise ValueError(f"anchor '{anchor}' 不在 specs 中: {table_names}")

        self._reader = reader
        self._specs = specs
        self._anchor = anchor
        self._tolerance = tolerance
        self._direction = direction
        self._lookback = lookback

        # 内存管理参数
        self._max_memory_mb = max_memory_mb
        self._max_rows = max_rows
        self._time_window_days = time_window_days

        # 缓存数据
        self._data: pd.DataFrame = pd.DataFrame()
        self._data.index.name = "timestamp"  # 设置索引名

        # 统计信息
        self._total_updates = 0
        self._total_rows_added = 0
        self._last_update_time: Optional[datetime] = None

    def update(self, force_full: bool = False) -> int:
        """
        拉取增量数据并合并到缓存

        Args:
            force_full: 强制全量读取（忽略 watermark）

        Returns:
            新增行数

        Examples:
            >>> new_rows = cache.update()
            >>> print(f"新增 {new_rows} 行")
        """
        if force_full:
            # 重置 watermark 强制全量读取
            for spec in self._specs:
                self._reader.reset_watermark(spec.table)

        # 读取增量数据
        frames = self._reader.read_multiple(self._specs, lookback=self._lookback)

        # 对齐数据
        new_data = align_asof(frames, anchor=self._anchor, tolerance=self._tolerance, direction=self._direction)

        if new_data.empty:
            return 0

        # 合并到缓存
        rows_before = len(self._data)
        self._merge_data(new_data)
        rows_after = len(self._data)
        new_rows = rows_after - rows_before

        # 更新统计
        self._total_updates += 1
        self._total_rows_added += new_rows
        self._last_update_time = datetime.now()

        # 检查内存限制
        self._check_memory_limits()

        return new_rows

    def _merge_data(self, new_data: pd.DataFrame):
        """
        合并新数据到缓存

        实现逻辑：
        1. 使用 pd.concat 合并新旧数据
        2. 按时间索引去重（保留最新）
        3. 自动排序
        """
        if self._data.empty:
            self._data = new_data.copy()
        else:
            # 合并
            combined = pd.concat([self._data, new_data], axis=0)

            # 去重：相同时间戳保留最新（来自 new_data）
            combined = combined[~combined.index.duplicated(keep="last")]

            # 只在必要时排序
            if not combined.index.is_monotonic_increasing:
                combined = combined.sort_index()

            self._data = combined

    def _check_memory_limits(self):
        """检查并应用内存限制"""
        if self._data.empty:
            return

        # 1. 时间窗口限制
        if self._time_window_days is not None:
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=self._time_window_days)
            self._data = self._data[self._data.index >= cutoff]

        # 2. 行数限制
        if self._max_rows is not None and len(self._data) > self._max_rows:
            # 保留最新的 max_rows 行
            self._data = self._data.iloc[-self._max_rows :]

        # 3. 内存限制
        if self._max_memory_mb is not None:
            current_mb = self._data.memory_usage(deep=True).sum() / 1024 / 1024
            if current_mb > self._max_memory_mb:
                # 估算需要删除多少行
                rows_to_keep = int(len(self._data) * (self._max_memory_mb / current_mb) * 0.95)
                self._data = self._data.iloc[-rows_to_keep:]
                warnings.warn(
                    f"内存超限 ({current_mb:.1f}MB > {self._max_memory_mb}MB)，删除旧数据，保留最新 {rows_to_keep} 行"
                )

    def __getitem__(self, key: Union[str, slice]) -> pd.DataFrame:
        """
        支持时间索引切片

        Examples:
            >>> cache['2025-12-15':'2025-12-16']
            >>> cache['2025-12-15 10:00:00']
        """
        return self._data.loc[key]

    @property
    def loc(self):
        """支持 loc 访问"""
        return self._data.loc

    @property
    def iloc(self):
        """支持 iloc 访问"""
        return self._data.iloc

    @property
    def data(self) -> pd.DataFrame:
        """
        返回完整 DataFrame

        支持所有 pandas 操作：

        Examples:
            >>> cache.data.resample('1min').mean()
            >>> cache.data.rolling('10min').std()
            >>> cache.data.describe()
        """
        return self._data

    @property
    def index(self) -> pd.DatetimeIndex:
        """返回时间索引"""
        return self._data.index

    @property
    def columns(self) -> pd.Index:
        """返回列名"""
        return self._data.columns

    @property
    def shape(self) -> tuple:
        """返回形状 (行数, 列数)"""
        return self._data.shape

    @property
    def memory_usage_mb(self) -> float:
        """当前内存占用（MB）"""
        return self._data.memory_usage(deep=True).sum() / 1024 / 1024

    @property
    def time_range(self) -> Optional[tuple]:
        """数据时间范围 (min, max)"""
        if self._data.empty:
            return None
        return (self._data.index.min(), self._data.index.max())

    @property
    def stats(self) -> Dict:
        """缓存统计信息"""
        return {
            "total_rows": len(self._data),
            "total_columns": len(self._data.columns),
            "memory_mb": self.memory_usage_mb,
            "time_range": self.time_range,
            "total_updates": self._total_updates,
            "total_rows_added": self._total_rows_added,
            "last_update": self._last_update_time,
        }

    def save(self, path: Union[str, Path], compression: str = "snappy"):
        """
        保存缓存到 Parquet 文件

        Args:
            path: 文件路径
            compression: 压缩算法，可选 'snappy', 'gzip', 'brotli', 'lz4', 'zstd'

        Examples:
            >>> cache.save('./cache.parquet')
            >>> cache.save('./cache.parquet.gz', compression='gzip')
        """
        path = Path(path)

        if self._data.empty:
            warnings.warn("缓存为空，不保存文件")
            return

        # 确保目录存在
        path.parent.mkdir(parents=True, exist_ok=True)

        # 保存数据
        self._data.to_parquet(
            path,
            compression=compression,
            engine="pyarrow",
            index=True,  # 保存时间索引
        )

        # 保存元数据（用于验证）
        metadata = {
            "anchor": self._anchor,
            "specs": [{"table": s.table, "time_col": s.time_col} for s in self._specs],
            "saved_at": datetime.now().isoformat(),
            "rows": len(self._data),
            "columns": list(self._data.columns),
        }
        meta_path = path.with_suffix(".meta.json")
        import json

        meta_path.write_text(json.dumps(metadata, indent=2))

    def load(self, path: Union[str, Path], merge: bool = False):
        """
        从 Parquet 文件加载缓存

        Args:
            path: 文件路径
            merge: 是否合并到现有数据（True）或替换（False）

        Examples:
            >>> cache.load('./cache.parquet')
            >>> cache.load('./cache.parquet', merge=True)  # 合并到现有数据
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {path}")

        # 读取数据
        loaded_data = pd.read_parquet(path, engine="pyarrow")

        # 确保索引是 DatetimeIndex
        if not isinstance(loaded_data.index, pd.DatetimeIndex):
            loaded_data.index = pd.to_datetime(loaded_data.index)

        if merge:
            # 合并到现有数据
            self._merge_data(loaded_data)
        else:
            # 替换现有数据
            self._data = loaded_data

        # 检查内存限制
        self._check_memory_limits()

    def clear(self):
        """清空缓存数据"""
        self._data = pd.DataFrame()
        self._data.index.name = "timestamp"
        self._total_updates = 0
        self._total_rows_added = 0
        self._last_update_time = None

    def reset(self, reset_watermark: bool = False):
        """
        重置缓存

        Args:
            reset_watermark: 是否同时重置 reader 的 watermark
        """
        self.clear()

        if reset_watermark:
            for spec in self._specs:
                self._reader.reset_watermark(spec.table)

    def __repr__(self):
        if self._data.empty:
            return f"AlignedDataCache(empty, anchor='{self._anchor}')"

        return (
            f"AlignedDataCache("
            f"rows={len(self._data)}, "
            f"cols={len(self._data.columns)}, "
            f"range={self.time_range[0]} to {self.time_range[1]}, "
            f"anchor='{self._anchor}', "
            f"memory={self.memory_usage_mb:.1f}MB)"
        )

    def __len__(self):
        return len(self._data)

    def plot_timeseries(
        self,
        column: Optional[Union[str, List[str]]] = None,
        time_range: Optional[Union[str, Tuple[str, str]]] = None,
        auto_refresh: bool = False,
        **kwargs,
    ) -> "go.Figure":
        """
        绘制时间序列图（从 cache 中自动读取数据）

        Args:
            column: 要绘制的列名（可选）
            time_range: 时间范围（可选）
                - None: 使用全部数据（降采样）
                - Tuple[str, str]: 时间范围，例如 ('2025-12-15', '2025-12-16')
            auto_refresh: 是否启用自动刷新（实验性功能）
                注意：在 Jupyter notebook 中，自动刷新需要手动调用 refresh_plot
                或使用 plot_timeseries_interactive 方法
            **kwargs: 其他参数传递给 visualizer.plot_timeseries

        Returns:
            Plotly figure 对象

        Examples:
            >>> cache = AlignedDataCache(reader, specs, anchor='tempdata')
            >>> cache.update()
            >>>
            >>> # 绘制单列
            >>> fig = cache.plot_timeseries('tempdata__Temperature')
            >>> fig.show()
            >>>
            >>> # 指定时间范围
            >>> fig = cache.plot_timeseries(
            ...     'tempdata__Temperature',
            ...     time_range=('2025-12-15', '2025-12-16')
            ... )
            >>> fig.show()
        """
        from .visualizer import plot_timeseries as _plot_timeseries

        fig = _plot_timeseries(self, column=column, time_range=time_range, **kwargs)

        # 存储 cache 引用以便后续刷新（如果启用自动刷新）
        if auto_refresh:
            fig._cache_ref = self
            fig._cache_column = column

        return fig

    def plot_timeseries_interactive(
        self,
        column: Optional[Union[str, List[str]]] = None,
        initial_time_range: Optional[Tuple[str, str]] = None,
        initial_max_points: int = 10000,
        high_res_max_points: int = 100000,
        **kwargs,
    ) -> "go.Figure":
        """
        交互式时间序列绘图，支持根据拖动时间范围自动读取高分辨率数据

        初始显示降采样数据，当用户拖动/缩放时间轴时，需要手动调用 refresh_plot
        来刷新为高分辨率数据。

        注意：在 Jupyter notebook 中，真正的自动刷新需要使用 Dash 或手动调用 refresh_plot。
        此方法提供了更便捷的接口，但仍需要手动刷新。

        Args:
            column: 要绘制的列名（可选）
            initial_time_range: 初始显示的时间范围（可选，默认全部数据）
            initial_max_points: 初始降采样点数，默认 10000
            high_res_max_points: 高分辨率模式的最大点数，默认 100000
            **kwargs: 其他参数传递给 plot_timeseries

        Returns:
            Plotly figure 对象（带 cache 引用，可用于 refresh_plot）

        Examples:
            >>> cache = AlignedDataCache(reader, specs, anchor='tempdata')
            >>> cache.update()
            >>>
            >>> # 创建交互式图表（初始降采样）
            >>> fig = cache.plot_timeseries_interactive(
            ...     'tempdata__Temperature',
            ...     initial_time_range=('2025-12-15', '2025-12-16')
            ... )
            >>> fig.show()
            >>>
            >>> # 用户拖动到感兴趣的时间范围后，手动刷新
            >>> # 方法 1：指定时间范围
            >>> fig = cache.refresh_plot(
            ...     fig,
            ...     time_range=('2025-12-15 10:00:00', '2025-12-15 12:00:00')
            ... )
            >>> fig.show()
        """
        # 创建初始图表（降采样）
        fig = self.plot_timeseries(
            column=column, time_range=initial_time_range, max_points=initial_max_points, **kwargs
        )

        # 存储 cache 引用和参数
        fig._cache_ref = self
        fig._cache_column = column
        fig._cache_high_res_max_points = high_res_max_points

        return fig

    def plot_subplots(
        self,
        columns: Optional[List[str]] = None,
        column_groups: Optional[List[List[str]]] = None,
        time_range: Optional[Union[str, Tuple[str, str]]] = None,
        **kwargs,
    ) -> "go.Figure":
        """
        创建子图布局（从 cache 中自动读取数据）

        Args:
            columns: 要绘制的列名列表（单列模式）
            column_groups: 列组列表（列组模式）
            time_range: 时间范围（可选）
            **kwargs: 其他参数传递给 visualizer.plot_subplots

        Returns:
            Plotly figure 对象

        Examples:
            >>> # 单列模式
            >>> fig = cache.plot_subplots(columns=['tempdata__Temperature1', 'tempdata__Temperature2'])
            >>> fig.show()
            >>>
            >>> # 列组模式（同步 X 轴）
            >>> fig = cache.plot_subplots(
            ...     column_groups=[
            ...         ['tempdata__Temperature1', 'tempdata__Temperature2'],
            ...         ['runlidata__Pressure1', 'runlidata__Pressure2']
            ...     ],
            ...     time_range=('2025-12-15', '2025-12-16')
            ... )
            >>> fig.show()
        """
        from .visualizer import plot_subplots as _plot_subplots

        return _plot_subplots(self, columns=columns, column_groups=column_groups, time_range=time_range, **kwargs)

    def plot_dual_axis(
        self, left_column: str, right_column: str, time_range: Optional[Union[str, Tuple[str, str]]] = None, **kwargs
    ) -> "go.Figure":
        """
        绘制双Y轴图表（从 cache 中自动读取数据）

        Args:
            left_column: 左Y轴的列名
            right_column: 右Y轴的列名
            time_range: 时间范围（可选）
            **kwargs: 其他参数传递给 visualizer.plot_dual_axis

        Returns:
            Plotly figure 对象

        Examples:
            >>> fig = cache.plot_dual_axis(
            ...     'tempdata__Temperature',
            ...     'runlidata__Pressure1',
            ...     time_range=('2025-12-15', '2025-12-16')
            ... )
            >>> fig.show()
        """
        from .visualizer import plot_dual_axis as _plot_dual_axis

        return _plot_dual_axis(
            self, left_column=left_column, right_column=right_column, time_range=time_range, **kwargs
        )

    def plot_temp_pressure_sync(
        self,
        temp_columns: Optional[List[str]] = None,
        pressure_columns: Optional[List[str]] = None,
        time_range: Optional[Union[str, Tuple[str, str]]] = None,
        **kwargs,
    ) -> "go.Figure":
        """
        绘制同步的温度和压强子图（从 cache 中自动读取数据）

        自动识别温度和压强列，然后创建同步子图。

        Args:
            temp_columns: 温度列名列表（可选，如果为 None 则自动识别）
            pressure_columns: 压强列名列表（可选，如果为 None 则自动识别）
            time_range: 时间范围（可选）
            **kwargs: 其他参数传递给 visualizer.plot_temp_pressure_sync

        Returns:
            Plotly figure 对象

        Examples:
            >>> # 自动识别温度和压强
            >>> fig = cache.plot_temp_pressure_sync()
            >>> fig.show()
            >>>
            >>> # 指定时间范围
            >>> fig = cache.plot_temp_pressure_sync(
            ...     time_range=('2025-12-15', '2025-12-16')
            ... )
            >>> fig.show()
        """
        from .visualizer import plot_temp_pressure_sync as _plot_temp_pressure_sync

        return _plot_temp_pressure_sync(
            self, temp_columns=temp_columns, pressure_columns=pressure_columns, time_range=time_range, **kwargs
        )

    def refresh_plot(
        self,
        fig: "go.Figure",
        time_range: Optional[Union[str, Tuple[str, str]]] = None,
        high_res_max_points: int = 100000,
    ) -> "go.Figure":
        """
        根据指定时间范围或图表当前显示范围，从 cache 中读取高分辨率数据并刷新图表

        注意：在 Jupyter notebook 中，用户拖动/缩放后，Python 端的 figure 对象不会自动更新。
        因此，建议使用方法 2（手动指定时间范围）或使用方法 3（通过 JavaScript 获取范围）。

        使用方法：
        方法 1（自动检测，可能不准确）：
        1. fig = cache.plot_timeseries('Temperature')
        2. fig.show()
        3. 用户拖动到感兴趣的时间范围
        4. fig = cache.refresh_plot(fig)  # 尝试从 figure 获取范围（可能不准确）
        5. fig.show()

        方法 2（推荐，手动指定范围）：
        1. fig = cache.plot_timeseries('Temperature')
        2. fig.show()
        3. 用户拖动到感兴趣的时间范围，记录时间范围
        4. fig = cache.refresh_plot(fig, time_range=('2025-12-15 10:00:00', '2025-12-15 12:00:00'))
        5. fig.show()

        方法 3（通过 JavaScript 获取范围）：
        1. fig = cache.plot_timeseries('Temperature')
        2. fig.show()
        3. 在浏览器控制台运行：`Plotly.relayout(gd, {xaxis: {range: null}});` 查看当前范围
        4. 或者使用 cache.get_plot_range_js() 获取 JavaScript 代码

        Args:
            fig: Plotly figure 对象
            time_range: 时间范围（可选）
                - None: 尝试从 figure.layout.xaxis.range 获取（可能不准确）
                - Tuple[str, str]: 手动指定时间范围，例如 ('2025-12-15 10:00:00', '2025-12-15 12:00:00')
            high_res_max_points: 高分辨率模式的最大点数，默认 100000

        Returns:
            更新后的 figure 对象

        Examples:
            >>> # 方法 1：自动检测（可能不准确）
            >>> fig = cache.plot_timeseries('tempdata__Temperature')
            >>> fig.show()
            >>> fig = cache.refresh_plot(fig)
            >>> fig.show()

            >>> # 方法 2：手动指定范围（推荐）
            >>> fig = cache.plot_timeseries('tempdata__Temperature')
            >>> fig.show()
            >>> # 用户拖动后，手动指定时间范围
            >>> fig = cache.refresh_plot(
            ...     fig,
            ...     time_range=('2025-12-15 10:00:00', '2025-12-15 12:00:00')
            ... )
            >>> fig.show()
        """
        if not _PLOTLY_AVAILABLE:
            warnings.warn("Plotly 不可用，无法刷新图表", UserWarning, stacklevel=2)
            return fig

        # 确定时间范围
        if time_range is not None:
            # 使用手动指定的时间范围
            if isinstance(time_range, str):
                data = self.loc[time_range]
                if isinstance(data, pd.Series):
                    data = data.to_frame().T
            elif isinstance(time_range, tuple) and len(time_range) == 2:
                data = self[time_range[0] : time_range[1]]
            else:
                warnings.warn(f"无效的时间范围格式: {time_range}", UserWarning, stacklevel=2)
                return fig
        else:
            # 尝试从 figure 获取范围（可能不准确）
            x_range = None
            if hasattr(fig.layout, "xaxis") and hasattr(fig.layout.xaxis, "range"):
                x_range = fig.layout.xaxis.range

            if x_range and len(x_range) == 2:
                try:
                    start = pd.Timestamp(x_range[0])
                    end = pd.Timestamp(x_range[1])
                    data = self[start:end]
                except (ValueError, KeyError, TypeError) as e:
                    warnings.warn(f"无法从图表获取时间范围，请手动指定 time_range 参数: {e}", UserWarning, stacklevel=2)
                    return fig
            else:
                warnings.warn(
                    "图表中没有时间范围信息，请手动指定 time_range 参数。"
                    "在 Jupyter notebook 中，用户拖动/缩放后，Python 端的 figure 对象不会自动更新。",
                    UserWarning,
                    stacklevel=2,
                )
                return fig

        if data.empty:
            warnings.warn("指定时间范围内没有数据", UserWarning, stacklevel=2)
            return fig

        # 导入降采样函数
        from .visualizer import _downsample_for_plotly

        # 更新所有轨迹
        for trace in fig.data:
            if hasattr(trace, "name") and trace.name:
                col_name = trace.name
                if col_name in data.columns:
                    col_data = data[col_name]

                    # 降采样（如果需要）
                    if len(col_data) > high_res_max_points:
                        col_data = _downsample_for_plotly(col_data, max_points=high_res_max_points)

                    # 更新轨迹数据
                    trace.x = col_data.index
                    trace.y = col_data.values

        return fig

    @staticmethod
    def get_plot_range_js() -> str:
        """
        返回用于在浏览器控制台获取当前图表显示范围的 JavaScript 代码

        使用方法：
        1. 在 Jupyter notebook 中显示图表：fig.show()
        2. 在浏览器中打开开发者工具（F12）
        3. 在控制台中运行此方法返回的 JavaScript 代码
        4. 复制输出的时间范围
        5. 使用该范围调用 refresh_plot(fig, time_range=(start, end))

        Returns:
            JavaScript 代码字符串

        Examples:
            >>> print(cache.get_plot_range_js())
            >>> # 在浏览器控制台运行返回的代码
            >>> # 然后使用返回的范围：
            >>> fig = cache.refresh_plot(fig, time_range=('2025-12-15 10:00:00', '2025-12-15 12:00:00'))
        """
        return """
// 获取当前 Plotly 图表的 X 轴范围
// 在浏览器控制台中运行此代码

// 方法 1：获取所有图表的范围
var plots = document.querySelectorAll('.plotly');
plots.forEach(function(plot, idx) {
    var gd = plot.data[0] ? plot : null;
    if (gd) {
        var layout = gd.layout;
        if (layout.xaxis && layout.xaxis.range) {
            console.log('Plot ' + idx + ' X-axis range:', layout.xaxis.range);
        }
    }
});

// 方法 2：如果图表有 ID，可以直接访问
// var gd = document.getElementById('your-plot-id');
// if (gd && gd.layout && gd.layout.xaxis && gd.layout.xaxis.range) {
//     console.log('X-axis range:', gd.layout.xaxis.range);
// }

// 方法 3：使用 Plotly API（需要图表对象引用）
// 在 Python 中：fig.show() 后，图表对象会被存储
// 在浏览器控制台中可以尝试：
// var gd = document.querySelector('.plotly');
// if (gd && gd._fullLayout) {
//     var range = gd._fullLayout.xaxis.range;
//     console.log('X-axis range:', range);
//     console.log('Time range tuple:', '(\"' + range[0] + '\", \"' + range[1] + '\")');
// }
"""
