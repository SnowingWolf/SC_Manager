"""
数据缓存管理模块

提供时间索引数据缓存，支持增量更新和 pandas 风格访问。
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union
import warnings

import pandas as pd

from .reader import SCReader
from .spec import TableSpec
from .align import align_asof


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
        tolerance: str = '200ms',
        direction: str = 'backward',
        lookback: str = '2s',
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
        self._data.index.name = 'timestamp'  # 设置索引名

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
        new_data = align_asof(
            frames,
            anchor=self._anchor,
            tolerance=self._tolerance,
            direction=self._direction
        )

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
            combined = combined[~combined.index.duplicated(keep='last')]

            # 排序
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
            self._data = self._data.iloc[-self._max_rows:]

        # 3. 内存限制
        if self._max_memory_mb is not None:
            current_mb = self._data.memory_usage(deep=True).sum() / 1024 / 1024
            if current_mb > self._max_memory_mb:
                # 估算需要删除多少行
                rows_to_keep = int(len(self._data) * (self._max_memory_mb / current_mb) * 0.95)
                self._data = self._data.iloc[-rows_to_keep:]
                warnings.warn(
                    f"内存超限 ({current_mb:.1f}MB > {self._max_memory_mb}MB)，"
                    f"删除旧数据，保留最新 {rows_to_keep} 行"
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
            'total_rows': len(self._data),
            'total_columns': len(self._data.columns),
            'memory_mb': self.memory_usage_mb,
            'time_range': self.time_range,
            'total_updates': self._total_updates,
            'total_rows_added': self._total_rows_added,
            'last_update': self._last_update_time,
        }

    def save(self, path: Union[str, Path], compression: str = 'snappy'):
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
            engine='pyarrow',
            index=True  # 保存时间索引
        )

        # 保存元数据（用于验证）
        metadata = {
            'anchor': self._anchor,
            'specs': [{'table': s.table, 'time_col': s.time_col} for s in self._specs],
            'saved_at': datetime.now().isoformat(),
            'rows': len(self._data),
            'columns': list(self._data.columns),
        }
        meta_path = path.with_suffix('.meta.json')
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
        loaded_data = pd.read_parquet(path, engine='pyarrow')

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
        self._data.index.name = 'timestamp'
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
