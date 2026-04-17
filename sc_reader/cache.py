"""
数据缓存管理模块

提供时间索引数据缓存，支持增量更新和 pandas 风格访问。
"""

import os
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from .align import align_asof
from .reader import SCReader
from .spec import TableSpec

try:
    import plotly.graph_objects as go

    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False
    go = None


class AlignedData:
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
        reader: Optional[SCReader],
        specs: List[TableSpec],
        anchor: str,
        tolerance: str = "5s",
        direction: str = "backward",
        lookback: str = "2s",
        max_memory_mb: Optional[float] = None,
        max_rows: Optional[int] = None,
        time_window_days: Optional[float] = None,
        cache_path: Optional[Union[str, Path]] = None,
        auto_load: bool = True,
        auto_save: bool = False,
        tail_recompute: bool = False,
        tail_recompute_window: Optional[str] = None,
        timing_log: bool = False,
    ):
        """
        初始化缓存管理器

        Args:
            reader: SCReader 实例，如果为 None 则只能使用本地缓存（离线模式）
            specs: TableSpec 列表，要读取的表
            anchor: 锚表名，以该表时间轴为基准
            tolerance: 时间对齐容差，默认 '200ms'
            direction: 对齐方向，默认 'backward'
            lookback: 回看窗口，默认 '2s'
            max_memory_mb: 最大内存占用（MB），超出则触发清理
            max_rows: 最大行数，超出则删除旧数据
            time_window_days: 时间窗口（天），只保留最近 N 天数据
            cache_path: 本地缓存文件路径（Parquet 格式），启用本地优先模式
            auto_load: 初始化时自动加载本地缓存（默认 True）
            auto_save: update() 后自动保存到本地缓存（默认 False）
            tail_recompute: 是否在增量对齐时回算缓存尾部窗口（默认 False）
            tail_recompute_window: 回算窗口大小，如 '5s'；None 表示使用 lookback
            timing_log: 是否打印 update() 分段耗时日志（默认 False）。
                也可通过环境变量 SC_ALIGNEDDATA_TIMING=1 开启。

        Raises:
            ValueError: anchor 不在 specs 中

        Examples:
            >>> # 本地优先模式：自动加载缓存，增量更新后自动保存
            >>> cache = AlignedDataCache(
            ...     reader, specs, anchor='tempdata',
            ...     cache_path='./cache.parquet',
            ...     auto_load=True,
            ...     auto_save=True,
            ... )
            >>> cache.update()  # 自动加载本地 -> 增量拉取 -> 自动保存
            >>>
            >>> # 离线模式：不连接数据库，只使用本地缓存
            >>> cache = AlignedDataCache(
            ...     reader=None, specs=specs, anchor='tempdata',
            ...     cache_path='./cache.parquet',
            ... )
            >>> df = cache['2025-01-01':'2025-01-31']
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
        self._tail_recompute = tail_recompute
        self._tail_recompute_window = tail_recompute_window

        # 内存管理参数
        self._max_memory_mb = max_memory_mb
        self._max_rows = max_rows
        self._time_window_days = time_window_days

        # 本地缓存参数
        self._cache_path = Path(cache_path) if cache_path else None
        self._auto_save = auto_save
        env_timing = os.getenv("SC_ALIGNEDDATA_TIMING", "").strip().lower()
        self._timing_log = timing_log or env_timing in {"1", "true", "yes", "on"}

        # 缓存数据
        self._data: pd.DataFrame = pd.DataFrame()
        self._data.index.name = "timestamp"  # 设置索引名

        # 统计信息
        self._total_updates = 0
        self._total_rows_added = 0
        self._last_update_time: Optional[datetime] = None
        self._last_update_timing: Dict[str, float] = {}
        self._last_frames_signature: Optional[Dict[str, Tuple[int, Optional[int], Optional[int], int]]] = None

        # 自动加载本地缓存
        if auto_load and self._cache_path and self._cache_path.exists():
            self.load(self._cache_path)

    def _parse_timedelta(self, value: Optional[Union[str, pd.Timedelta]]) -> Optional[pd.Timedelta]:
        if value is None:
            return None
        if isinstance(value, pd.Timedelta):
            return value
        try:
            return pd.Timedelta(value)
        except (TypeError, ValueError):
            return None

    def _frame_signature(self, df: pd.DataFrame) -> Tuple[int, Optional[int], Optional[int], int]:
        """生成用于增量空跑判断的 DataFrame 签名。"""
        if df.empty:
            return (0, None, None, 0)
        idx = df.index
        if not isinstance(idx, pd.DatetimeIndex):
            idx = pd.to_datetime(idx)
        # hash_pandas_object 覆盖索引和值，保证同时间戳值变化也可检测到
        payload_hash = int(pd.util.hash_pandas_object(df, index=True).sum())
        return (len(df), int(idx.min().value), int(idx.max().value), payload_hash)

    def _frames_signature(
        self, frames: Dict[str, pd.DataFrame]
    ) -> Dict[str, Tuple[int, Optional[int], Optional[int], int]]:
        return {table: self._frame_signature(df) for table, df in frames.items()}

    def _get_incremental_anchor_index(
        self, anchor_df: pd.DataFrame, frames: Optional[Dict[str, pd.DataFrame]] = None
    ) -> pd.DatetimeIndex:
        """
        计算本轮需要重新对齐的 anchor 时间索引。
        默认只包含新增 anchor 行；可选回算尾窗。
        """
        if anchor_df.empty:
            return pd.DatetimeIndex([])

        idx = anchor_df.index
        if not isinstance(idx, pd.DatetimeIndex):
            idx = pd.to_datetime(idx)
        if idx.has_duplicates:
            idx = idx[~idx.duplicated(keep="last")]
        if not idx.is_monotonic_increasing:
            idx = idx.sort_values()

        # 首次加载：全量对齐
        if self._data.empty:
            return idx

        last_cached_ts = self._data.index.max()
        incremental_idx = idx[idx > last_cached_ts]

        # 非锚表晚到数据可能需要回填历史 anchor 行（即使 anchor 无新增）
        # 这里根据非锚表时间范围 + 容差窗口，自动补充一段回算索引。
        backfill_idx = pd.DatetimeIndex([])
        if frames:
            tol_td = self._parse_timedelta(self._tolerance) or pd.Timedelta(0)
            margin = tol_td

            non_anchor_min = None
            non_anchor_max = None
            for table, df in frames.items():
                if table == self._anchor or df.empty:
                    continue
                right_idx = df.index
                if not isinstance(right_idx, pd.DatetimeIndex):
                    right_idx = pd.to_datetime(right_idx)
                if len(right_idx) == 0:
                    continue
                cur_min = right_idx.min()
                cur_max = right_idx.max()
                non_anchor_min = cur_min if non_anchor_min is None else min(non_anchor_min, cur_min)
                non_anchor_max = cur_max if non_anchor_max is None else max(non_anchor_max, cur_max)

            if non_anchor_min is not None and non_anchor_max is not None:
                backfill_start = non_anchor_min - margin
                backfill_end = min(non_anchor_max + margin, last_cached_ts)
                if backfill_end >= backfill_start:
                    backfill_idx = idx[(idx >= backfill_start) & (idx <= backfill_end)]

        target_idx = incremental_idx.union(backfill_idx)

        if not self._tail_recompute:
            return target_idx

        # 可选回算尾窗：用于处理极端乱序写入
        recompute_window = self._parse_timedelta(self._tail_recompute_window) or self._parse_timedelta(self._lookback)
        if recompute_window is None:
            return target_idx

        tail_start = last_cached_ts - recompute_window
        tail_idx = idx[(idx >= tail_start) & (idx <= last_cached_ts)]
        if len(tail_idx) == 0:
            return target_idx
        return target_idx.union(tail_idx)

    def _align_incremental(self, frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        仅对新增 anchor 行做对齐，避免每轮对整批增量 frame 全量重算。
        """
        anchor_df = frames.get(self._anchor, pd.DataFrame())
        if anchor_df.empty:
            return pd.DataFrame()

        anchor_idx = self._get_incremental_anchor_index(anchor_df, frames=frames)
        if len(anchor_idx) == 0:
            return pd.DataFrame()

        tol_td = self._parse_timedelta(self._tolerance) or pd.Timedelta(0)
        lookback_td = self._parse_timedelta(self._lookback) or pd.Timedelta(0)
        recompute_td = self._parse_timedelta(self._tail_recompute_window) or pd.Timedelta(0)
        margin = max(tol_td, lookback_td, recompute_td)

        left_min = anchor_idx.min() - margin
        left_max = anchor_idx.max() + margin

        sliced_frames: Dict[str, pd.DataFrame] = {}
        for table, df in frames.items():
            if df.empty:
                sliced_frames[table] = df
                continue

            if table == self._anchor:
                # 保持和 anchor_idx 相同的时间窗口
                sliced_frames[table] = df[df.index.isin(anchor_idx)]
                continue

            # 非锚表仅保留潜在匹配窗口，减少匹配输入规模
            sliced_frames[table] = df[(df.index >= left_min) & (df.index <= left_max)]

        return align_asof(sliced_frames, anchor=self._anchor, tolerance=self._tolerance, direction=self._direction)

    def update(self, force_full: bool = False) -> int:
        """
        拉取增量数据并合并到缓存

        如果启用了本地缓存（cache_path），会：
        1. 初始化时自动加载本地缓存（如果 auto_load=True）
        2. 从数据库拉取增量数据
        3. 合并到内存缓存
        4. 自动保存到本地（如果 auto_save=True）

        Args:
            force_full: 强制全量读取（忽略 watermark）

        Returns:
            新增行数

        Raises:
            RuntimeError: 离线模式下调用 update()

        Examples:
            >>> new_rows = cache.update()
            >>> print(f"新增 {new_rows} 行")
        """
        t_total_start = time.perf_counter()
        timings: Dict[str, float] = {}

        # 离线模式检查
        if self._reader is None:
            raise RuntimeError(
                "离线模式下无法调用 update()。"
                "请使用 reader 参数初始化，或直接使用 load() 加载本地缓存。"
            )

        if force_full:
            # 重置 watermark 强制全量读取
            for spec in self._specs:
                self._reader.reset_watermark(spec.table)

        # 读取增量数据
        t0 = time.perf_counter()
        frames = self._reader.read_multiple(self._specs, lookback=self._lookback)
        timings["read_s"] = time.perf_counter() - t0
        timings["read_rows"] = float(sum(len(df) for df in frames.values()))
        frame_sig = self._frames_signature(frames)
        changed_tables: List[str]
        if self._last_frames_signature is None:
            changed_tables = sorted(frame_sig.keys())
        else:
            all_tables = set(frame_sig.keys()) | set(self._last_frames_signature.keys())
            changed_tables = sorted([t for t in all_tables if frame_sig.get(t) != self._last_frames_signature.get(t)])
        timings["frame_changed_tables"] = changed_tables

        # 空跑短路：连续两轮输入完全一致，跳过对齐和合并
        if not force_full and self._last_frames_signature is not None and not changed_tables:
            timings["align_s"] = 0.0
            timings["align_rows"] = 0.0
            timings["merge_s"] = 0.0
            timings["memory_s"] = 0.0
            timings["save_s"] = 0.0
            timings["skipped_align"] = True
            timings["total_s"] = time.perf_counter() - t_total_start
            self._last_update_timing = timings
            if self._timing_log:
                print(
                    "[AlignedData.update] "
                    f"force_full={force_full} read={timings['read_s']:.4f}s "
                    "align=0.0000s merge=0.0000s memory=0.0000s save=0.0000s "
                    f"total={timings['total_s']:.4f}s rows_in={int(timings['read_rows'])} "
                    f"rows_out=0 cache_rows={len(self._data)} skipped_align=True"
                )
            return 0

        # 对齐数据
        t0 = time.perf_counter()
        if force_full:
            new_data = align_asof(frames, anchor=self._anchor, tolerance=self._tolerance, direction=self._direction)
        else:
            new_data = self._align_incremental(frames)
        timings["align_s"] = time.perf_counter() - t0
        timings["align_rows"] = float(len(new_data))
        timings["skipped_align"] = False
        self._last_frames_signature = frame_sig

        if new_data.empty:
            timings["merge_s"] = 0.0
            timings["memory_s"] = 0.0
            timings["save_s"] = 0.0
            timings["total_s"] = time.perf_counter() - t_total_start
            self._last_update_timing = timings
            if self._timing_log:
                print(
                    "[AlignedData.update] "
                    f"force_full={force_full} read={timings['read_s']:.4f}s "
                    f"align={timings['align_s']:.4f}s merge=0.0000s "
                    f"memory=0.0000s save=0.0000s total={timings['total_s']:.4f}s "
                    f"rows_in={int(timings['read_rows'])} rows_out=0 cache_rows={len(self._data)}"
                )
            return 0

        # 合并到缓存
        rows_before = len(self._data)
        t0 = time.perf_counter()
        self._merge_data(new_data)
        timings["merge_s"] = time.perf_counter() - t0
        rows_after = len(self._data)
        new_rows = rows_after - rows_before

        # 更新统计
        self._total_updates += 1
        self._total_rows_added += new_rows
        self._last_update_time = datetime.now()

        # 检查内存限制
        t0 = time.perf_counter()
        self._check_memory_limits()
        timings["memory_s"] = time.perf_counter() - t0

        # 自动保存
        t0 = time.perf_counter()
        if self._auto_save and self._cache_path:
            self.save(self._cache_path)
        timings["save_s"] = time.perf_counter() - t0

        timings["total_s"] = time.perf_counter() - t_total_start
        self._last_update_timing = timings
        if self._timing_log:
            print(
                "[AlignedData.update] "
                f"force_full={force_full} read={timings['read_s']:.4f}s "
                f"align={timings['align_s']:.4f}s merge={timings['merge_s']:.4f}s "
                f"memory={timings['memory_s']:.4f}s save={timings['save_s']:.4f}s "
                f"total={timings['total_s']:.4f}s rows_in={int(timings['read_rows'])} "
                f"rows_out={int(timings['align_rows'])} new_rows={new_rows} cache_rows={len(self._data)}"
            )

        return new_rows

    def _merge_data(self, new_data: pd.DataFrame):
        """
        合并新数据到缓存

        实现逻辑：
        1. 使用 pd.concat 合并新旧数据
        2. 按时间索引去重（保留最新）
        3. 自动排序
        """
        if new_data.empty:
            return

        # 新数据先去重（保留最新），避免全量 concat 后再去重
        if new_data.index.has_duplicates:
            new_data = new_data[~new_data.index.duplicated(keep="last")]

        if self._data.empty:
            self._data = new_data.copy()
            return

        data_sorted = self._data.index.is_monotonic_increasing
        new_sorted = new_data.index.is_monotonic_increasing

        def _merge_existing(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
            overlap = existing.index.intersection(incoming.index)
            if not overlap.empty:
                # 仅用 incoming 的非空值覆盖，避免把已有数据替换成 NaN
                incoming_overlap = incoming.loc[overlap]
                existing_overlap = existing.loc[overlap, incoming.columns]
                merged_overlap = incoming_overlap.combine_first(existing_overlap)
                existing.loc[overlap, incoming.columns] = merged_overlap

            new_idx = incoming.index.difference(existing.index)
            if not new_idx.empty:
                existing = pd.concat([existing, incoming.loc[new_idx]], axis=0, copy=False)

            if not existing.index.is_monotonic_increasing:
                existing = existing.sort_index()
            return existing

        # 局部合并：利用 lookback 只处理尾部窗口
        if data_sorted and new_sorted:
            lookback_td = None
            if self._lookback is not None:
                try:
                    lookback_td = pd.Timedelta(self._lookback)
                except (ValueError, TypeError):
                    lookback_td = None

            if lookback_td is not None:
                try:
                    new_min = new_data.index[0]
                except (IndexError, KeyError):
                    new_min = new_data.index.min()

                cutoff = new_min - lookback_td
                if cutoff > self._data.index[0]:
                    start = self._data.index.searchsorted(cutoff, side="left")
                    if start > 0:
                        head = self._data.iloc[:start]
                        tail = self._data.iloc[start:]
                        merged_tail = _merge_existing(tail, new_data)
                        self._data = pd.concat([head, merged_tail], axis=0, copy=False)
                        return

            # 快路径：无重叠、时间递增，直接追加
            try:
                if new_data.index[0] > self._data.index[-1]:
                    self._data = pd.concat([self._data, new_data], axis=0, copy=False)
                    return
            except (IndexError, KeyError):
                pass

        # 常规路径：仅更新重叠索引，再追加新索引，避免全量 concat
        self._data = _merge_existing(self._data, new_data)

    def _check_memory_limits(self):
        """检查并应用内存限制"""
        if self._data.empty:
            return

        # 1. 时间窗口限制
        if self._time_window_days is not None:
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=self._time_window_days)
            if self._data.index.is_monotonic_increasing:
                self._data = self._data.loc[cutoff:]
            else:
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
            "cache_path": str(self._cache_path) if self._cache_path else None,
            "auto_save": self._auto_save,
            "timing_log": self._timing_log,
            "offline_mode": self._reader is None,
            "last_update_timing": self._last_update_timing.copy() if self._last_update_timing else None,
        }

    @property
    def cache_path(self) -> Optional[Path]:
        """本地缓存文件路径"""
        return self._cache_path

    @property
    def is_offline(self) -> bool:
        """是否为离线模式（无数据库连接）"""
        return self._reader is None

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
        self._last_frames_signature = None

        # 检查内存限制
        self._check_memory_limits()

    def clear(self):
        """清空缓存数据"""
        self._data = pd.DataFrame()
        self._data.index.name = "timestamp"
        self._total_updates = 0
        self._total_rows_added = 0
        self._last_update_time = None
        self._last_update_timing = {}
        self._last_frames_signature = None

    def reset(self, reset_watermark: bool = False):
        """
        重置缓存

        Args:
            reset_watermark: 是否同时重置 reader 的 watermark
        """
        self.clear()

        if reset_watermark and self._reader is not None:
            for spec in self._specs:
                self._reader.reset_watermark(spec.table)

    def __repr__(self):
        mode = "offline" if self._reader is None else "online"
        cache_info = f", cache='{self._cache_path}'" if self._cache_path else ""

        if self._data.empty:
            return f"AlignedDataCache(empty, anchor='{self._anchor}', mode={mode}{cache_info})"

        time_range = self.time_range
        return (
            f"AlignedDataCache("
            f"rows={len(self._data)}, "
            f"cols={len(self._data.columns)}, "
            f"range={time_range[0] if time_range else 'N/A'} to {time_range[1] if time_range else 'N/A'}, "
            f"anchor='{self._anchor}', "
            f"memory={self.memory_usage_mb:.1f}MB, "
            f"mode={mode}{cache_info})"
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
        return_overview: bool = False,
        **kwargs,
    ) -> "go.Figure":
        """
        绘制同步的温度和压强子图（从 cache 中自动读取数据）

        自动识别温度和压强列，然后创建同步子图。

        Args:
            temp_columns: 温度列名列表（可选，如果为 None 则自动识别）
            pressure_columns: 压强列名列表（可选，如果为 None 则自动识别）
            time_range: 时间范围（可选）
            return_overview: 是否返回额外的总览图（温度总览 + 压强总览）
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

            >>> # 返回额外总览图（压强 + 温度）
            >>> sync_fig, p_fig, t_fig = cache.plot_temp_pressure_sync(
            ...     time_range=('2025-12-15', '2025-12-16'),
            ...     return_overview=True
            ... )
            >>> p_fig.show(); t_fig.show()
        """
        from .visualizer import plot_temp_pressure_sync as _plot_temp_pressure_sync

        return _plot_temp_pressure_sync(
            self,
            temp_columns=temp_columns,
            pressure_columns=pressure_columns,
            time_range=time_range,
            return_overview=return_overview,
            **kwargs,
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
