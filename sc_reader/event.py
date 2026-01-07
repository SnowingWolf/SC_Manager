"""
事件检测与窗口读取模块

功能：
1. 事件检测：在增量读取过程中检测特定事件
   - EdgeTrigger: 开关沿触发（rising/falling edge）
   - StepTrigger: 设定值阶跃触发
2. 事件窗口读取：检测到事件后读取时间窗口数据并对齐合并
3. 事件监控：持续轮询并触发用户回调

数据库表：
- piddata: PID温度 (timestamp varchar)
- runlidata: 压力/流量 (timestamp datetime)
- statedata: 设备状态 (timestamp datetime)
- tempdata: 温度传感器 (timestamp datetime, anchor)
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import time

import pandas as pd

from .spec import TableSpec
from .reader import SCReader
from .align import align_asof


class TriggerType(Enum):
    """触发类型"""
    RISING_EDGE = 'rising_edge'    # 0 -> 1
    FALLING_EDGE = 'falling_edge'  # 1 -> 0
    BOTH_EDGE = 'both_edge'        # 0 -> 1 或 1 -> 0
    STEP_CHANGE = 'step_change'    # 值变化超过阈值


@dataclass
class EventSpec:
    """
    事件规格定义

    Attributes:
        name: 事件名称
        table: 源表名
        column: 触发列名
        trigger_type: 触发类型
        threshold: 阈值（用于 STEP_CHANGE）
        enabled: 是否启用
    """
    name: str
    table: str
    column: str
    trigger_type: TriggerType
    threshold: float = 0.5  # 用于 STEP_CHANGE
    enabled: bool = True


@dataclass
class Event:
    """
    事件实例

    Attributes:
        event_id: 事件ID（自增）
        event_type: 事件类型名称
        event_time: 事件发生时间
        source_table: 源表名
        trigger_col: 触发列名
        trigger_type: 触发类型
        value_from: 变化前的值
        value_to: 变化后的值
        metadata: 额外元数据
    """
    event_id: int
    event_type: str
    event_time: datetime
    source_table: str
    trigger_col: str
    trigger_type: TriggerType
    value_from: Any
    value_to: Any
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self):
        return (f"Event(id={self.event_id}, type='{self.event_type}', "
                f"time={self.event_time}, {self.trigger_col}: {self.value_from}->{self.value_to})")


@dataclass
class WindowConfig:
    """
    事件窗口配置

    Attributes:
        pre_seconds: 事件前窗口（秒）
        post_seconds: 事件后窗口（秒）
        anchor_table: 锚表名
        tolerance: 对齐容差
        direction: 对齐方向
        ffill_tables: 需要 forward-fill 的表
    """
    pre_seconds: float = 30.0
    post_seconds: float = 120.0
    anchor_table: str = 'tempdata'
    tolerance: str = '2s'
    direction: str = 'backward'
    ffill_tables: List[str] = field(default_factory=lambda: ['statedata'])


class EventDetector:
    """
    事件检测器

    检测增量数据中的事件，支持：
    - 开关沿触发（rising/falling edge）
    - 设定值阶跃触发（step change）

    Examples:
        >>> detector = EventDetector()
        >>> detector.add_edge_trigger('valve_open', 'statedata', 'Valve_N2', TriggerType.RISING_EDGE)
        >>> detector.add_step_trigger('coldwater_change', 'runlidata', 'coldwater_Set', threshold=0.5)
        >>> events = detector.detect(df_statedata, 'statedata')
    """

    def __init__(self):
        self._specs: Dict[str, EventSpec] = {}
        self._event_counter = 0
        # 记录每个 (table, column) 的上一个值，用于检测变化
        self._last_values: Dict[tuple, Any] = {}

    def add_edge_trigger(
        self,
        name: str,
        table: str,
        column: str,
        trigger_type: TriggerType = TriggerType.RISING_EDGE,
        enabled: bool = True
    ):
        """添加开关沿触发事件"""
        if trigger_type not in (TriggerType.RISING_EDGE, TriggerType.FALLING_EDGE, TriggerType.BOTH_EDGE):
            raise ValueError("Edge trigger 必须是 RISING_EDGE/FALLING_EDGE/BOTH_EDGE")
        self._specs[name] = EventSpec(name, table, column, trigger_type, enabled=enabled)

    def add_step_trigger(
        self,
        name: str,
        table: str,
        column: str,
        threshold: float = 0.5,
        enabled: bool = True
    ):
        """添加设定值阶跃触发事件"""
        self._specs[name] = EventSpec(name, table, column, TriggerType.STEP_CHANGE, threshold, enabled)

    def detect(self, df: pd.DataFrame, table: str) -> List[Event]:
        """
        检测 DataFrame 中的事件

        Args:
            df: 增量数据 DataFrame，索引为时间
            table: 表名

        Returns:
            检测到的事件列表
        """
        if df.empty:
            return []

        events = []

        for spec in self._specs.values():
            if not spec.enabled or spec.table != table:
                continue

            if spec.column not in df.columns:
                continue

            col_events = self._detect_column(df, spec)
            events.extend(col_events)

        return events

    def _detect_column(self, df: pd.DataFrame, spec: EventSpec) -> List[Event]:
        """检测单列的事件"""
        events = []
        column = spec.column
        table = spec.table
        key = (table, column)

        # 获取上一个值
        last_value = self._last_values.get(key)

        # 遍历数据行检测变化
        for idx, row in df.iterrows():
            current_value = row[column]

            # 跳过 NaN
            if pd.isna(current_value):
                continue

            if last_value is not None and not pd.isna(last_value):
                event = self._check_trigger(spec, idx, last_value, current_value)
                if event:
                    events.append(event)

            last_value = current_value

        # 更新最后值
        if last_value is not None:
            self._last_values[key] = last_value

        return events

    def _check_trigger(
        self,
        spec: EventSpec,
        event_time: datetime,
        value_from: Any,
        value_to: Any
    ) -> Optional[Event]:
        """检查是否触发事件"""
        triggered = False

        if spec.trigger_type == TriggerType.RISING_EDGE:
            # 0 -> 1 (或 0 -> 非0)
            triggered = (value_from == 0 and value_to != 0)

        elif spec.trigger_type == TriggerType.FALLING_EDGE:
            # 1 -> 0 (或 非0 -> 0)
            triggered = (value_from != 0 and value_to == 0)

        elif spec.trigger_type == TriggerType.BOTH_EDGE:
            # 任意边沿
            triggered = (value_from == 0 and value_to != 0) or (value_from != 0 and value_to == 0)

        elif spec.trigger_type == TriggerType.STEP_CHANGE:
            # 值变化超过阈值
            try:
                delta = abs(float(value_to) - float(value_from))
                triggered = delta >= spec.threshold
            except (TypeError, ValueError):
                triggered = False

        if triggered:
            self._event_counter += 1
            return Event(
                event_id=self._event_counter,
                event_type=spec.name,
                event_time=event_time if isinstance(event_time, datetime) else event_time.to_pydatetime(),
                source_table=spec.table,
                trigger_col=spec.column,
                trigger_type=spec.trigger_type,
                value_from=value_from,
                value_to=value_to
            )

        return None

    def reset(self):
        """重置检测器状态"""
        self._last_values.clear()
        self._event_counter = 0


class EventWindowReader:
    """
    事件窗口读取器

    检测到事件后，从多个表读取时间窗口数据并对齐合并。

    Examples:
        >>> reader = SCReader()
        >>> window_reader = EventWindowReader(reader, window_config)
        >>> df = window_reader.read_window(event)
    """

    def __init__(
        self,
        reader: SCReader,
        config: Optional[WindowConfig] = None,
        table_specs: Optional[Dict[str, TableSpec]] = None
    ):
        """
        初始化窗口读取器

        Args:
            reader: SCReader 实例
            config: 窗口配置
            table_specs: 表规格字典 {table_name: TableSpec}
        """
        self.reader = reader
        self.config = config or WindowConfig()

        # 默认表规格
        self.table_specs = table_specs or {
            'tempdata': TableSpec('tempdata', 'timestamp', key_col='id'),
            'runlidata': TableSpec('runlidata', 'timestamp', key_col='id'),
            'statedata': TableSpec('statedata', 'timestamp', key_col='id'),
            'piddata': TableSpec('piddata', 'timestamp', key_col='id'),
        }

    def read_window(self, event: Event) -> pd.DataFrame:
        """
        读取事件窗口数据并对齐

        Args:
            event: 事件实例

        Returns:
            对齐后的 DataFrame，包含 t_seconds 列
        """
        event_time = event.event_time
        pre = timedelta(seconds=self.config.pre_seconds)
        post = timedelta(seconds=self.config.post_seconds)

        start_time = event_time - pre
        end_time = event_time + post

        # 从各表读取窗口数据
        frames: Dict[str, pd.DataFrame] = {}

        for table_name, spec in self.table_specs.items():
            df = self._read_table_window(spec, start_time, end_time)
            frames[table_name] = df

        # 检查 anchor 表是否有数据
        anchor = self.config.anchor_table
        if anchor not in frames or frames[anchor].empty:
            return pd.DataFrame()

        # 对齐合并
        aligned = align_asof(
            frames,
            anchor=anchor,
            tolerance=self.config.tolerance,
            direction=self.config.direction
        )

        if aligned.empty:
            return aligned

        # 对 statedata 列做 forward-fill
        for table in self.config.ffill_tables:
            ffill_cols = [c for c in aligned.columns if c.startswith(f'{table}__')]
            if ffill_cols:
                aligned[ffill_cols] = aligned[ffill_cols].ffill()

        # 添加 t_seconds 列
        aligned = aligned.reset_index()
        time_col = aligned.columns[0]
        aligned['t_seconds'] = (aligned[time_col] - event_time).dt.total_seconds()

        # 重命名时间列
        aligned = aligned.rename(columns={time_col: 'timestamp'})

        # 添加事件元数据
        aligned['event_id'] = event.event_id
        aligned['event_type'] = event.event_type
        aligned['event_time'] = event_time

        return aligned

    def _read_table_window(
        self,
        spec: TableSpec,
        start_time: datetime,
        end_time: datetime
    ) -> pd.DataFrame:
        """读取单表的时间窗口数据"""
        table = spec.table
        time_col = spec.time_col or 'timestamp'

        # 构建 SQL
        start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

        # 使用 reader 的底层查询
        # 注意 piddata 的时间列是 varchar，需要特殊处理
        if table == 'piddata':
            # varchar 时间列，直接字符串比较（格式一致时有效）
            sql = f"""
                SELECT * FROM `{table}`
                WHERE `{time_col}` >= '{start_str}'
                  AND `{time_col}` <= '{end_str}'
                ORDER BY `{time_col}`
            """
        else:
            # datetime 时间列
            sql = f"""
                SELECT * FROM `{table}`
                WHERE `{time_col}` >= '{start_str}'
                  AND `{time_col}` <= '{end_str}'
                ORDER BY `{time_col}`
            """

        try:
            df = self.reader.query_df(sql, time_column=time_col)
            return df
        except Exception as e:
            print(f"读取 {table} 窗口数据失败: {e}")
            return pd.DataFrame()


def run_event_monitor(
    reader: SCReader,
    detector: EventDetector,
    on_event: Callable[[pd.DataFrame, Event], None],
    window_config: Optional[WindowConfig] = None,
    table_specs: Optional[Dict[str, TableSpec]] = None,
    monitor_tables: Optional[List[str]] = None,
    poll_interval: float = 5.0,
    lookback: str = '2s'
):
    """
    运行事件监控

    持续增量读取数据，检测事件，触发用户回调。

    Args:
        reader: SCReader 实例
        detector: EventDetector 实例
        on_event: 事件回调函数 (aligned_df, event) -> None
        window_config: 窗口配置
        table_specs: 表规格字典
        monitor_tables: 要监控的表列表，默认 ['statedata', 'runlidata']
        poll_interval: 轮询间隔（秒）
        lookback: 回看窗口

    Examples:
        >>> reader = SCReader(state_path='./event_watermark.json')
        >>> detector = EventDetector()
        >>> detector.add_edge_trigger('valve_open', 'statedata', 'Valve_N2', TriggerType.RISING_EDGE)
        >>>
        >>> def handle_event(df, event):
        ...     print(f"检测到事件: {event}")
        ...     df.to_csv(f'event_{event.event_id}.csv')
        >>>
        >>> run_event_monitor(reader, detector, handle_event)
    """
    window_config = window_config or WindowConfig()
    monitor_tables = monitor_tables or ['statedata', 'runlidata']

    # 默认表规格
    default_specs = {
        'tempdata': TableSpec('tempdata', 'timestamp', key_col='id'),
        'runlidata': TableSpec('runlidata', 'timestamp', key_col='id'),
        'statedata': TableSpec('statedata', 'timestamp', key_col='id'),
        'piddata': TableSpec('piddata', 'timestamp', key_col='id'),
    }
    table_specs = table_specs or default_specs

    window_reader = EventWindowReader(reader, window_config, table_specs)

    print("事件监控启动")
    print(f"监控表: {monitor_tables}")
    print(f"轮询间隔: {poll_interval}s")
    print(f"窗口: -{window_config.pre_seconds}s ~ +{window_config.post_seconds}s")
    print("(Ctrl+C 停止)")
    print()

    try:
        while True:
            # 增量读取监控表
            for table in monitor_tables:
                if table not in table_specs:
                    continue

                spec = table_specs[table]
                df = reader.read_incremental(spec, lookback=lookback)

                if df.empty:
                    continue

                # 检测事件
                events = detector.detect(df, table)

                # 处理每个事件
                for event in events:
                    ts = datetime.now().strftime('%H:%M:%S')
                    print(f"[{ts}] 检测到事件: {event}")

                    try:
                        # 读取事件窗口
                        aligned_df = window_reader.read_window(event)

                        if not aligned_df.empty:
                            print(f"  窗口数据: {len(aligned_df)} 行, {len(aligned_df.columns)} 列")
                            # 调用用户回调
                            on_event(aligned_df, event)
                        else:
                            print("  窗口数据为空")

                    except Exception as e:
                        print(f"  处理事件失败: {e}")

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        print("\n事件监控已停止")


# 便捷函数：创建常用事件检测器
def create_default_detector() -> EventDetector:
    """
    创建默认事件检测器

    包含：
    - valve_open: Valve_N2 从 0->1
    - valve_close: Valve_N2 从 1->0
    - coldwater_change: coldwater_Set 变化 >= 0.5
    """
    detector = EventDetector()
    detector.add_edge_trigger('valve_open', 'statedata', 'Valve_N2', TriggerType.RISING_EDGE)
    detector.add_edge_trigger('valve_close', 'statedata', 'Valve_N2', TriggerType.FALLING_EDGE)
    detector.add_step_trigger('coldwater_change', 'runlidata', 'coldwater_Set', threshold=0.5)
    return detector
