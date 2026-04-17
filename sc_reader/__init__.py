"""
sc_reader - 慢控数据读取、增量同步、事件检测与可视化库

提供便捷的 API 用于：
1. 从 MySQL 数据库读取慢控数据（SCReader，支持全量和增量读取）
2. 多表时间对齐合并（align_asof, collect_and_align）
3. 时间索引数据缓存管理（AlignedDataCache）
4. 事件检测与窗口读取（EventDetector, EventWindowReader）
5. 数据可视化分析（plot_* 系列函数）
6. 交互式 Dashboard（SCDashboard, run_dashboard）

Examples:
    # 基础用法：全量读取
    >>> from sc_reader import SCReader
    >>> reader = SCReader()
    >>> data = reader.query_by_time('tempdata', '2025-12-15', '2025-12-26')

    # 增量读取 + 时间对齐
    >>> from sc_reader import SCReader, TableSpec, collect_and_align
    >>> reader = SCReader(state_path='./watermark.json')
    >>> specs = [TableSpec('tempdata', 'timestamp'), TableSpec('runlidata', 'timestamp')]
    >>> df = collect_and_align(reader, specs, anchor='tempdata')

    # 事件监控
    >>> from sc_reader import EventDetector, TriggerType, run_event_monitor
    >>> detector = EventDetector()
    >>> detector.add_edge_trigger('valve_open', 'statedata', 'Valve_N2', TriggerType.RISING_EDGE)
    >>> run_event_monitor(reader, detector, on_event=lambda df, e: print(e))

    # Dashboard
    >>> from sc_reader.dashboard import run_dashboard
    >>> run_dashboard()
"""

from .align import align_asof, collect_and_align
from .cache import AlignedData
from .config import DEFAULT_ALIGN_CONFIG, DEFAULT_MYSQL_CONFIG, AlignConfig, MySQLConfig
from .event import (
    Event,
    EventDetector,
    EventSpec,
    EventWindowReader,
    TriggerType,
    WindowConfig,
    create_default_detector,
    run_event_monitor,
)
from .phase_diagram import (
    # 新 API
    GAS_PROPERTIES,
    get_phase,
    phase_boundary_bar,
    plot_pt_path,
    psat_bar,
    psub_bar,
)
from .reader import SCReader
from .spec import TableSpec
from .visualizer import (
    interactive_pt_diagram,
    plot_boxplot,
    plot_correlation,
    plot_distribution,
    plot_dual_axis,
    plot_rolling_stats,
    plot_subplots,
    plot_temp_pressure_sync,
    plot_timeseries,
)

__version__ = "1.4.0"
__author__ = "SC_Manager"

__all__ = [
    # 配置
    "MySQLConfig",
    "AlignConfig",
    "DEFAULT_MYSQL_CONFIG",
    "DEFAULT_ALIGN_CONFIG",
    # 规格
    "TableSpec",
    # 读取器
    "SCReader",
    # 对齐
    "align_asof",
    "collect_and_align",
    # 缓存管理
    "AlignedData",
    # 事件检测
    "TriggerType",
    "EventSpec",
    "Event",
    "WindowConfig",
    "EventDetector",
    "EventWindowReader",
    "run_event_monitor",
    "create_default_detector",
    # 可视化
    "plot_timeseries",
    "plot_dual_axis",
    "plot_subplots",
    "plot_temp_pressure_sync",
    "plot_distribution",
    "plot_boxplot",
    "plot_correlation",
    "plot_rolling_stats",
    "interactive_pt_diagram",
    # 相图 (新 API)
    "GAS_PROPERTIES",
    "psub_bar",
    "psat_bar",
    "phase_boundary_bar",
    "get_phase",
    "plot_pt_path",
    # Dashboard (通过 sc_reader.dashboard 子模块访问)
    # from sc_reader.dashboard import run_dashboard, SCDashboard, DashboardConfig
]
