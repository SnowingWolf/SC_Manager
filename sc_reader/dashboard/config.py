"""
Dashboard 配置数据类

提供 DashboardConfig 用于配置 Dashboard 的各项参数，
支持从 JSON 配置文件加载。
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class DashboardConfig:
    """Dashboard 配置类

    Attributes:
        anchor_table: 锚定表名（用于时间对齐）
        time_col: 时间列名
        table_specs: 表规格列表 [(table, time_col), ...]

        gas: 相图气体类型 ("argon" 或 "xenon")
        phase_pressure_primary: 主压力列名
        phase_pressure_secondary: 次压力列名（可选）
        phase_temperatures: 温度列名列表
        T_range: 温度范围 (min, max)
        P_range: 压力范围 (min, max)

        temp_scale: 温度缩放因子
        temp_offset: 温度偏移量（如 273.15 用于摄氏度转开尔文）
        press_scale: 压力缩放因子（如 0.01 用于 mbar 转 bar）
        press_offset: 压力偏移量

        initial_load_hours: 初始加载时间窗口（小时），None 表示全量加载
        ts_max_points: 时间序列图最大点数
        phase_max_points: 相图最大点数

        host: 服务器主机地址
        port: 服务器端口
        debug: 是否启用调试模式
        poll_interval: 数据刷新间隔（秒）
    """

    # 表配置
    anchor_table: str = "piddata"
    time_col: str = "timestamp"
    table_specs: List[Tuple[str, str]] = field(default_factory=lambda: [
        ("tempdata", "timestamp"),
        ("runlidata", "timestamp"),
        ("statedata", "timestamp"),
        ("piddata", "timestamp"),
    ])
    table_columns: Optional[Dict[str, List[str]]] = None

    # 相图配置
    gas: str = "argon"
    phase_pressure_primary: str = "runlidata__Pressure5"
    phase_pressure_secondary: Optional[str] = "runlidata__Pressure6"
    phase_temperatures: List[str] = field(default_factory=lambda: [
        "piddata__A_Temperature",
        "piddata__B_Temperature",
        "piddata__C_Temperature",
        "piddata__D_Temperature",
    ])
    T_range: Tuple[float, float] = (80, 110)
    P_range: Tuple[float, float] = (0.5, 3.5)

    # 单位转换
    temp_scale: float = 1.0
    temp_offset: float = 0.0
    press_scale: float = 1.0
    press_offset: float = 0.0

    # 显示设置
    initial_load_hours: Optional[int] = 6
    ts_max_points: int = 8000
    phase_max_points: int = 10000

    # 服务器设置
    host: str = "127.0.0.1"
    port: int = 8051
    debug: bool = True
    poll_interval: float = 5.0

    @classmethod
    def from_json(cls, path: Optional[str] = None) -> "DashboardConfig":
        """从 JSON 配置文件加载配置

        配置文件查找顺序：
        1. 显式指定的路径
        2. SC_CONFIG_PATH 环境变量
        3. ./sc_config.json
        4. ~/.sc_config.json

        Args:
            path: 配置文件路径（可选）

        Returns:
            DashboardConfig 实例
        """
        config_path = cls._find_config_path(path)
        if config_path is None:
            return cls()

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return cls()

        return cls._from_dict(data)

    @classmethod
    def _find_config_path(cls, path: Optional[str]) -> Optional[str]:
        """查找配置文件路径"""
        candidates = []
        if path:
            candidates.append(path)
        if env_path := os.environ.get("SC_CONFIG_PATH"):
            candidates.append(env_path)
        candidates.extend([
            "./sc_config.json",
            str(Path.home() / ".sc_config.json"),
        ])

        for candidate in candidates:
            if Path(candidate).is_file():
                return candidate
        return None

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "DashboardConfig":
        """从字典创建配置"""
        dashboard_data = data.get("dashboard", {})
        align_data = data.get("align", {})

        kwargs: Dict[str, Any] = {}

        # 表配置
        tables_cfg = dashboard_data.get("tables", {})
        if anchor := tables_cfg.get("anchor"):
            kwargs["anchor_table"] = anchor
        if time_col := tables_cfg.get("time_col"):
            kwargs["time_col"] = time_col
        if specs := tables_cfg.get("specs"):
            kwargs["table_specs"] = [
                (s["table"], s.get("time_col", "timestamp"))
                for s in specs
            ]
        if columns := tables_cfg.get("columns"):
            kwargs["table_columns"] = columns

        # 相图配置
        phase_cfg = dashboard_data.get("phase_diagram", {})
        if gas := phase_cfg.get("gas"):
            kwargs["gas"] = gas
        if pressure_primary := phase_cfg.get("pressure_primary"):
            kwargs["phase_pressure_primary"] = pressure_primary
        if pressure_secondary := phase_cfg.get("pressure_secondary"):
            kwargs["phase_pressure_secondary"] = pressure_secondary
        if temperatures := phase_cfg.get("temperatures"):
            kwargs["phase_temperatures"] = temperatures
        if T_range := phase_cfg.get("T_range"):
            kwargs["T_range"] = tuple(T_range)
        if P_range := phase_cfg.get("P_range"):
            kwargs["P_range"] = tuple(P_range)

        # 单位转换
        units_cfg = dashboard_data.get("units", {})
        if temp_scale := units_cfg.get("temp_scale"):
            kwargs["temp_scale"] = temp_scale
        if temp_offset := units_cfg.get("temp_offset"):
            kwargs["temp_offset"] = temp_offset
        if press_scale := units_cfg.get("press_scale"):
            kwargs["press_scale"] = press_scale
        if press_offset := units_cfg.get("press_offset"):
            kwargs["press_offset"] = press_offset

        # 显示设置
        display_cfg = dashboard_data.get("display", {})
        if initial_load_hours := display_cfg.get("initial_load_hours"):
            kwargs["initial_load_hours"] = initial_load_hours
        if ts_max_points := display_cfg.get("ts_max_points"):
            kwargs["ts_max_points"] = ts_max_points
        if phase_max_points := display_cfg.get("phase_max_points"):
            kwargs["phase_max_points"] = phase_max_points

        # 服务器设置
        server_cfg = dashboard_data.get("server", {})
        if host := server_cfg.get("host"):
            kwargs["host"] = host
        if port := server_cfg.get("port"):
            kwargs["port"] = port
        if "debug" in server_cfg:
            kwargs["debug"] = server_cfg["debug"]

        # 从 align 配置获取 poll_interval
        if poll_interval := align_data.get("poll_interval"):
            kwargs["poll_interval"] = poll_interval

        return cls(**kwargs)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "tables": {
                "anchor": self.anchor_table,
                "time_col": self.time_col,
                "specs": [
                    {"table": t, "time_col": tc}
                    for t, tc in self.table_specs
                ],
                "columns": self.table_columns,
            },
            "phase_diagram": {
                "gas": self.gas,
                "pressure_primary": self.phase_pressure_primary,
                "pressure_secondary": self.phase_pressure_secondary,
                "temperatures": self.phase_temperatures,
                "T_range": list(self.T_range),
                "P_range": list(self.P_range),
            },
            "units": {
                "temp_scale": self.temp_scale,
                "temp_offset": self.temp_offset,
                "press_scale": self.press_scale,
                "press_offset": self.press_offset,
            },
            "display": {
                "initial_load_hours": self.initial_load_hours,
                "ts_max_points": self.ts_max_points,
                "phase_max_points": self.phase_max_points,
            },
            "server": {
                "host": self.host,
                "port": self.port,
                "debug": self.debug,
            },
        }
