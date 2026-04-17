"""
SCDashboard 应用工厂类

提供 Dashboard 应用的创建和管理。
"""

import atexit
from pathlib import Path
from typing import Optional, Union

from dash import Dash

from ..cache import AlignedData
from ..config import AlignConfig, MySQLConfig
from ..reader import SCReader
from ..spec import TableSpec
from .callbacks import register_callbacks
from .config import DashboardConfig
from .layouts import create_layout


class SCDashboard:
    """SC Dashboard 应用类

    提供数据可视化 Dashboard，包括：
    - 时间序列图（温度、压力）
    - P-T 相图
    - 交互式时间范围选择
    - 自动数据刷新

    Examples:
        # 快速启动
        >>> dashboard = SCDashboard()
        >>> dashboard.run()

        # 使用配置文件
        >>> dashboard = SCDashboard.from_config("./sc_config.json")
        >>> dashboard.run(port=8080)

        # 完全控制
        >>> from sc_reader.dashboard import DashboardConfig
        >>> config = DashboardConfig(anchor_table="tempdata", gas="xenon")
        >>> dashboard = SCDashboard(config=config)
        >>> dashboard.run()
    """

    def __init__(
        self,
        config: Optional[DashboardConfig] = None,
        mysql_config: Optional[MySQLConfig] = None,
        align_config: Optional[AlignConfig] = None,
        config_path: Optional[str] = None,
    ):
        """初始化 Dashboard

        Args:
            config: Dashboard 配置（可选）
            mysql_config: MySQL 配置（可选）
            align_config: 对齐配置（可选）
            config_path: 配置文件路径（可选）
        """
        # 加载配置
        self._config = config or DashboardConfig.from_json(config_path)
        self._mysql_config = mysql_config or MySQLConfig.from_json(config_path)
        self._align_config = align_config or AlignConfig.from_json(config_path)

        # 创建读取器和缓存
        self._reader = SCReader(
            config=self._mysql_config,
            state_path="./dash_watermark.json",
        )

        self._specs = [
            TableSpec(
                table,
                time_col,
                cols=(self._config.table_columns or {}).get(table),
            )
            for table, time_col in self._config.table_specs
        ]

        self._cache = AlignedData(
            self._reader,
            self._specs,
            anchor=self._config.anchor_table,
            tolerance="20s",
            direction=self._align_config.direction,
            lookback="20s",
        )

        # 注册清理函数
        atexit.register(self._cleanup)

        # 创建 Dash 应用
        self._app = Dash(__name__)
        self._setup_app()

    def _cleanup(self):
        """清理资源"""
        try:
            self._reader.close()
        except Exception:
            pass

    def _setup_app(self):
        """设置 Dash 应用"""
        poll_interval_ms = int(self._config.poll_interval * 1000)
        self._app.layout = create_layout(self._config, poll_interval_ms)
        register_callbacks(
            self._app,
            self._cache,
            self._reader,
            self._specs,
            self._config,
        )

    @classmethod
    def from_config(cls, path: str) -> "SCDashboard":
        """从配置文件创建 Dashboard

        Args:
            path: 配置文件路径

        Returns:
            SCDashboard 实例
        """
        return cls(config_path=path)

    def run(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        debug: Optional[bool] = None,
    ) -> None:
        """运行 Dashboard 服务器

        Args:
            host: 主机地址（覆盖配置）
            port: 端口号（覆盖配置）
            debug: 调试模式（覆盖配置）
        """
        self._app.run(
            host=host or self._config.host,
            port=port or self._config.port,
            debug=debug if debug is not None else self._config.debug,
        )

    @property
    def app(self) -> Dash:
        """获取底层 Dash 应用"""
        return self._app

    @property
    def cache(self) -> AlignedData:
        """获取数据缓存"""
        return self._cache

    @property
    def reader(self) -> SCReader:
        """获取数据读取器"""
        return self._reader

    @property
    def config(self) -> DashboardConfig:
        """获取 Dashboard 配置"""
        return self._config
