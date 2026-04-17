"""
sc_reader.dashboard - 数据可视化 Dashboard 模块

提供交互式 Web Dashboard 用于：
- 时间序列数据可视化
- P-T 相图显示
- 交互式时间范围选择
- 自动数据刷新

Examples:
    # 方式 1: 快速启动
    >>> from sc_reader.dashboard import run_dashboard
    >>> run_dashboard()

    # 方式 2: 使用配置文件
    >>> run_dashboard("./sc_config.json", port=8080)

    # 方式 3: 完全控制
    >>> from sc_reader.dashboard import SCDashboard, DashboardConfig
    >>> config = DashboardConfig(anchor_table="tempdata", gas="xenon")
    >>> dashboard = SCDashboard(config=config)
    >>> dashboard.run()

    # 方式 4: 命令行
    $ python -m sc_reader.dashboard --port 8051 --debug
"""

from typing import Optional, Union

from .app import SCDashboard
from .config import DashboardConfig


def run_dashboard(
    config: Optional[Union[str, DashboardConfig]] = None,
    host: str = "127.0.0.1",
    port: int = 8051,
    debug: bool = True,
) -> None:
    """快速启动 Dashboard

    Args:
        config: 配置文件路径或 DashboardConfig 实例
        host: 服务器主机地址
        port: 服务器端口
        debug: 是否启用调试模式

    Examples:
        # 使用默认配置
        >>> run_dashboard()

        # 使用配置文件
        >>> run_dashboard("./sc_config.json")

        # 使用自定义配置
        >>> config = DashboardConfig(gas="xenon", port=8080)
        >>> run_dashboard(config)
    """
    if isinstance(config, str):
        dashboard = SCDashboard.from_config(config)
    elif isinstance(config, DashboardConfig):
        dashboard = SCDashboard(config=config)
    else:
        dashboard = SCDashboard()

    dashboard.run(host=host, port=port, debug=debug)


__all__ = [
    "DashboardConfig",
    "SCDashboard",
    "run_dashboard",
]
