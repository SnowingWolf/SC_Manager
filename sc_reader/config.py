"""
配置管理模块

支持从 JSON 文件或环境变量加载配置。

配置文件路径优先级：
1. 显式传入的路径
2. 环境变量 SC_CONFIG_PATH
3. ./sc_config.json
4. ~/.sc_config.json
5. 使用默认值
"""
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# 默认配置值
_DEFAULTS = {
    'mysql': {
        'host': '10.11.50.141',
        'port': 3306,
        'user': 'read',
        'password': '111111',
        'database': 'slowcontroldata',
        'charset': 'utf8mb4',
    },
    'align': {
        'tolerance': '200ms',
        'direction': 'backward',
        'lookback': '2s',
        'chunksize': 200000,
        'poll_interval': 5.0,
    }
}


def _find_config_file() -> Optional[Path]:
    """
    按优先级查找配置文件

    Returns:
        配置文件路径，未找到返回 None
    """
    # 环境变量指定的路径
    env_path = os.getenv('SC_CONFIG_PATH')
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p

    # 当前目录
    local = Path('./sc_config.json')
    if local.exists():
        return local

    # 用户主目录
    home = Path.home() / '.sc_config.json'
    if home.exists():
        return home

    return None


def load_config(path: Optional[str] = None) -> dict:
    """
    从 JSON 文件加载配置

    Args:
        path: 配置文件路径，None 则自动查找

    Returns:
        配置字典，未找到文件则返回空字典
    """
    if path:
        config_path = Path(path)
    else:
        config_path = _find_config_file()

    if config_path and config_path.exists():
        return json.loads(config_path.read_text(encoding='utf-8'))

    return {}


@dataclass
class MySQLConfig:
    """
    MySQL 连接配置

    支持三种方式加载：
    1. 直接传参
    2. 从 JSON 文件加载（from_json）
    3. 从环境变量加载（默认行为）

    Examples:
        # 从 JSON 文件加载
        >>> config = MySQLConfig.from_json()
        >>> config = MySQLConfig.from_json('./my_config.json')

        # 直接传参
        >>> config = MySQLConfig(host='192.168.4.19')

        # 获取连接 URL
        >>> url = config.url
    """
    host: str = field(default_factory=lambda: os.getenv('MYSQL_HOST', _DEFAULTS['mysql']['host']))
    port: int = field(default_factory=lambda: int(os.getenv('MYSQL_PORT', str(_DEFAULTS['mysql']['port']))))
    user: str = field(default_factory=lambda: os.getenv('MYSQL_USER', _DEFAULTS['mysql']['user']))
    password: str = field(default_factory=lambda: os.getenv('MYSQL_PASSWORD', _DEFAULTS['mysql']['password']))
    database: str = field(default_factory=lambda: os.getenv('MYSQL_DATABASE', _DEFAULTS['mysql']['database']))
    charset: str = field(default=_DEFAULTS['mysql']['charset'])

    @classmethod
    def from_json(cls, path: Optional[str] = None) -> 'MySQLConfig':
        """
        从 JSON 配置文件创建实例

        Args:
            path: 配置文件路径，None 则自动查找

        Returns:
            MySQLConfig 实例
        """
        config = load_config(path)
        mysql_config = config.get('mysql', {})

        return cls(
            host=mysql_config.get('host', _DEFAULTS['mysql']['host']),
            port=mysql_config.get('port', _DEFAULTS['mysql']['port']),
            user=mysql_config.get('user', _DEFAULTS['mysql']['user']),
            password=mysql_config.get('password', _DEFAULTS['mysql']['password']),
            database=mysql_config.get('database', _DEFAULTS['mysql']['database']),
            charset=mysql_config.get('charset', _DEFAULTS['mysql']['charset']),
        )

    @property
    def url(self) -> str:
        """SQLAlchemy 连接 URL"""
        return f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def pymysql_kwargs(self) -> dict:
        """pymysql 连接参数"""
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database,
            'charset': self.charset,
        }


@dataclass
class AlignConfig:
    """
    时间对齐配置

    Attributes:
        tolerance: 时间容差，如 '200ms', '1s'
        direction: 对齐方向 (backward/forward/nearest)
        lookback: 回看窗口，处理乱序写入
        chunksize: 分块读取大小
        poll_interval: 轮询间隔（秒）
    """
    tolerance: str = field(default=_DEFAULTS['align']['tolerance'])
    direction: str = field(default=_DEFAULTS['align']['direction'])
    lookback: str = field(default=_DEFAULTS['align']['lookback'])
    chunksize: int = field(default=_DEFAULTS['align']['chunksize'])
    poll_interval: float = field(default=_DEFAULTS['align']['poll_interval'])

    @classmethod
    def from_json(cls, path: Optional[str] = None) -> 'AlignConfig':
        """从 JSON 配置文件创建实例"""
        config = load_config(path)
        align_config = config.get('align', {})

        return cls(
            tolerance=align_config.get('tolerance', _DEFAULTS['align']['tolerance']),
            direction=align_config.get('direction', _DEFAULTS['align']['direction']),
            lookback=align_config.get('lookback', _DEFAULTS['align']['lookback']),
            chunksize=align_config.get('chunksize', _DEFAULTS['align']['chunksize']),
            poll_interval=align_config.get('poll_interval', _DEFAULTS['align']['poll_interval']),
        )


# 默认配置实例（优先从 JSON 加载）
DEFAULT_MYSQL_CONFIG = MySQLConfig.from_json()
DEFAULT_ALIGN_CONFIG = AlignConfig.from_json()
