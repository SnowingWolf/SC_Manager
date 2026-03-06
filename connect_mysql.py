"""
向后兼容模块

此模块提供 MySQLReader 类作为 SCReader 的别名，以保持向后兼容性。
新代码应该直接使用 sc_reader.SCReader。

Examples:
    >>> # 旧的用法（仍然支持）
    >>> from connect_mysql import MySQLReader
    >>> reader = MySQLReader(host='10.11.50.141', user='read', password='111111')

    >>> # 推荐的新用法
    >>> from sc_reader import SCReader
    >>> reader = SCReader(host='10.11.50.141', user='read', password='111111')
"""

from sc_reader import SCReader

# 向后兼容：MySQLReader 是 SCReader 的别名
MySQLReader = SCReader

__all__ = ['MySQLReader']

