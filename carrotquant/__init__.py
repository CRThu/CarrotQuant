"""
CarrotQuant: 高性能 A 股全市场 Numba 事件驱动量化回测引擎
"""

__version__ = "0.1.0"

from carrotquant.strategy.base import strategy
from carrotquant.strategy.context import BarContext
from carrotquant.engine.runner import Engine

__all__ = ["strategy", "BarContext", "Engine"]
