"""
CarrotQuant: 高性能全市场 Numba 事件驱动量化回测引擎
"""

__version__ = "1.0.1"

from carrotquant.strategy.base import strategy
from carrotquant.strategy.context import BarContext
from carrotquant.engine.runner import Engine
from carrotquant.engine.matching import MatchingMode
from carrotquant.data.column_loader import MarketData, MarketDataContainer, ColumnDataLoader

__all__ = [
    "strategy",
    "BarContext",
    "Engine",
    "MatchingMode",
    "MarketData",
    "MarketDataContainer",
    "ColumnDataLoader",
]

