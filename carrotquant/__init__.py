"""
CarrotQuant - High-performance, full-stack quantitative trading, data pipeline and backtesting framework.

Umbrella package providing:
- carrotquant-engine -> cq.engine
- carrotquant-data   -> cq.data
"""

__version__ = "1.1.0"

# 结构化暴露子命名空间模块，遵循清晰的分层调用语义
try:
    import cq.engine as engine
except (ImportError, AttributeError):
    engine = None

try:
    import cq.data as data
except (ImportError, AttributeError):
    data = None

__all__ = [
    "__version__",
    "engine",
    "data",
]
