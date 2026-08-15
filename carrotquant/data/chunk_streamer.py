"""
按时间 Window 流式 Chunk 分块器 (ChunkStreamer)

用于超大规模全市场行情数据（如 1m 频段多年数据）的流式推流与 Window 分块加载。
"""

from typing import Generator, List, Optional, Tuple, Union
import numpy as np
from carrotquant.data.column_loader import MarketDataContainer


class ChunkStreamer:
    """
    行情 Chunk 流式分块推流器
    """

    def __init__(self, data: MarketDataContainer, chunk_size: int = 1000):
        """
        初始化 ChunkStreamer

        Args:
            data: 完整的 MarketDataContainer 实例
            chunk_size: 每个 Chunk 包含的时间步 Bar 数量
        """
        self.data = data
        self.chunk_size = chunk_size
        self.total_steps = data.n_steps

    def iter_chunks(self) -> Generator[Tuple[int, int, MarketDataContainer], None, None]:
        """
        按 chunk_size 产生时间片段 [start_idx, end_idx) 的 MarketDataContainer 切片
        """
        for start_idx in range(0, self.total_steps, self.chunk_size):
            end_idx = min(start_idx + self.chunk_size, self.total_steps)
            chunk_container = MarketDataContainer(
                timestamps=self.data.timestamps[start_idx:end_idx],
                symbols=self.data.symbols,
                open_price=self.data.open[start_idx:end_idx],
                high_price=self.data.high[start_idx:end_idx],
                low_price=self.data.low[start_idx:end_idx],
                close_price=self.data.close[start_idx:end_idx],
                raw_close_price=self.data.raw_close[start_idx:end_idx],
                volume=self.data.volume[start_idx:end_idx],
                amount=self.data.amount[start_idx:end_idx],
                is_tradable=self.data.is_tradable[start_idx:end_idx],
            )
            yield start_idx, end_idx, chunk_container
