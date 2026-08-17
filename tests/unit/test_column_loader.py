"""
单元测试：ColumnDataLoader, ChunkStreamer 与 MarketDataContainer
100% 独立于外部 data/ 路径，完全使用 tmp_path 生成内存与临时测试数据。
"""

import pytest
import numpy as np
import polars as pl
from cq.engine.feed.column_loader import ColumnDataLoader, MarketDataContainer
from cq.engine.feed.chunk_streamer import ChunkStreamer


def test_market_data_container_initialization():
    timestamps = np.array(["2024-01-01", "2024-01-02"])
    symbols = ["000001.SZ", "600000.SH"]

    open_p = np.array([[10.0, 20.0], [10.5, 20.5]])
    high_p = np.array([[10.8, 21.0], [11.0, 21.5]])
    low_p = np.array([[9.9, 19.5], [10.2, 19.8]])
    close_p = np.array([[10.5, 20.2], [10.8, 21.0]])

    container = MarketDataContainer(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=high_p,
        low_price=low_p,
        close_price=close_p,
    )

    assert container.n_steps == 2
    assert container.n_stocks == 2
    assert container.open.flags.c_contiguous
    assert container.close.flags.c_contiguous
    assert np.all(container.is_tradable == True)


def test_column_loader_from_mock_parquet(tmp_path):
    # 构建 Mock Parquet 数据，完全独立于外部 data/ 目录
    df = pl.DataFrame({
        "symbol": ["000001.SZ", "000001.SZ", "600000.SH", "600000.SH"],
        "datetime": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
        "open": [10.0, 10.5, 20.0, 20.5],
        "high": [10.8, 11.0, 21.0, 21.5],
        "low": [9.9, 10.2, 19.5, 19.8],
        "close": [10.5, 10.8, 20.2, 21.0],
        "volume": [1000.0, 1200.0, 5000.0, 5500.0],
        "amount": [10500.0, 12600.0, 101000.0, 115500.0],
    })

    parquet_dir = tmp_path / "kline"
    parquet_dir.mkdir()
    df.write_parquet(parquet_dir / "data.parquet")

    container = ColumnDataLoader.load_parquet(path=parquet_dir)

    assert container.n_steps == 2
    assert container.n_stocks == 2
    assert set(container.symbols) == {"000001.SZ", "600000.SH"}
    assert container.close.shape == (2, 2)


def test_column_loader_from_mock_csv(tmp_path):
    # 构建 Mock CSV 数据与复权因子 CSV，完全独立于外部 data/ 目录
    df = pl.DataFrame({
        "symbol": ["000001.SZ", "000001.SZ", "600000.SH", "600000.SH"],
        "datetime": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
        "open": [10.0, 10.5, 20.0, 20.5],
        "high": [10.8, 11.0, 21.0, 21.5],
        "low": [9.9, 10.2, 19.5, 19.8],
        "close": [10.0, 10.0, 20.0, 20.0],
        "volume": [1000.0, 1200.0, 5000.0, 5500.0],
        "amount": [10000.0, 12000.0, 100000.0, 110000.0],
    })

    adj_df = pl.DataFrame({
        "symbol": ["000001.SZ", "000001.SZ", "600000.SH", "600000.SH"],
        "datetime": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
        "back_adj_factor": [1.1, 1.1, 1.2, 1.2],
    })

    csv_dir = tmp_path / "csv_kline"
    csv_dir.mkdir()
    df.write_csv(csv_dir / "stock.csv")

    adj_dir = tmp_path / "csv_adj"
    adj_dir.mkdir()
    adj_df.write_csv(adj_dir / "adj.csv")

    container = ColumnDataLoader.load_csv(path=csv_dir, adj_factor_path=adj_dir)

    assert container.n_steps == 2
    assert container.n_symbols == 2
    # 确认原始价格为未复权: 10.0, 20.0
    assert container.close[0, 0] == 10.0
    assert container.close[0, 1] == 20.0
    # 确认乘上复权因子后: 10.0 * 1.1 = 11.0, 20.0 * 1.2 = 24.0
    assert container.adj.close[0, 0] == 11.0
    assert container.adj.close[0, 1] == 24.0



def test_chunk_streamer():
    timestamps = np.array([f"2024-01-{i+1:02d}" for i in range(10)])
    symbols = ["000001.SZ"]
    prices = np.ones((10, 1))

    container = MarketDataContainer(
        timestamps=timestamps,
        symbols=symbols,
        open_price=prices,
        high_price=prices,
        low_price=prices,
        close_price=prices,
    )

    streamer = ChunkStreamer(container, chunk_size=3)
    chunks = list(streamer.iter_chunks())

    # 10 个步长，chunk_size=3，应产生 4 个 chunk (3, 3, 3, 1)
    assert len(chunks) == 4
    start_idx, end_idx, chunk = chunks[0]
    assert start_idx == 0
    assert end_idx == 3
    assert chunk.n_steps == 3
