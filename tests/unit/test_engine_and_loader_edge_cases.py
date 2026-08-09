"""
单元测试：引擎、加载器与结果分析的边界条件与异常处理
"""

import pytest
import numpy as np
import polars as pl

from carrotquant import Engine, MarketData, MatchingMode, ColumnDataLoader
from carrotquant.analytics.post_process import BacktestResult
from carrotquant.strategy.context import BarContext


def test_engine_run_invalid_arguments():
    engine = Engine()
    with pytest.raises(ValueError, match="Must provide data"):
        engine.run(strategy=lambda ctx: None, data=None)

    with pytest.raises(ValueError, match="无法解析的撮合模式"):
        Engine(matching_mode="INVALID_MODE")


def test_backtest_result_zero_trades():
    """测试无交易笔数时的 BacktestResult summary 与 metrics 边界"""
    pv = np.array([100000.0, 100000.0, 100000.0])
    cash_h = np.array([100000.0, 100000.0, 100000.0])
    timestamps = np.array(["2024-01-01", "2024-01-02", "2024-01-03"])
    symbols = ["AAPL"]
    trade_logs_mat = np.zeros((0, 7), dtype=np.float64)

    result = BacktestResult(
        trade_logs_mat=trade_logs_mat,
        trade_count=0,
        portfolio_value=pv,
        cash_history=cash_h,
        timestamps=timestamps,
        symbols=symbols,
        initial_cash=100000.0,
    )

    metrics = result.calc_metrics()
    assert metrics["total_trades"] == 0
    assert metrics["total_return"] == 0.0
    assert metrics["sharpe_ratio"] == 0.0
    assert metrics["max_drawdown"] == 0.0

    summary_str = result.summary()
    assert "总交易笔数 (Total Trades):   0" in summary_str


def test_scan_parquet_chunks_by_month(tmp_path):
    """测试 scan_parquet_chunks 按月 partition_by="month" 边界"""
    df = pl.DataFrame({
        "symbol": ["AAPL", "AAPL", "AAPL"],
        "datetime": ["2024-01-15 09:30", "2024-02-15 09:30", "2024-03-15 09:30"],
        "open": [150.0, 155.0, 160.0],
        "high": [152.0, 157.0, 162.0],
        "low": [149.0, 154.0, 159.0],
        "close": [151.0, 156.0, 161.0],
        "volume": [1000.0, 1000.0, 1000.0],
        "amount": [151000.0, 156000.0, 161000.0],
    })

    parquet_dir = tmp_path / "month_kline"
    parquet_dir.mkdir()
    df.write_parquet(parquet_dir / "data.parquet")

    chunks = list(ColumnDataLoader.scan_parquet_chunks(path=parquet_dir, partition_by="month"))
    assert len(chunks) == 3
    assert chunks[0].n_steps == 1
    assert chunks[1].n_steps == 1
    assert chunks[2].n_steps == 1


def test_column_loader_empty_filter_raises_error(tmp_path):
    """测试指定不存在的 symbol 筛选引发 ValueError"""
    df = pl.DataFrame({
        "symbol": ["AAPL"],
        "datetime": ["2024-01-01"],
        "open": [100.0],
        "high": [105.0],
        "low": [95.0],
        "close": [102.0],
        "volume": [1000.0],
        "amount": [102000.0],
    })

    p_dir = tmp_path / "empty_test"
    p_dir.mkdir()
    df.write_parquet(p_dir / "data.parquet")

    with pytest.raises(ValueError, match="No parquet data found"):
        ColumnDataLoader.load_parquet(path=p_dir, symbols=["NON_EXISTENT"])
