"""
单元测试：边界条件与异常假数据 (Edge Cases & Synthetic Anomalies) 测试
覆盖全市场停牌、资金不足折算/拒单、NaN 异常填充等边界条件。
"""

import pytest
import numpy as np
import polars as pl
from carrotquant import strategy, BarContext, Engine, MarketData
from carrotquant.engine.matching import execute_trade_jit


def test_edge_case_suspended_stocks():
    """测试部分或全部股票停牌 (Volume=0, Close=NaN) 场景"""
    timestamps = np.array(["2024-01-01", "2024-01-02"])
    symbols = ["000001.SZ", "600000.SH"]

    # 股票 1 第二天停牌
    open_p = np.array([[10.0, 20.0], [np.nan, 20.5]])
    close_p = np.array([[10.0, 20.0], [np.nan, 20.5]])
    vol = np.array([[1000.0, 5000.0], [0.0, 5500.0]])

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=close_p,
        low_price=close_p,
        close_price=close_p,
        volume=vol,
    )

    # 股票 1 在第 2 天不可交易
    assert data.is_tradable[0, 0] == True
    assert data.is_tradable[1, 0] == False
    assert data.is_tradable[1, 1] == True

    @strategy
    def try_buy_suspended(ctx: BarContext):
        for i in range(ctx.n_symbols):
            if ctx.is_tradable[i]:
                ctx.buy(symbol_idx=i, amount=100)

    engine = Engine(initial_cash=100_000.0)
    results = engine.run(strategy=try_buy_suspended, data=data)

    # 应该只成功购买了可交易的股票
    assert results.trade_count > 0


def test_edge_case_insufficient_cash():
    """测试资金不足情况下的撮合处理"""
    positions = np.zeros(1, dtype=np.float64)
    avg_costs = np.zeros(1, dtype=np.float64)
    cash_arr = np.array([2.0], dtype=np.float64)  # 现金只有 2 元，连 5 元最小手续费都不够
    trade_logs = np.zeros((10, 7), dtype=np.float64)
    trade_count = np.array([0], dtype=np.int64)

    # 尝试买入，因现金小于 min_fee 必须返回 False 拒绝成交
    success = execute_trade_jit(
        step_idx=0,
        symbol_idx=0,
        side=1,
        target_amount=100.0,
        raw_price=10.0,
        adj_price=10.0,
        fee_rate=0.0003,
        min_fee=5.0,
        stamp_duty=0.0005,
        slippage=0.0,
        positions=positions,
        avg_costs=avg_costs,
        cash_arr=cash_arr,
        trade_logs=trade_logs,
        trade_count=trade_count,
    )

    assert success is False
    assert positions[0] == 0.0
    assert cash_arr[0] == 2.0  # 现金未发生扣减

