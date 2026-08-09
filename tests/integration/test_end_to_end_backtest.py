"""
端到端测试：全流程行情装载、多空策略回测与 Polars 绩效报告导出
"""

import pytest
import numpy as np
from carrotquant import strategy, BarContext, Engine, MarketData


def test_end_to_end_long_short_backtest():
    n_steps = 50
    n_symbols = 5
    timestamps = np.array([f"2024-01-01 {i:02d}:00" for i in range(n_steps)])
    symbols = [f"stock_{i}" for i in range(n_symbols)]

    # 构造确定性价格序列：前 25 步上涨，后 25 步下跌
    base_p = 10.0
    close_p = np.zeros((n_steps, n_symbols), dtype=np.float64)
    for t in range(n_steps):
        delta = 0.2 if t < 25 else -0.2
        base_p += delta
        close_p[t, :] = base_p

    open_p = close_p * 0.99
    high_p = close_p * 1.01
    low_p = close_p * 0.98

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=high_p,
        low_price=low_p,
        close_price=close_p,
    )

    # 策略：前 20 步做多 stock_0，第 25 步反手做空 stock_0
    @strategy
    def trend_strategy(ctx: BarContext):
        if ctx.step == 5 and ctx.positions[0] == 0:
            ctx.buy(symbol_idx=0, amount=100)  # 做多 100 股
        elif ctx.step == 25 and ctx.positions[0] > 0:
            ctx.sell(symbol_idx=0, amount=200)  # 卖出 200 股 (平多 100 + 开空 100)

    engine = Engine(initial_cash=100000.0, fee_rate=0.0001, min_fee=0.0, stamp_duty=0.0, slippage=0.0)
    result = engine.run(strategy=trend_strategy, data=data)

    assert result.trade_count == 2
    assert len(result.portfolio_value) == 50

    # 验证 Polars 交易日志
    trade_df = result.trade_logs
    assert trade_df.height == 2
    assert "symbol" in trade_df.columns
    assert "cash_after" in trade_df.columns

    # 验证 summary 文本报告
    summary_text = result.summary()
    assert "回测绩效报告" in summary_text
    assert "夏普比率" in summary_text
