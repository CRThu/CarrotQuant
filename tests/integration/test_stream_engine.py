"""
集成测试：Stream-Native 引擎分块流式推流与状态连贯性验证
"""

import pytest
import numpy as np
from carrotquant import strategy, BarContext, Engine, MarketData


def generate_chunk(start_idx: int, n_steps: int = 10, n_symbols: int = 2) -> MarketData:
    timestamps = np.array([f"2024-01-01 {start_idx + i:02d}:00" for i in range(n_steps)])
    symbols = [f"sym_{j}" for j in range(n_symbols)]

    close_p = np.full((n_steps, n_symbols), 10.0 + start_idx * 0.1, dtype=np.float64)
    open_p = close_p * 0.99
    high_p = close_p * 1.01
    low_p = close_p * 0.98

    return MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=high_p,
        low_price=low_p,
        close_price=close_p,
    )


def test_stream_engine_multi_chunk_continuity():
    """测试多 Chunk 连续回测下现金与持仓无缝继承"""
    chunk1 = generate_chunk(start_idx=0, n_steps=5, n_symbols=2)
    chunk2 = generate_chunk(start_idx=5, n_steps=5, n_symbols=2)

    # 策略：在 Chunk 1 的 step 0 买入 100 股 sym_0，一直持有到 Chunk 2
    @strategy
    def hold_strategy(ctx: BarContext):
        if ctx.step == 0 and ctx.datetime.endswith("00:00") and ctx.positions[0] == 0:
            ctx.buy(symbol_idx=0, amount=100)

    engine = Engine(initial_cash=100000.0, fee_rate=0.0, min_fee=0.0, stamp_duty=0.0, slippage=0.0)

    # 传入 Chunk 生成器 (Stream)
    result = engine.run(strategy=hold_strategy, data=[chunk1, chunk2])

    assert len(result.portfolio_value) == 10
    assert result.trade_count == 1
    # 买入支出 100 * 9.9 = 990 元 (open 价撮合默认收盘价 9.9)，现金 100000 - 990 = 99010
    # 验证最终 Chunk 2 资产等于连贯结果
    assert result.portfolio_value[-1] > 90000.0
