"""
端到端集成测试：使用自定义列 (factor_b) 驱动策略交易全流程
"""

import numpy as np
import pytest
from cq.engine import MarketData, BarContext, strategy, Engine


def test_end_to_end_backtest_with_custom_factor():
    """测试基于自定义列 factor_b 的交易信号产生与全链路交易撮合"""
    timestamps = np.array(["2024-01-01", "2024-01-02", "2024-01-03"])
    symbols = ["000001.SZ", "000002.SZ"]

    open_p = np.array([
        [10.0, 20.0],
        [10.5, 20.5],
        [11.0, 21.0],
    ])
    close_p = np.array([
        [10.0, 20.0],
        [10.5, 20.5],
        [11.0, 21.0],
    ])
    volume_p = np.array([
        [1000.0, 1000.0],
        [1000.0, 1000.0],
        [1000.0, 1000.0],
    ])

    # 自定义因子 factor_b: 000001.SZ 在 t=0 为看多(0.9)，000002.SZ 在 t=1 为看多(0.85)
    factor_b = np.array([
        [0.90, 0.10],
        [0.10, 0.85],
        [-0.5, -0.5],
    ])

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=close_p,
        low_price=open_p,
        close_price=close_p,
        volume=volume_p,
        custom_fields={"factor_b": factor_b},
    )

    @strategy
    def b_factor_strategy(ctx: BarContext):
        # 获取当前时间步的因子 b 快照 (2,)
        b_val = ctx.get("factor_b")

        for i in range(ctx.n_symbols):
            if b_val[i] > 0.8:
                ctx.buy(symbol_idx=i, amount=100)

    engine = Engine(initial_cash=100000.0, fee_rate=0.0, min_fee=0.0, slippage=0.0)
    res = engine.run(strategy=b_factor_strategy, data=data)

    assert res is not None
    assert len(res.trade_logs) == 2  # 应在 t=0 产生 1 笔买单，t=1 产生 1 笔买单

    # 验证第一笔买入 000001.SZ (symbol="000001.SZ")
    trade_0 = res.trade_logs[0]
    assert trade_0["symbol"][0] == "000001.SZ"
    assert trade_0["amount"][0] == 100.0

    # 验证第二笔买入 000002.SZ (symbol="000002.SZ")
    trade_1 = res.trade_logs[1]
    assert trade_1["symbol"][0] == "000002.SZ"
    assert trade_1["amount"][0] == 100.0
