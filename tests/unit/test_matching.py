"""
单元测试：撮合算子 execute_trade_jit 与费率/滑点/多空机制计算
"""

import pytest
import numpy as np
from cq.engine.matching import (
    execute_trade_jit,
    get_execution_price,
    MatchingMode,
    MATCHING_MODE_OPEN,
    MATCHING_MODE_CLOSE,
    MATCHING_MODE_VWAP,
    MATCHING_MODE_TWAP,
)


def test_matching_mode_enum_and_parse():
    assert MatchingMode.parse("close") == MATCHING_MODE_CLOSE
    assert MatchingMode.parse("OPEN") == MATCHING_MODE_OPEN
    assert MatchingMode.parse("vwap") == MATCHING_MODE_VWAP
    assert MatchingMode.parse("TWAP") == MATCHING_MODE_TWAP
    assert MatchingMode.parse(MATCHING_MODE_CLOSE) == MATCHING_MODE_CLOSE


def test_execution_price_modes():
    open_p, high_p, low_p, close_p = 10.0, 12.0, 9.0, 11.0
    vol, amt = 1000.0, 10500.0  # VWAP = 10.5

    assert get_execution_price(MATCHING_MODE_OPEN, open_p, high_p, low_p, close_p, vol, amt) == 10.0
    assert get_execution_price(MATCHING_MODE_CLOSE, open_p, high_p, low_p, close_p, vol, amt) == 11.0
    assert get_execution_price(MATCHING_MODE_VWAP, open_p, high_p, low_p, close_p, vol, amt) == 10.5
    assert get_execution_price(MATCHING_MODE_TWAP, open_p, high_p, low_p, close_p, vol, amt) == (high_p + low_p + close_p) / 3.0


def test_execute_trade_buy_with_min_fee():
    positions = np.zeros(1, dtype=np.float64)
    avg_costs = np.zeros(1, dtype=np.float64)
    cash_arr = np.array([10000.0], dtype=np.float64)
    trade_logs = np.zeros((10, 7), dtype=np.float64)
    trade_count = np.array([0], dtype=np.int64)

    # 买入 100 股 10.0 元，原成交额 1000.0，按万3佣金为 0.3 元，但受最小 5 元限制，总支出为 1005.0
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

    assert success is True
    assert positions[0] == 100.0
    assert cash_arr[0] == 10000.0 - 1005.0
    assert trade_count[0] == 1
    assert trade_logs[0, 5] == 5.0  # 手续费为 5.0


def test_execute_trade_sell_short_and_cover():
    """测试天然支持做空与买入平空"""
    positions = np.zeros(1, dtype=np.float64)
    avg_costs = np.zeros(1, dtype=np.float64)
    cash_arr = np.array([10000.0], dtype=np.float64)
    trade_logs = np.zeros((10, 7), dtype=np.float64)
    trade_count = np.array([0], dtype=np.int64)

    # 1. 卖出 100 股开空，价格 100 元 (免除最小佣金以简化计算)
    success_short = execute_trade_jit(
        step_idx=0,
        symbol_idx=0,
        side=-1,
        target_amount=100.0,
        raw_price=100.0,
        adj_price=100.0,
        fee_rate=0.0,
        min_fee=0.0,
        stamp_duty=0.0,
        slippage=0.0,
        positions=positions,
        avg_costs=avg_costs,
        cash_arr=cash_arr,
        trade_logs=trade_logs,
        trade_count=trade_count,
    )

    assert success_short is True
    assert positions[0] == -100.0  # 持仓变为 -100 (做空)
    assert cash_arr[0] == 10000.0 + 10000.0  # 增加卖出所得 10,000 元

    # 2. 价格下跌到 90 元买入 100 股平空
    success_cover = execute_trade_jit(
        step_idx=1,
        symbol_idx=0,
        side=1,
        target_amount=100.0,
        raw_price=90.0,
        adj_price=90.0,
        fee_rate=0.0,
        min_fee=0.0,
        stamp_duty=0.0,
        slippage=0.0,
        positions=positions,
        avg_costs=avg_costs,
        cash_arr=cash_arr,
        trade_logs=trade_logs,
        trade_count=trade_count,
    )

    assert success_cover is True
    assert positions[0] == 0.0  # 持仓回归为 0
    assert cash_arr[0] == 20000.0 - 9000.0  # 支出 9,000 元买入，净盈利 1,000 元 (最终 Cash = 11,000)


def test_execute_trade_max_volume_ratio_limit():
    """测试 max_volume_ratio 成交量比例限制限制交易上限"""
    positions = np.zeros(1, dtype=np.float64)
    avg_costs = np.zeros(1, dtype=np.float64)
    cash_arr = np.array([100000.0], dtype=np.float64)
    trade_logs = np.zeros((10, 7), dtype=np.float64)
    trade_count = np.array([0], dtype=np.int64)

    # 试图买入 1000 股，但当前 Bar 总成交量为 500 股，且 max_volume_ratio = 0.1 (最多只能买 50 股)
    success = execute_trade_jit(
        step_idx=0,
        symbol_idx=0,
        side=1,
        target_amount=1000.0,
        raw_price=10.0,
        adj_price=10.0,
        fee_rate=0.0,
        min_fee=0.0,
        stamp_duty=0.0,
        slippage=0.0,
        positions=positions,
        avg_costs=avg_costs,
        cash_arr=cash_arr,
        trade_logs=trade_logs,
        trade_count=trade_count,
        volume=500.0,
        max_volume_ratio=0.1,
    )

    assert success is True
    assert positions[0] == 50.0  # 最终成交量被裁减限制为 50 股 (500 * 0.1)
    assert trade_logs[0, 3] == 50.0


def test_vectorized_engine_max_volume_ratio_limit():
    """测试 JIT 向量模式下 Engine(max_volume_ratio=0.2) 限制撮合交易量上限"""
    from cq.engine import Engine, MarketData

    timestamps = np.array(["2024-01-01"])
    symbols = ["SYM0"]
    open_p = np.array([[10.0]])
    high_p = np.array([[10.0]])
    low_p = np.array([[10.0]])
    close_p = np.array([[10.0]])
    vol = np.array([[200.0]])  # 成交量为 200

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=high_p,
        low_price=low_p,
        close_price=close_p,
        volume=vol,
    )

    signals = np.array([[1]], dtype=np.int8)
    amounts = np.array([[1000.0]], dtype=np.float64)  # 下单 1000

    engine = Engine(initial_cash=100000.0, max_volume_ratio=0.2)  # 最多成交 200 * 0.2 = 40 股
    res = engine.run(signals=signals, amounts=amounts, data=data)

    assert res.trade_count == 1
    assert res.trade_logs[0, "amount"] == 40.0


def test_open_matching_mode_delayed_execution():
    """测试 matching_mode='open' 时 t 步决策在 t+1 步开盘价撮合 (彻底无未来函数偷看)"""
    from cq.engine import Engine, MarketData, strategy, BarContext

    timestamps = np.array(["2024-01-01", "2024-01-02"])
    symbols = ["SYM0"]
    open_p = np.array([[10.0], [12.0]])   # t=0 open=10.0, t=1 open=12.0
    close_p = np.array([[10.0], [12.0]])

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=close_p,
        low_price=close_p,
        close_price=close_p,
    )

    @strategy
    def buy_at_step0(ctx: BarContext):
        if ctx.step == 0:
            ctx.buy(symbol_idx=0, amount=100)

    engine = Engine(initial_cash=100000.0, matching_mode="open", fee_rate=0.0, min_fee=0.0, slippage=0.0)
    res = engine.run(strategy=buy_at_step0, data=data)

    assert res.trade_count == 1
    # 应该在 step_idx=1 (第二天) 开盘按 12.0 精确成交
    assert res.trade_logs[0, "step_idx"] == 1
    assert res.trade_logs[0, "price"] == 12.0





