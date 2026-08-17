"""
单元测试：动态懒复权 (Lazy Adj) 行为与全属性覆盖测试
验证在不提供复权因子时零额外内存开销，以及提供复权因子时动态乘法计算与物理切片的准确性。
"""

import numpy as np
import pytest
from cq.engine import MarketData, BarContext, strategy, Engine


def test_market_data_without_adj_factor_defaults_to_raw():
    """测试当无复权因子时，data.adj.close 零开销返回 raw_close"""
    timestamps = np.array(["2024-01-01", "2024-01-02"])
    symbols = ["000001.SZ"]
    raw_close = np.array([[10.0], [12.0]])
    open_p = np.array([[9.5], [11.5]])
    high_p = np.array([[10.5], [12.5]])
    low_p = np.array([[9.0], [11.0]])

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=high_p,
        low_price=low_p,
        close_price=raw_close,
    )

    assert data.adj_factor is None
    np.testing.assert_array_equal(data.adj.close, raw_close)
    np.testing.assert_array_equal(data.adj.open, open_p)
    np.testing.assert_array_equal(data.adj.high, high_p)
    np.testing.assert_array_equal(data.adj.low, low_p)


def test_market_data_with_adj_factor_dynamic_evaluation():
    """测试提供 adj_factor 时，data.adj 视图动态乘法求值"""
    timestamps = np.array(["2024-01-01", "2024-01-02"])
    symbols = ["000001.SZ", "000002.SZ"]
    raw_close = np.array([[10.0, 20.0], [12.0, 22.0]])
    open_p = np.array([[9.0, 19.0], [11.0, 21.0]])
    high_p = np.array([[10.5, 20.5], [12.5, 22.5]])
    low_p = np.array([[8.5, 18.5], [10.5, 20.5]])
    adj_factor = np.array([[1.5, 2.0], [1.5, 2.0]])

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=high_p,
        low_price=low_p,
        close_price=raw_close,
        adj_factor=adj_factor,
    )

    expected_adj_close = raw_close * adj_factor
    expected_adj_open = open_p * adj_factor
    expected_adj_high = high_p * adj_factor
    expected_adj_low = low_p * adj_factor

    np.testing.assert_array_equal(data.adj.close, expected_adj_close)
    np.testing.assert_array_equal(data.adj.open, expected_adj_open)
    np.testing.assert_array_equal(data.adj.high, expected_adj_high)
    np.testing.assert_array_equal(data.adj.low, expected_adj_low)


def test_bar_context_lazy_adj_all_properties_and_histories():
    """测试 BarContext 中 ctx.adj 与 ctx 的全量属性及历史切片"""
    timestamps = np.array(["2024-01-01", "2024-01-02", "2024-01-03"])
    symbols = ["000001.SZ"]
    raw_close = np.array([[10.0], [12.0], [14.0]])
    open_p = np.array([[9.5], [11.5], [13.5]])
    high_p = np.array([[10.5], [12.5], [14.5]])
    low_p = np.array([[9.0], [11.0], [13.0]])
    adj_factor = np.array([[1.1], [1.2], [1.3]])

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=high_p,
        low_price=low_p,
        close_price=raw_close,
        adj_factor=adj_factor,
    )

    @strategy
    def test_all_props_strat(ctx: BarContext):
        t = ctx.step
        # 当前步快捷方式
        assert ctx.datetime == timestamps[t]
        assert ctx.price == float(raw_close[t, 0])

        # 买卖快捷调取
        if t == 0:
            ctx.buy_single(100)
        elif t == 1:
            ctx.sell_single(50)

        # 动态复权切片
        np.testing.assert_allclose(ctx.adj.open, open_p[t, :] * adj_factor[t, :])
        np.testing.assert_allclose(ctx.adj.high, high_p[t, :] * adj_factor[t, :])
        np.testing.assert_allclose(ctx.adj.low, low_p[t, :] * adj_factor[t, :])
        np.testing.assert_allclose(ctx.adj.close, raw_close[t, :] * adj_factor[t, :])

        # 动态复权历史切片
        np.testing.assert_allclose(ctx.adj.open_history, open_p[: t + 1, :] * adj_factor[: t + 1, :])
        np.testing.assert_allclose(ctx.adj.high_history, high_p[: t + 1, :] * adj_factor[: t + 1, :])
        np.testing.assert_allclose(ctx.adj.low_history, low_p[: t + 1, :] * adj_factor[: t + 1, :])
        np.testing.assert_allclose(ctx.adj.close_history, raw_close[: t + 1, :] * adj_factor[: t + 1, :])

    engine = Engine(initial_cash=100000.0)
    res = engine.run(strategy=test_all_props_strat, data=data)
    assert res is not None


def test_bar_context_without_adj_factor_histories():
    """测试无复权因子时 BarContext 中 ctx.adj 与未复权历史一致"""
    timestamps = np.array(["2024-01-01", "2024-01-02"])
    symbols = ["000001.SZ"]
    raw_close = np.array([[10.0], [12.0]])
    open_p = np.array([[9.5], [11.5]])

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=raw_close,
        low_price=open_p,
        close_price=raw_close,
    )

    @strategy
    def no_adj_strat(ctx: BarContext):
        np.testing.assert_allclose(ctx.adj.open, ctx.open)
        np.testing.assert_allclose(ctx.adj.high, ctx.high)
        np.testing.assert_allclose(ctx.adj.low, ctx.low)
        np.testing.assert_allclose(ctx.adj.close, ctx.close)
        np.testing.assert_allclose(ctx.adj.open_history, ctx.open_history)
        np.testing.assert_allclose(ctx.adj.high_history, ctx.high_history)
        np.testing.assert_allclose(ctx.adj.low_history, ctx.low_history)
        np.testing.assert_allclose(ctx.adj.close_history, ctx.close_history)

    engine = Engine(initial_cash=100000.0)
    engine.run(strategy=no_adj_strat, data=data)
