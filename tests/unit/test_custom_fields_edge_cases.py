"""
边界与异常测试：自定义列/特征的边界情况验证
"""

import numpy as np
import pytest
from cq.engine import MarketData, BarContext, strategy, Engine
from cq.engine.feed.column_loader import LazyCustomFields


def test_custom_fields_non_existent_column_raises_keyerror():
    """测试读取不存在的自定义列名抛出 KeyError"""
    timestamps = np.array(["2024-01-01"])
    symbols = ["SYM0"]
    open_p = np.array([[10.0]])
    close_p = np.array([[10.0]])

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=close_p,
        low_price=close_p,
        close_price=close_p,
        custom_fields={"factor_a": np.array([[0.5]])},
    )

    # 存在 factor_a
    np.testing.assert_array_equal(data["factor_a"], np.array([[0.5]]))

    # 不存在 factor_non_exist
    with pytest.raises(KeyError):
        _ = data["factor_non_exist"]


def test_custom_fields_with_nan_values():
    """测试自定义列中包含 NaN 值时的正确获取与切片"""
    timestamps = np.array(["2024-01-01", "2024-01-02"])
    symbols = ["SYM0", "SYM1"]
    close_p = np.ones((2, 2))
    factor_mat = np.array([[np.nan, 1.2], [3.4, np.nan]])

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=close_p,
        high_price=close_p,
        low_price=close_p,
        close_price=close_p,
        custom_fields={"factor_nan": factor_mat},
    )

    assert np.isnan(data["factor_nan"][0, 0])
    assert data["factor_nan"][0, 1] == 1.2

    @strategy
    def nan_strat(ctx: BarContext):
        val = ctx.get("factor_nan")
        if ctx.step == 0:
            assert np.isnan(val[0])
            assert val[1] == 1.2

    engine = Engine(initial_cash=10000.0)
    engine.run(strategy=nan_strat, data=data)


def test_custom_fields_single_symbol_single_timestamp():
    """测试单标的 (N=1) 与单时间步 (T=1) 的极值维度切片"""
    timestamps = np.array(["2024-01-01"])
    symbols = ["SYM0"]
    close_p = np.array([[100.0]])
    factor_mat = np.array([[42.0]])

    data = MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=close_p,
        high_price=close_p,
        low_price=close_p,
        close_price=close_p,
        custom_fields={"single_val": factor_mat},
    )

    @strategy
    def single_strat(ctx: BarContext):
        snapshot = ctx.get("single_val")
        history = ctx.get_history("single_val")

        assert snapshot.shape == (1,)
        assert history.shape == (1, 1)
        assert snapshot[0] == 42.0
        assert history[0, 0] == 42.0

    engine = Engine(initial_cash=10000.0)
    engine.run(strategy=single_strat, data=data)
