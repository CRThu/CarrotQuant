"""
单元测试：Numba 动态指标算子 (calc_sma_step_jit, calc_ema_step_jit)
"""

import pytest
import numpy as np
from cq.engine.indicators.dynamic_ma import (
    calc_sma_step_jit,
    calc_ema_step_jit,
    BaseDynamicIndicator,
)


def test_base_dynamic_indicator_interface():
    base_ind = BaseDynamicIndicator()
    with pytest.raises(NotImplementedError):
        base_ind.update(10.0)


def test_calc_sma_step_jit():
    # 5 步，1 个标的
    prices = np.array([[10.0], [12.0], [14.0], [16.0], [18.0]], dtype=np.float64)

    # 窗口长度 3
    # t=0,1 数据不足 3，返回 NaN
    assert np.isnan(calc_sma_step_jit(prices, step=0, symbol_idx=0, window=3))
    assert np.isnan(calc_sma_step_jit(prices, step=1, symbol_idx=0, window=3))

    # t=2, 均值 (10 + 12 + 14) / 3 = 12.0
    sma2 = calc_sma_step_jit(prices, step=2, symbol_idx=0, window=3)
    assert pytest.approx(sma2) == 12.0

    # t=4, 均值 (14 + 16 + 18) / 3 = 16.0
    sma4 = calc_sma_step_jit(prices, step=4, symbol_idx=0, window=3)
    assert pytest.approx(sma4) == 16.0


def test_calc_sma_step_jit_with_nan():
    # 包含 NaN 的情况
    prices = np.array([[10.0], [np.nan], [14.0], [16.0]], dtype=np.float64)
    # 因为存在 NaN，无法凑满 valid_cnt == window，返回 NaN
    assert np.isnan(calc_sma_step_jit(prices, step=2, symbol_idx=0, window=3))



def test_calc_ema_step_jit():
    alpha = 0.2
    # 首次开局 prev_ema 为 NaN，直接返回 current_price
    ema1 = calc_ema_step_jit(prev_ema=np.nan, current_price=10.0, alpha=alpha)
    assert ema1 == 10.0

    # 第二步: alpha * 20.0 + (1 - alpha) * 10.0 = 0.2 * 20 + 0.8 * 10 = 12.0
    ema2 = calc_ema_step_jit(prev_ema=10.0, current_price=20.0, alpha=alpha)
    assert pytest.approx(ema2) == 12.0

    # 当前价格为 NaN 时，保持 prev_ema 不变
    ema_nan = calc_ema_step_jit(prev_ema=12.0, current_price=np.nan, alpha=alpha)
    assert ema_nan == 12.0
