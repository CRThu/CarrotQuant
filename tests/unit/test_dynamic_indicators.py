"""
单元测试：动态滑窗指标 calc_sma_step_jit / calc_ema_step_jit
"""

import pytest
import numpy as np
from carrotquant.indicators.dynamic_ma import calc_sma_step_jit, calc_ema_step_jit


def test_calc_sma_step_jit():
    # 模拟 5 个时间步的价格矩阵 (5, 1)
    prices = np.array([[10.0], [12.0], [14.0], [16.0], [18.0]], dtype=np.float64)

    # 步长不足窗口长度 3 时返回 NaN
    assert np.isnan(calc_sma_step_jit(prices, step=0, symbol_idx=0, window=3))
    assert np.isnan(calc_sma_step_jit(prices, step=1, symbol_idx=0, window=3))

    # step=2 (价格: 10, 12, 14) -> SMA = 12.0
    sma2 = calc_sma_step_jit(prices, step=2, symbol_idx=0, window=3)
    assert sma2 == 12.0

    # step=4 (价格: 14, 16, 18) -> SMA = 16.0
    sma4 = calc_sma_step_jit(prices, step=4, symbol_idx=0, window=3)
    assert sma4 == 16.0



def test_calc_ema_step_jit():
    prev_ema = 10.0
    curr_price = 12.0
    alpha = 0.5  # 50% 权重

    new_ema = calc_ema_step_jit(prev_ema, curr_price, alpha)
    assert new_ema == 11.0
