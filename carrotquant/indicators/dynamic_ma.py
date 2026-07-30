"""
动态滑窗指标算子 (Dynamic Indicators)

基于 Numba JIT 极速递推移动平均线 (MA) 算子与自定义指标基类。
"""

import numpy as np
from numba import njit


class BaseDynamicIndicator:
    """
    动态递推指标基类
    """

    def update(self, new_val: float) -> float:
        raise NotImplementedError


@njit(fastmath=True, nogil=True)
def calc_sma_step_jit(
    prices: np.ndarray,
    step: int,
    stock_idx: int,
    window: int,
) -> float:
    """
    JIT 单步递推计算第 step 步 stock_idx 标的的移动平均线 (SMA)

    Args:
        prices: (T, N) 价格矩阵
        step: 当前时间步 t
        stock_idx: 股票编号 i
        window: 滑动窗口长度

    Returns:
        float: 计算所得 SMA 值，数据不足时返回 NaN
    """
    if step + 1 < window:
        return np.nan

    start_idx = step + 1 - window
    sum_val = 0.0
    valid_cnt = 0

    for i in range(start_idx, step + 1):
        p = prices[i, stock_idx]
        if not np.isnan(p):
            sum_val += p
            valid_cnt += 1

    if valid_cnt == window:
        return sum_val / window
    return np.nan


@njit(fastmath=True, nogil=True)
def calc_ema_step_jit(
    prev_ema: float,
    current_price: float,
    alpha: float,
) -> float:
    """
    JIT 步进式指数移动平均 (EMA) 递推算子
    """
    if np.isnan(current_price):
        return prev_ema
    if np.isnan(prev_ema):
        return current_price
    return alpha * current_price + (1.0 - alpha) * prev_ema
