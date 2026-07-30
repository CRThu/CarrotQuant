"""
单元测试：撮合算子 execute_trade_jit 与费率/滑点计算
"""

import pytest
import numpy as np
from carrotquant.engine.matching import (
    execute_trade_jit,
    get_execution_price,
    MATCHING_MODE_OPEN,
    MATCHING_MODE_CLOSE,
    MATCHING_MODE_VWAP,
    MATCHING_MODE_TWAP,
)


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
        stock_idx=0,
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


def test_execute_trade_sell_stamp_duty():
    positions = np.array([100.0], dtype=np.float64)
    avg_costs = np.array([10.0], dtype=np.float64)
    cash_arr = np.array([5000.0], dtype=np.float64)
    trade_logs = np.zeros((10, 7), dtype=np.float64)
    trade_count = np.array([0], dtype=np.int64)

    # 卖出 100 股 20.0 元，成交额 2000.0
    # 佣金 = max(2000 * 0.0003, 5.0) = 5.0 元
    # 印花税 = 2000 * 0.0005 = 1.0 元
    # 总费用 = 6.0 元，净收回 1994.0 元
    success = execute_trade_jit(
        step_idx=1,
        stock_idx=0,
        side=-1,
        target_amount=100.0,
        raw_price=20.0,
        adj_price=20.0,
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
    assert positions[0] == 0.0
    assert cash_arr[0] == 5000.0 + 1994.0
    assert trade_logs[0, 5] == 6.0  # 总费用
