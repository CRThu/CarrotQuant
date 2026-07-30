"""
市价/限价撮合引擎内核算子 (@njit)

提供纯 JIT 编译的带费率、印花税、滑点与多种撮合价格模式的订单撮合逻辑。
"""

import numpy as np
from numba import njit

# 撮合价格模式常量
MATCHING_MODE_OPEN = 0
MATCHING_MODE_CLOSE = 1
MATCHING_MODE_VWAP = 2
MATCHING_MODE_TWAP = 3


@njit(fastmath=True, nogil=True)
def get_execution_price(
    mode: int,
    open_p: float,
    high_p: float,
    low_p: float,
    close_p: float,
    volume: float,
    amount: float,
) -> float:
    """根据指定的撮合价格模式计算执行价格"""
    if mode == MATCHING_MODE_OPEN:
        return open_p
    elif mode == MATCHING_MODE_CLOSE:
        return close_p
    elif mode == MATCHING_MODE_VWAP:
        if volume > 0.0:
            return amount / volume
        return close_p
    elif mode == MATCHING_MODE_TWAP:
        return (high_p + low_p + close_p) / 3.0
    else:
        return close_p


@njit(fastmath=True, nogil=True)
def execute_trade_jit(
    step_idx: int,
    stock_idx: int,
    side: int,  # 1 为买入, -1 为卖出
    target_amount: float,
    raw_price: float,
    adj_price: float,
    fee_rate: float,
    min_fee: float,
    stamp_duty: float,
    slippage: float,
    positions: np.ndarray,
    avg_costs: np.ndarray,
    cash_arr: np.ndarray,  # 长度为 1 的 1D 数组方便原地更新 cash
    trade_logs: np.ndarray,
    trade_count: np.ndarray,
) -> bool:
    """
    JIT 极速撮合单笔交易。

    Returns:
        bool: 是否成功成交
    """
    if target_amount <= 0.0 or np.isnan(raw_price) or raw_price <= 0.0:
        return False

    current_cash = cash_arr[0]
    curr_pos = positions[stock_idx]

    # 计算考虑滑点后的真实价格
    if side == 1:  # 买入
        exec_raw_price = raw_price * (1.0 + slippage)
        exec_adj_price = adj_price * (1.0 + slippage)
    else:  # 卖出
        exec_raw_price = raw_price * (1.0 - slippage)
        exec_adj_price = adj_price * (1.0 - slippage)

    if side == 1:  # 买入逻辑
        raw_trade_value = target_amount * exec_raw_price
        comm = max(raw_trade_value * fee_rate, min_fee) if fee_rate > 0 else 0.0
        total_cost = raw_trade_value + comm

        # 校验现金是否充足，若不足则调整买入数量
        if total_cost > current_cash:
            if current_cash <= min_fee:
                return False
            # 重新反算可买数量
            target_amount = (current_cash - min_fee) / (exec_raw_price * (1.0 + fee_rate))
            if target_amount <= 0.0:
                return False
            raw_trade_value = target_amount * exec_raw_price
            comm = max(raw_trade_value * fee_rate, min_fee)
            total_cost = raw_trade_value + comm

        # 更新持仓与现金
        new_pos = curr_pos + target_amount
        if new_pos > 0:
            avg_costs[stock_idx] = (curr_pos * avg_costs[stock_idx] + target_amount * exec_adj_price) / new_pos
        positions[stock_idx] = new_pos
        cash_arr[0] -= total_cost
        paid_fee = comm

    else:  # 卖出逻辑 (side == -1)
        # 卖出数量不得超过当前持仓
        actual_sell_amount = min(target_amount, curr_pos)
        if actual_sell_amount <= 0.0:
            return False

        raw_trade_value = actual_sell_amount * exec_raw_price
        comm = max(raw_trade_value * fee_rate, min_fee) if fee_rate > 0 else 0.0
        duty = raw_trade_value * stamp_duty
        total_fee = comm + duty

        net_proceeds = raw_trade_value - total_fee
        positions[stock_idx] -= actual_sell_amount
        if positions[stock_idx] <= 1e-8:
            positions[stock_idx] = 0.0
            avg_costs[stock_idx] = 0.0

        cash_arr[0] += net_proceeds
        target_amount = actual_sell_amount
        paid_fee = total_fee

    # 写入预分配交易日志
    idx = trade_count[0]
    if idx < trade_logs.shape[0]:
        trade_logs[idx, 0] = float(step_idx)
        trade_logs[idx, 1] = float(stock_idx)
        trade_logs[idx, 2] = float(side)
        trade_logs[idx, 3] = target_amount
        trade_logs[idx, 4] = exec_adj_price
        trade_logs[idx, 5] = paid_fee
        trade_logs[idx, 6] = cash_arr[0]
        trade_count[0] += 1

    return True
