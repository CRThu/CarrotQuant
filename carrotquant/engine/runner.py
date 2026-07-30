"""
Engine: 回测引擎主入口与 Bar 循环调度器

连接行情数据、SoA 状态数组、撮合核算子与策略逻辑。
提供纯 JIT 全速运行模式 (Fast JIT Mode) 与 Python 灵活调度模式。
"""

from typing import Callable, Optional, Union
import numpy as np
from numba import njit

from carrotquant.data.column_loader import MarketDataContainer
from carrotquant.engine.state import EngineState
from carrotquant.engine.matching import (
    execute_trade_jit,
    get_execution_price,
    MATCHING_MODE_CLOSE,
)
from carrotquant.strategy.context import BarContext
from carrotquant.analytics.post_process import BacktestResult


@njit(fastmath=True, nogil=True)
def run_engine_jit_kernel(
    open_mat: np.ndarray,
    high_mat: np.ndarray,
    low_mat: np.ndarray,
    close_mat: np.ndarray,
    raw_close_mat: np.ndarray,
    volume_mat: np.ndarray,
    amount_mat: np.ndarray,
    is_tradable_mat: np.ndarray,
    signals_mat: np.ndarray,  # 预生成的信号矩阵 (T, N) : 1=Buy, -1=Sell, 0=Hold
    amounts_mat: np.ndarray,  # 下单数量矩阵 (T, N)
    matching_mode: int,
    fee_rate: float,
    min_fee: float,
    stamp_duty: float,
    slippage: float,
    positions: np.ndarray,
    avg_costs: np.ndarray,
    cash_arr: np.ndarray,
    portfolio_value: np.ndarray,
    cash_history: np.ndarray,
    trade_logs: np.ndarray,
    trade_count: np.ndarray,
):
    """
    全 JIT 内核化主 Bar 循环 (Full JIT Loop Kernel)

    整个循环 100% 编译为 LLVM 机器码，在底层 CPU 缓存与流水线中极速推流运行，
    零 Python 对象创建，零 CPython <-> C 外嵌桥接开销。
    """
    n_steps, n_stocks = open_mat.shape

    for t in range(n_steps):
        # 1. 扫描当前时间步 t 的信号矩阵 (T, N)
        for i in range(n_stocks):
            sig = signals_mat[t, i]
            if sig != 0 and is_tradable_mat[t, i]:
                amt = amounts_mat[t, i]
                if amt > 0:
                    raw_p = get_execution_price(
                        matching_mode,
                        open_mat[t, i],
                        high_mat[t, i],
                        low_mat[t, i],
                        raw_close_mat[t, i],
                        volume_mat[t, i],
                        amount_mat[t, i],
                    )
                    adj_p = get_execution_price(
                        matching_mode,
                        open_mat[t, i],
                        high_mat[t, i],
                        low_mat[t, i],
                        close_mat[t, i],
                        volume_mat[t, i],
                        amount_mat[t, i],
                    )

                    execute_trade_jit(
                        step_idx=t,
                        stock_idx=i,
                        side=int(sig),
                        target_amount=amt,
                        raw_price=raw_p,
                        adj_price=adj_p,
                        fee_rate=fee_rate,
                        min_fee=min_fee,
                        stamp_duty=stamp_duty,
                        slippage=slippage,
                        positions=positions,
                        avg_costs=avg_costs,
                        cash_arr=cash_arr,
                        trade_logs=trade_logs,
                        trade_count=trade_count,
                    )

        # 2. 极速计算当前 Bar 的账户资产
        current_cash = cash_arr[0]
        cash_history[t] = current_cash

        pos_val = 0.0
        for i in range(n_stocks):
            if positions[i] > 0.0:
                p = close_mat[t, i]
                if np.isnan(p) or p <= 0.0:
                    p = avg_costs[i]
                pos_val += positions[i] * p

        portfolio_value[t] = current_cash + pos_val


class Engine:
    """
    CarrotQuant 事件驱动回测引擎主控制器
    """

    def __init__(
        self,
        initial_cash: float = 1_000_000.0,
        fee_rate: float = 0.0003,      # 双边佣金率 (万三)
        min_fee: float = 5.0,          # 最小佣金 5 元
        stamp_duty: float = 0.0005,    # 卖出印花税 (千0.5)
        slippage: float = 0.0001,      # 交易滑点 (万一)
        matching_mode: int = MATCHING_MODE_CLOSE,  # 默认按收盘价撮合
        max_trades: int = 1_000_000,
    ):
        self.initial_cash = initial_cash
        self.fee_rate = fee_rate
        self.min_fee = min_fee
        self.stamp_duty = stamp_duty
        self.slippage = slippage
        self.matching_mode = matching_mode
        self.max_trades = max_trades

    def run(
        self,
        strategy: Callable[[BarContext], None],
        data: MarketDataContainer,
    ) -> BacktestResult:
        """
        运行事件驱动回测 (支持 Python 策略回调)
        """
        n_steps, n_stocks = data.shape
        state = EngineState(
            n_steps=n_steps,
            n_stocks=n_stocks,
            initial_cash=self.initial_cash,
            max_trades=self.max_trades,
        )

        cash_arr = np.array([state.cash], dtype=np.float64)
        orders_buffer = []

        # 仅创建 1 次 BarContext 对象 (对象池复用)
        ctx = BarContext(
            step=0,
            n_stocks=n_stocks,
            timestamps=data.timestamps,
            open_mat=data.open,
            high_mat=data.high,
            low_mat=data.low,
            close_mat=data.close,
            raw_close_mat=data.raw_close,
            volume_mat=data.volume,
            amount_mat=data.amount,
            is_tradable_mat=data.is_tradable,
            positions=state.positions,
            cash=cash_arr[0],
            orders_buffer=orders_buffer,
        )

        # 主 Bar 循环 (零 Python 对象创建开销)
        for t in range(n_steps):
            ctx.update_step(t, cash_arr[0])

            strategy(ctx)

            for side, stock_idx, target_amount in orders_buffer:
                raw_p = get_execution_price(
                    self.matching_mode,
                    data.open[t, stock_idx],
                    data.high[t, stock_idx],
                    data.low[t, stock_idx],
                    data.raw_close[t, stock_idx],
                    data.volume[t, stock_idx],
                    data.amount[t, stock_idx],
                )
                adj_p = get_execution_price(
                    self.matching_mode,
                    data.open[t, stock_idx],
                    data.high[t, stock_idx],
                    data.low[t, stock_idx],
                    data.close[t, stock_idx],
                    data.volume[t, stock_idx],
                    data.amount[t, stock_idx],
                )

                execute_trade_jit(
                    step_idx=t,
                    stock_idx=stock_idx,
                    side=side,
                    target_amount=target_amount,
                    raw_price=raw_p,
                    adj_price=adj_p,
                    fee_rate=self.fee_rate,
                    min_fee=self.min_fee,
                    stamp_duty=self.stamp_duty,
                    slippage=self.slippage,
                    positions=state.positions,
                    avg_costs=state.avg_costs,
                    cash_arr=cash_arr,
                    trade_logs=state.trade_logs,
                    trade_count=state.trade_count,
                )

            state.cash = cash_arr[0]
            state.cash_history[t] = state.cash

            pos_val = 0.0
            for i in range(n_stocks):
                if state.positions[i] > 0:
                    curr_price = data.close[t, i]
                    if np.isnan(curr_price) or curr_price <= 0:
                        curr_price = state.avg_costs[i]
                    pos_val += state.positions[i] * curr_price

            state.portfolio_value[t] = state.cash + pos_val

        return BacktestResult(
            trade_logs_mat=state.trade_logs,
            trade_count=int(state.trade_count[0]),
            portfolio_value=state.portfolio_value,
            cash_history=state.cash_history,
            timestamps=data.timestamps,
            symbols=data.symbols,
            initial_cash=self.initial_cash,
        )

    def run_fast(
        self,
        signals: np.ndarray,
        amounts: np.ndarray,
        data: MarketDataContainer,
    ) -> BacktestResult:
        """
        全 JIT 内核化全速模式 (Fast JIT Mode)

        传入预生成的信号矩阵 signals (T, N) 和 amounts (T, N)，
        整个 Bar 循环与撮合在 Numba JIT C-Engine 内部一通到底运行，提供极限吞吐吞吐速度。
        """
        n_steps, n_stocks = data.shape
        state = EngineState(
            n_steps=n_steps,
            n_stocks=n_stocks,
            initial_cash=self.initial_cash,
            max_trades=self.max_trades,
        )

        cash_arr = np.array([state.cash], dtype=np.float64)

        signals_mat = np.ascontiguousarray(signals, dtype=np.int8)
        amounts_mat = np.ascontiguousarray(amounts, dtype=np.float64)

        run_engine_jit_kernel(
            open_mat=data.open,
            high_mat=data.high,
            low_mat=data.low,
            close_mat=data.close,
            raw_close_mat=data.raw_close,
            volume_mat=data.volume,
            amount_mat=data.amount,
            is_tradable_mat=data.is_tradable,
            signals_mat=signals_mat,
            amounts_mat=amounts_mat,
            matching_mode=self.matching_mode,
            fee_rate=self.fee_rate,
            min_fee=self.min_fee,
            stamp_duty=self.stamp_duty,
            slippage=self.slippage,
            positions=state.positions,
            avg_costs=state.avg_costs,
            cash_arr=cash_arr,
            portfolio_value=state.portfolio_value,
            cash_history=state.cash_history,
            trade_logs=state.trade_logs,
            trade_count=state.trade_count,
        )

        return BacktestResult(
            trade_logs_mat=state.trade_logs,
            trade_count=int(state.trade_count[0]),
            portfolio_value=state.portfolio_value,
            cash_history=state.cash_history,
            timestamps=data.timestamps,
            symbols=data.symbols,
            initial_cash=self.initial_cash,
        )
