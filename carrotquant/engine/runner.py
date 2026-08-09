"""
Engine: 回测引擎主入口与 Bar 循环调度器

连接行情数据、SoA 状态数组、撮合核算子与策略逻辑。
提供统一的 Stream-Native 回测调度入口 engine.run()。
"""

from typing import Callable, Iterable, List, Optional, Union
import numpy as np
from numba import njit

from carrotquant.data.column_loader import MarketData
from carrotquant.engine.state import EngineState
from carrotquant.engine.matching import (
    execute_trade_jit,
    get_execution_price,
    MatchingMode,
    MATCHING_MODE_CLOSE,
    MATCHING_MODE_OPEN,
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
    max_volume_ratio: float = 1.0,
    adj_open_mat: np.ndarray = None,
):
    """
    全 JIT 内核化主 Bar 循环 (Full JIT Loop Kernel)
    """
    n_steps, n_symbols = open_mat.shape
    adj_open = adj_open_mat if adj_open_mat is not None else open_mat

    for t in range(n_steps):
        # 1. 扫描与撮合
        if matching_mode == MATCHING_MODE_OPEN:
            # OPEN 撮合模式：t 步在开盘按 open_mat[t, i] 撮合 t-1 步产生的信号 (防止偷看当 Bar Close 买当 Bar Open)
            if t > 0:
                for i in range(n_symbols):
                    sig = signals_mat[t - 1, i]
                    if sig != 0 and is_tradable_mat[t, i]:
                        amt = amounts_mat[t - 1, i]
                        if amt > 0:
                            raw_p = open_mat[t, i]
                            adj_p = adj_open[t, i]
                            vol_val = volume_mat[t, i] if volume_mat is not None else 0.0

                            execute_trade_jit(
                                step_idx=t,
                                symbol_idx=i,
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
                                volume=vol_val,
                                max_volume_ratio=max_volume_ratio,
                            )
        else:
            # 当 Bar 撮合模式 (CLOSE / VWAP / TWAP)
            for i in range(n_symbols):
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
                            adj_open[t, i],
                            high_mat[t, i],
                            low_mat[t, i],
                            close_mat[t, i],
                            volume_mat[t, i],
                            amount_mat[t, i],
                        )

                        vol_val = volume_mat[t, i] if volume_mat is not None else 0.0

                        execute_trade_jit(
                            step_idx=t,
                            symbol_idx=i,
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
                            volume=vol_val,
                            max_volume_ratio=max_volume_ratio,
                        )


        # 2. 极速计算当前 Bar 的账户净资产 (PV = Cash + sum(pos * close))
        current_cash = cash_arr[0]
        cash_history[t] = current_cash

        pos_val = 0.0
        for i in range(n_symbols):
            if positions[i] != 0.0:
                pos_val += positions[i] * close_mat[t, i]

        portfolio_value[t] = current_cash + pos_val


class Engine:
    """
    CarrotQuant 通用事件驱动与向量化回测引擎主控制器
    """

    def __init__(
        self,
        initial_cash: float = 1_000_000.0,
        fee_rate: float = 0.0003,      # 佣金率
        min_fee: float = 5.0,          # 最小佣金 5 元
        stamp_duty: float = 0.0005,    # 卖出印花税
        slippage: float = 0.0001,      # 交易滑点
        max_volume_ratio: float = 1.0, # 盘口最大成交量比例 (如 0.1 表示单笔最多成交当前 Bar 10% 流动性)
        matching_mode: Union[int, str, MatchingMode] = MATCHING_MODE_CLOSE,  # 支持字符串或 Enum
        max_trades: int = 1_000_000,
    ):
        self.initial_cash = initial_cash
        self.fee_rate = fee_rate
        self.min_fee = min_fee
        self.stamp_duty = stamp_duty
        self.slippage = slippage
        self.max_volume_ratio = max_volume_ratio
        self.matching_mode = MatchingMode.parse(matching_mode)
        self.max_trades = max_trades

    def run(
        self,
        strategy: Optional[Callable[[BarContext], None]] = None,
        signals: Optional[np.ndarray] = None,
        amounts: Optional[np.ndarray] = None,
        data: Union[MarketData, Iterable[MarketData]] = None,
    ) -> BacktestResult:
        """
        统一回测运行入口 (Unified Stream-Native Engine Run API)

        自动支持:
          1. Python 回调策略: engine.run(strategy=my_strat, data=data)
          2. Fast Vectorized 模式: engine.run(signals=signals, amounts=amounts, data=data)
          3. 磁盘分块流式模式: engine.run(strategy=my_strat, data=scan_chunks(...))
        """
        if data is None:
            raise ValueError("Must provide data or data stream generator to engine.run()")

        # 归一化为 Chunks 流
        if isinstance(data, MarketData):
            chunk_stream = [data]
        else:
            chunk_stream = data

        all_portfolio_values = []
        all_cash_histories = []
        all_timestamps = []
        all_trade_logs = []
        symbols = None

        current_cash = self.initial_cash
        positions = None
        avg_costs = None

        for chunk in chunk_stream:
            if symbols is None:
                symbols = chunk.symbols
                positions = np.zeros(chunk.n_symbols, dtype=np.float64)
                avg_costs = np.zeros(chunk.n_symbols, dtype=np.float64)

            state = EngineState(
                n_steps=chunk.n_steps,
                n_symbols=chunk.n_symbols,
                initial_cash=current_cash,
                max_trades=self.max_trades,
            )
            # 继承上一个 Chunk 的持仓与开仓成本
            state.positions[:] = positions
            state.avg_costs[:] = avg_costs
            cash_arr = np.array([current_cash], dtype=np.float64)

            if signals is not None and amounts is not None:
                # 极速向量模式
                sig_mat = np.ascontiguousarray(signals, dtype=np.int8)
                amt_mat = np.ascontiguousarray(amounts, dtype=np.float64)

                adj_open_view = chunk.adj.open if hasattr(chunk, "adj") else chunk.open

                run_engine_jit_kernel(
                    open_mat=chunk.open,
                    high_mat=chunk.high,
                    low_mat=chunk.low,
                    close_mat=chunk.close,
                    raw_close_mat=chunk.raw_close,
                    volume_mat=chunk.volume,
                    amount_mat=chunk.amount,
                    is_tradable_mat=chunk.is_tradable,
                    signals_mat=sig_mat,
                    amounts_mat=amt_mat,
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
                    max_volume_ratio=self.max_volume_ratio,
                    adj_open_mat=adj_open_view,
                )


            elif strategy is not None:
                # Python 回调策略模式
                orders_buffer = []
                pending_orders = []

                # 确定使用的收盘价矩阵 (优先使用复权视图计算策略信号)
                close_view = chunk.adj.close if hasattr(chunk, "adj") else chunk.close
                open_view = chunk.adj.open if hasattr(chunk, "adj") else chunk.open
                high_view = chunk.adj.high if hasattr(chunk, "adj") else chunk.high
                low_view = chunk.adj.low if hasattr(chunk, "adj") else chunk.low

                ctx = BarContext(
                    step=0,
                    n_symbols=chunk.n_symbols,
                    timestamps=chunk.timestamps,
                    open_mat=open_view,
                    high_mat=high_view,
                    low_mat=low_view,
                    close_mat=close_view,
                    raw_close_mat=chunk.raw_close,
                    adj_close_mat=close_view,
                    adj_open_mat=open_view,
                    adj_high_mat=high_view,
                    adj_low_mat=low_view,
                    volume_mat=chunk.volume,
                    amount_mat=chunk.amount,
                    is_tradable_mat=chunk.is_tradable,
                    positions=state.positions,
                    cash=cash_arr[0],
                    orders_buffer=orders_buffer,
                )

                for t in range(chunk.n_steps):
                    # 1. 如果是 OPEN 撮合模式，优先在 t 步开盘撮合 t-1 步产生的挂单 (彻底消除未来函数隐患)
                    if self.matching_mode == MATCHING_MODE_OPEN and len(pending_orders) > 0:
                        for side, sym_idx, target_amount in pending_orders:
                            raw_p = chunk.open[t, sym_idx]
                            adj_p = open_view[t, sym_idx]
                            vol_val = chunk.volume[t, sym_idx] if chunk.volume is not None else 0.0

                            execute_trade_jit(
                                step_idx=t,
                                symbol_idx=sym_idx,
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
                                volume=vol_val,
                                max_volume_ratio=self.max_volume_ratio,
                            )
                        pending_orders.clear()

                    # 2. 运行当前 Bar 的策略逻辑
                    ctx.update_step(t, cash_arr[0])
                    strategy(ctx)

                    # 3. 处理当前 Bar 产生的订单
                    if len(orders_buffer) > 0:
                        if self.matching_mode == MATCHING_MODE_OPEN:
                            # OPEN 模式下挂单延迟至 t+1 步开盘撮合
                            pending_orders.extend(orders_buffer)
                        else:
                            # CLOSE / VWAP / TWAP 模式下当 Bar 实时撮合
                            for side, sym_idx, target_amount in orders_buffer:
                                raw_p = get_execution_price(
                                    self.matching_mode,
                                    chunk.open[t, sym_idx],
                                    chunk.high[t, sym_idx],
                                    chunk.low[t, sym_idx],
                                    chunk.raw_close[t, sym_idx],
                                    chunk.volume[t, sym_idx],
                                    chunk.amount[t, sym_idx],
                                )
                                adj_p = get_execution_price(
                                    self.matching_mode,
                                    open_view[t, sym_idx],
                                    high_view[t, sym_idx],
                                    low_view[t, sym_idx],
                                    close_view[t, sym_idx],
                                    chunk.volume[t, sym_idx],
                                    chunk.amount[t, sym_idx],
                                )

                                vol_val = chunk.volume[t, sym_idx] if chunk.volume is not None else 0.0

                                execute_trade_jit(
                                    step_idx=t,
                                    symbol_idx=sym_idx,
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
                                    volume=vol_val,
                                    max_volume_ratio=self.max_volume_ratio,
                                )
                        orders_buffer.clear()

                    state.cash = cash_arr[0]
                    state.cash_history[t] = state.cash

                    pos_val = 0.0
                    for i in range(chunk.n_symbols):
                        if state.positions[i] != 0.0:
                            curr_price = chunk.close[t, i]
                            if np.isnan(curr_price) or curr_price <= 0:
                                curr_price = state.avg_costs[i]
                            pos_val += state.positions[i] * curr_price

                    state.portfolio_value[t] = state.cash + pos_val

            # 收集该 Chunk 的运行记录
            all_portfolio_values.append(state.portfolio_value)
            all_cash_histories.append(state.cash_history)
            all_timestamps.append(chunk.timestamps)

            t_count = int(state.trade_count[0])
            if t_count > 0:
                all_trade_logs.append(state.trade_logs[:t_count].copy())

            # 提取期末状态为下一个 Chunk 继承
            current_cash = float(cash_arr[0])
            positions[:] = state.positions[:]
            avg_costs[:] = state.avg_costs[:]

        # 合并多 Chunk 结果
        full_pv = np.concatenate(all_portfolio_values) if all_portfolio_values else np.array([])
        full_cash = np.concatenate(all_cash_histories) if all_cash_histories else np.array([])
        full_timestamps = np.concatenate(all_timestamps) if all_timestamps else np.array([])

        if all_trade_logs:
            full_trade_logs = np.vstack(all_trade_logs)
            full_trade_count = len(full_trade_logs)
        else:
            full_trade_logs = np.zeros((0, 7), dtype=np.float64)
            full_trade_count = 0

        return BacktestResult(
            trade_logs_mat=full_trade_logs,
            trade_count=full_trade_count,
            portfolio_value=full_pv,
            cash_history=full_cash,
            timestamps=full_timestamps,
            symbols=symbols if symbols is not None else [],
            initial_cash=self.initial_cash,
        )

    def run_fast(
        self,
        signals: np.ndarray,
        amounts: np.ndarray,
        data: Union[MarketData, Iterable[MarketData]],
    ) -> BacktestResult:
        """
        向后兼容的向量化快捷运行方法
        """
        return self.run(signals=signals, amounts=amounts, data=data)

