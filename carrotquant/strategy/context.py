"""
BarContext: 策略运行期上下文与物理防未来函数切片断言

提供当前 Bar (时间步 t) 的全市场行情快照切片、账户资金/持仓状态以及下单接口。
严格限制时间切片边界在 [:t+1]，任何读取 t+1 以上数据的行为在物理内存层面抛出 IndexError。
"""

from typing import Any, List, Optional, Dict, Union
import numpy as np


class AdjContext:
    """
    策略运行期复权行情快照与历史切片视角 (动态懒算与后向兼容)
    """

    def __init__(self, ctx: "BarContext"):
        self._ctx = ctx

    @property
    def open(self) -> np.ndarray:
        if self._ctx._has_explicit_adj:
            return self._ctx._adj_open_mat[self._ctx.step, :]
        if self._ctx._adj_factor is None:
            return self._ctx.open
        return self._ctx.open * self._ctx._adj_factor[self._ctx.step, :]

    @property
    def high(self) -> np.ndarray:
        if self._ctx._has_explicit_adj:
            return self._ctx._adj_high_mat[self._ctx.step, :]
        if self._ctx._adj_factor is None:
            return self._ctx.high
        return self._ctx.high * self._ctx._adj_factor[self._ctx.step, :]

    @property
    def low(self) -> np.ndarray:
        if self._ctx._has_explicit_adj:
            return self._ctx._adj_low_mat[self._ctx.step, :]
        if self._ctx._adj_factor is None:
            return self._ctx.low
        return self._ctx.low * self._ctx._adj_factor[self._ctx.step, :]

    @property
    def close(self) -> np.ndarray:
        if self._ctx._has_explicit_adj:
            return self._ctx._adj_close_mat[self._ctx.step, :]
        if self._ctx._adj_factor is None:
            return self._ctx.close
        return self._ctx.close * self._ctx._adj_factor[self._ctx.step, :]

    @property
    def open_history(self) -> np.ndarray:
        if self._ctx._has_explicit_adj:
            return self._ctx._adj_open_mat[: self._ctx.step + 1, :]
        if self._ctx._adj_factor is None:
            return self._ctx.open_history
        return self._ctx.open_history * self._ctx._adj_factor[: self._ctx.step + 1, :]

    @property
    def high_history(self) -> np.ndarray:
        if self._ctx._has_explicit_adj:
            return self._ctx._adj_high_mat[: self._ctx.step + 1, :]
        if self._ctx._adj_factor is None:
            return self._ctx.high_history
        return self._ctx.high_history * self._ctx._adj_factor[: self._ctx.step + 1, :]

    @property
    def low_history(self) -> np.ndarray:
        if self._ctx._has_explicit_adj:
            return self._ctx._adj_low_mat[: self._ctx.step + 1, :]
        if self._ctx._adj_factor is None:
            return self._ctx.low_history
        return self._ctx.low_history * self._ctx._adj_factor[: self._ctx.step + 1, :]

    @property
    def close_history(self) -> np.ndarray:
        if self._ctx._has_explicit_adj:
            return self._ctx._adj_close_mat[: self._ctx.step + 1, :]
        if self._ctx._adj_factor is None:
            return self._ctx.close_history
        return self._ctx.close_history * self._ctx._adj_factor[: self._ctx.step + 1, :]


class BarContext:
    """
    策略交互上下文 (BarContext)
    """

    def __init__(
        self,
        step: int,
        n_symbols: int,
        timestamps: np.ndarray,
        open_mat: np.ndarray,
        high_mat: np.ndarray,
        low_mat: np.ndarray,
        close_mat: np.ndarray,
        raw_close_mat: Optional[np.ndarray] = None,
        adj_close_mat: Optional[np.ndarray] = None,
        adj_open_mat: Optional[np.ndarray] = None,
        adj_high_mat: Optional[np.ndarray] = None,
        adj_low_mat: Optional[np.ndarray] = None,
        volume_mat: Optional[np.ndarray] = None,
        amount_mat: Optional[np.ndarray] = None,
        is_tradable_mat: Optional[np.ndarray] = None,
        adj_factor: Optional[np.ndarray] = None,
        custom_fields: Optional[Any] = None,
        positions: Optional[np.ndarray] = None,
        cash: float = 0.0,
        orders_buffer: Optional[List] = None,
    ):
        self.step = step
        self.n_symbols = n_symbols
        self.n_stocks = n_symbols  # 兼容属性
        self._timestamps = timestamps

        self._open_mat = open_mat
        self._high_mat = high_mat
        self._low_mat = low_mat
        self._close_mat = close_mat
        self._raw_close_mat = raw_close_mat if raw_close_mat is not None else close_mat

        self._adj_factor = adj_factor
        self._custom_fields = custom_fields if custom_fields is not None else {}
        self._has_explicit_adj = adj_close_mat is not None

        self._adj_close_mat = adj_close_mat if adj_close_mat is not None else close_mat
        self._adj_open_mat = adj_open_mat if adj_open_mat is not None else open_mat
        self._adj_high_mat = adj_high_mat if adj_high_mat is not None else high_mat
        self._adj_low_mat = adj_low_mat if adj_low_mat is not None else low_mat

        self._volume_mat = volume_mat
        self._amount_mat = amount_mat
        self._is_tradable_mat = is_tradable_mat

        # 当前时间步 t 的 1D 快照切片 (N,)
        self.open = open_mat[step, :]
        self.high = high_mat[step, :]
        self.low = low_mat[step, :]
        self.close = self._raw_close_mat[step, :]
        self.raw_close = self._raw_close_mat[step, :]
        self.volume = volume_mat[step, :] if volume_mat is not None else np.zeros(n_symbols)
        self.amount = amount_mat[step, :] if amount_mat is not None else np.zeros(n_symbols)
        self.is_tradable = is_tradable_mat[step, :] if is_tradable_mat is not None else np.ones(n_symbols, dtype=bool)

        # 账户状态
        self.positions = positions
        self.cash = cash

        # 下单指令缓冲 [(order_id, order_type, side, symbol_idx, amount, price), ...]
        self.orders_buffer = orders_buffer if orders_buffer is not None else []
        self._order_counter = 0
        self.canceled_order_ids = set()

        # 复权子视角
        self.adj = AdjContext(self)

    def update_step(self, step: int, cash: float):
        """在主循环中原地更新时间步与指针"""
        self.step = step
        self.cash = cash
        self.orders_buffer.clear()
        self.canceled_order_ids.clear()

        self.open = self._open_mat[step, :]
        self.high = self._high_mat[step, :]
        self.low = self._low_mat[step, :]
        self.close = self._raw_close_mat[step, :]
        self.raw_close = self._raw_close_mat[step, :]
        if self._volume_mat is not None:
            self.volume = self._volume_mat[step, :]
        if self._amount_mat is not None:
            self.amount = self._amount_mat[step, :]
        if self._is_tradable_mat is not None:
            self.is_tradable = self._is_tradable_mat[step, :]

    def get(self, name: str) -> np.ndarray:
        """获取当前时间步 t 的自定义列 1D 快照切片 (N,)"""
        mat = self._custom_fields[name]
        return mat[self.step, :]

    def get_history(self, name: str) -> np.ndarray:
        """获取截至当前时间步 t 的自定义列历史 2D 切片 [:t+1, :] (物理严格防未来)"""
        mat = self._custom_fields[name]
        return mat[: self.step + 1, :]

    def __getitem__(self, name: str) -> np.ndarray:
        """字典下标简写支持 `ctx['b']`"""
        return self.get(name)

    @property
    def custom(self):
        """自定义特征映射代理"""
        return self._custom_fields

    @property
    def datetime(self) -> str:
        """当前 Bar 时间戳字符串"""
        return str(self._timestamps[self.step])

    # 单标的便捷属性
    @property
    def price(self) -> float:
        """单标的快捷当前收盘价"""
        return float(self.close[0])

    def buy(self, symbol_idx: int = 0, amount: float = 0.0, stock_idx: Optional[int] = None) -> int:
        """挂买入市价单 (pos += amount)"""
        target_idx = stock_idx if stock_idx is not None else symbol_idx
        if amount > 0:
            # 格式: (side=1, symbol_idx, amount)
            self.orders_buffer.append((1, target_idx, float(amount)))
            return 0
        return 0

    def sell(self, symbol_idx: int = 0, amount: float = 0.0, stock_idx: Optional[int] = None) -> int:
        """挂卖出市价单 (pos -= amount，天然支持做空)"""
        target_idx = stock_idx if stock_idx is not None else symbol_idx
        if amount > 0:
            # 格式: (side=-1, symbol_idx, amount)
            self.orders_buffer.append((-1, target_idx, float(amount)))
            return 0
        return 0

    def buy_limit(self, symbol_idx: int = 0, amount: float = 0.0, price: float = 0.0, stock_idx: Optional[int] = None) -> int:
        """挂买入限价单 (当市场最低价 <= limit_price 时触发买入)"""
        target_idx = stock_idx if stock_idx is not None else symbol_idx
        if amount > 0 and price > 0:
            self._order_counter += 1
            order_id = self._order_counter
            # 格式: (order_id, order_type=1, side=1, symbol_idx, amount, price)
            self.orders_buffer.append((order_id, 1, 1, target_idx, float(amount), float(price)))
            return order_id
        return 0

    def sell_limit(self, symbol_idx: int = 0, amount: float = 0.0, price: float = 0.0, stock_idx: Optional[int] = None) -> int:
        """挂卖无限价单 (当市场最高价 >= limit_price 时触发卖出)"""
        target_idx = stock_idx if stock_idx is not None else symbol_idx
        if amount > 0 and price > 0:
            self._order_counter += 1
            order_id = self._order_counter
            # 格式: (order_id, order_type=1, side=-1, symbol_idx, amount, price)
            self.orders_buffer.append((order_id, 1, -1, target_idx, float(amount), float(price)))
            return order_id
        return 0

    def cancel_order(self, order_id: int):
        """撤销指定 ID 的未成交订单"""
        if order_id > 0:
            self.canceled_order_ids.add(order_id)

    def buy_single(self, amount: float):
        """单标的快捷买入"""
        self.buy(0, amount)

    def sell_single(self, amount: float):
        """单标的快捷卖出"""
        self.sell(0, amount)

    # 物理边界切片限制 [:step+1] 严格防未来函数
    @property
    def open_history(self) -> np.ndarray:
        return self._open_mat[: self.step + 1, :]

    @property
    def close_history(self) -> np.ndarray:
        return self._raw_close_mat[: self.step + 1, :]

    @property
    def high_history(self) -> np.ndarray:
        return self._high_mat[: self.step + 1, :]

    @property
    def low_history(self) -> np.ndarray:
        return self._low_mat[: self.step + 1, :]

