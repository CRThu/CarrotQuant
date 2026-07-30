"""
BarContext: 策略运行期上下文与物理防未来函数切片断言

提供当前 Bar (时间步 t) 的全市场行情快照切片、账户资金/持仓状态以及下单接口。
严格限制时间切片边界在 [:t+1]，任何读取 t+1 以上数据的行为在物理内存层面抛出 IndexError。
"""

from typing import List, Optional
import numpy as np


class BarContext:
    """
    策略交互上下文 (BarContext)
    """

    def __init__(
        self,
        step: int,
        n_stocks: int,
        timestamps: np.ndarray,
        open_mat: np.ndarray,
        high_mat: np.ndarray,
        low_mat: np.ndarray,
        close_mat: np.ndarray,
        raw_close_mat: np.ndarray,
        volume_mat: np.ndarray,
        amount_mat: np.ndarray,
        is_tradable_mat: np.ndarray,
        positions: np.ndarray,
        cash: float,
        orders_buffer: List,
    ):
        self.step = step
        self.n_stocks = n_stocks
        self._timestamps = timestamps

        self._open_mat = open_mat
        self._high_mat = high_mat
        self._low_mat = low_mat
        self._close_mat = close_mat
        self._raw_close_mat = raw_close_mat
        self._volume_mat = volume_mat
        self._amount_mat = amount_mat
        self._is_tradable_mat = is_tradable_mat

        # 物理边界切片限制 [:step+1] 严格防未来函数
        self._open_history = open_mat[: step + 1, :]
        self._high_history = high_mat[: step + 1, :]
        self._low_history = low_mat[: step + 1, :]
        self._close_history = close_mat[: step + 1, :]
        self._raw_close_history = raw_close_mat[: step + 1, :]
        self._volume_history = volume_mat[: step + 1, :]
        self._amount_history = amount_mat[: step + 1, :]
        self._is_tradable_history = is_tradable_mat[: step + 1, :]

        # 当前时间步 t 的 1D 快照切片 (N,)
        self.open = open_mat[step, :]
        self.high = high_mat[step, :]
        self.low = low_mat[step, :]
        self.close = close_mat[step, :]
        self.raw_close = raw_close_mat[step, :]
        self.volume = volume_mat[step, :]
        self.amount = amount_mat[step, :]
        self.is_tradable = is_tradable_mat[step, :]

        # 账户状态
        self.positions = positions
        self.cash = cash

        # 下单指令缓冲 [(side, stock_idx, amount), ...]
        self.orders_buffer = orders_buffer

    def update_step(self, step: int, cash: float):
        """在主循环中原地更新时间步与指针"""
        self.step = step
        self.cash = cash
        self.orders_buffer.clear()

        # 物理边界切片限制 [:step+1] 严格防未来函数
        self._open_history = self._open_mat[: step + 1, :]
        self._high_history = self._high_mat[: step + 1, :]
        self._low_history = self._low_mat[: step + 1, :]
        self._close_history = self._close_mat[: step + 1, :]
        self._raw_close_history = self._raw_close_mat[: step + 1, :]
        self._volume_history = self._volume_mat[: step + 1, :]
        self._amount_history = self._amount_mat[: step + 1, :]
        self._is_tradable_history = self._is_tradable_mat[: step + 1, :]

        self.open = self._open_mat[step, :]
        self.high = self._high_mat[step, :]
        self.low = self._low_mat[step, :]
        self.close = self._close_mat[step, :]
        self.raw_close = self._raw_close_mat[step, :]
        self.volume = self._volume_mat[step, :]
        self.amount = self._amount_mat[step, :]
        self.is_tradable = self._is_tradable_mat[step, :]

    @property
    def datetime(self) -> str:
        """当前 Bar 时间戳字符串"""
        return str(self._timestamps[self.step])

    # 单标的便捷属性 ($N=1$ 时特例支持)
    @property
    def price(self) -> float:
        """单标的快捷当前收盘价"""
        return float(self.close[0])

    def buy(self, stock_idx: int, amount: float):
        """挂买单"""
        if amount > 0:
            self.orders_buffer.append((1, stock_idx, float(amount)))

    def sell(self, stock_idx: int, amount: float):
        """挂卖单"""
        if amount > 0:
            self.orders_buffer.append((-1, stock_idx, float(amount)))

    def buy_single(self, amount: float):
        """单标的快捷买入"""
        self.buy(0, amount)

    def sell_single(self, amount: float):
        """单标的快捷卖出"""
        self.sell(0, amount)

    # 完整历史切片只读属性
    @property
    def open_history(self) -> np.ndarray:
        return self._open_history

    @property
    def close_history(self) -> np.ndarray:
        return self._close_history

    @property
    def high_history(self) -> np.ndarray:
        return self._high_history

    @property
    def low_history(self) -> np.ndarray:
        return self._low_history
