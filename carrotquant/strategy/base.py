"""
@strategy 装饰器与 Strategy 基类定义

实现零开销打平内联 (Zero-Overhead Flattening) 的策略表达层。
"""

from typing import Callable, Any
import functools
from carrotquant.strategy.context import BarContext


class BaseStrategy:
    """
    策略抽象基类（可选）
    """

    def on_bar(self, ctx: BarContext):
        raise NotImplementedError


def strategy(fn: Callable[[BarContext], None]) -> Callable[[BarContext], None]:
    """
    @strategy 装饰器
    用于注册策略函数，消除类继承模板代码，方便直接传入 Engine 运行。
    """
    @functools.wraps(fn)
    def wrapper(ctx: BarContext, *args, **kwargs):
        return fn(ctx, *args, **kwargs)

    setattr(wrapper, "_is_carrot_strategy", True)
    return wrapper
