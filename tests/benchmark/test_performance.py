"""
5000 标的 TPS 性能基准测试 (Python 逐个循环 vs Python 向量化选股 vs Fast JIT)
"""

import time
import tracemalloc
import pytest
import numpy as np
from pathlib import Path

from cq.engine import strategy, BarContext, Engine, MarketData


def generate_synthetic_market_data(n_steps: int = 240, n_symbols: int = 5000) -> MarketData:
    """生成全市场 5000 标的 1 天 1m K 线 (240 步) 连续 C-Array 模拟矩阵"""
    timestamps = np.array([f"2024-01-01 {i//60:02d}:{i%60:02d}" for i in range(n_steps)])
    symbols = [f"sym_{i:04d}" for i in range(n_symbols)]

    np.random.seed(42)
    base_prices = np.random.uniform(5.0, 100.0, size=(1, n_symbols))
    deltas = np.random.randn(n_steps, n_symbols) * 0.05
    close_p = np.ascontiguousarray(base_prices + np.cumsum(deltas, axis=0), dtype=np.float64)
    open_p = np.ascontiguousarray(close_p * 0.999, dtype=np.float64)
    high_p = np.ascontiguousarray(close_p * 1.001, dtype=np.float64)
    low_p = np.ascontiguousarray(close_p * 0.998, dtype=np.float64)
    vol = np.ascontiguousarray(np.full((n_steps, n_symbols), 1000.0), dtype=np.float64)
    amt = np.ascontiguousarray(close_p * vol, dtype=np.float64)

    return MarketData(
        timestamps=timestamps,
        symbols=symbols,
        open_price=open_p,
        high_price=high_p,
        low_price=low_p,
        close_price=close_p,
        volume=vol,
        amount=amt,
    )


def test_benchmark_5000_stocks_tps():
    n_steps = 240
    n_symbols = 5000
    total_ticks = n_steps * n_symbols  # 1,200,000 Ticks
    data = generate_synthetic_market_data(n_steps=n_steps, n_symbols=n_symbols)
    engine = Engine(initial_cash=10_000_000.0)

    # 1. 逐标的 Python 循环策略 (Slow Loop)
    @strategy
    def slow_loop_strategy(ctx: BarContext):
        for i in range(100):
            if ctx.is_tradable[i] and ctx.close[i] > 10.0:
                ctx.buy(symbol_idx=i, amount=100)

    engine.run(strategy=slow_loop_strategy, data=data)  # 预热
    start_slow = time.perf_counter()
    engine.run(strategy=slow_loop_strategy, data=data)
    slow_elapsed = time.perf_counter() - start_slow
    slow_tps = total_ticks / slow_elapsed

    # 2. NumPy 向量化选股 Python 策略 (Vectorized Strategy)
    @strategy
    def vectorized_strategy(ctx: BarContext):
        # 向量化掩码筛选
        mask = ctx.is_tradable & (ctx.close > 10.0)
        selected_indices = np.where(mask)[0]
        for i in selected_indices[:100]:
            ctx.buy(symbol_idx=i, amount=100)

    engine.run(strategy=vectorized_strategy, data=data)  # 预热
    start_vec = time.perf_counter()
    engine.run(strategy=vectorized_strategy, data=data)
    vec_elapsed = time.perf_counter() - start_vec
    vec_tps = total_ticks / vec_elapsed

    # 3. 统一 JIT 矩阵模式 (engine.run)
    signals = np.zeros((n_steps, n_symbols), dtype=np.int8)
    amounts = np.zeros((n_steps, n_symbols), dtype=np.float64)
    signals[:, :100] = 1
    amounts[:, :100] = 100.0

    # 预热 JIT 编译
    engine.run(signals=signals, amounts=amounts, data=data)

    # 纯净性能测试 (无 tracemalloc 污染)
    start_fast = time.perf_counter()
    engine.run(signals=signals, amounts=amounts, data=data)
    fast_elapsed = time.perf_counter() - start_fast

    fast_tps = total_ticks / fast_elapsed
    speedup_vec = slow_elapsed / vec_elapsed if vec_elapsed > 0 else 1.0

    report_md = rf"""# CarrotQuant 5000 标的性能瓶颈与向量化优化报告 (Benchmark Report)

- **测试时间**: {time.strftime('%Y-%m-%d %H:%M:%S')}
- **测试规模**: {n_steps} 时间步 (Bars) × {n_symbols} 标的池 = {total_ticks:,} 行情数据节点

## 1. 核心性能对比 (5000 标的全量回测)
| 策略表达方式 | 物理机制 | 总耗时 (ms) | 吞吐量 (Ticks/sec) | 相对加速比 |
| :--- | :--- | :--- | :--- | :--- |
| **Fast JIT 矩阵模式 (`engine.run`)** | **100% C/LLVM 机器码** | **{fast_elapsed * 1000:.2f} ms** | **{fast_tps:,.2f} Ticks/s** | **{slow_elapsed/fast_elapsed:.1f}x 🚀** |
| **Python 向量化策略 (`np.where`)** | **NumPy SIMD 矩阵过滤** | **{vec_elapsed * 1000:.2f} ms** | **{vec_tps:,.2f} Ticks/s** | **{speedup_vec:.1f}x ⚡** |
| **Python 逐元素循环 (`for i in range`)**| CPython 解释器逐行解释 | {slow_elapsed * 1000:.2f} ms | {slow_tps:,.2f} Ticks/s | 1.0x |
"""

    report_path = Path("benchmark_report.md")
    report_path.write_text(report_md, encoding="utf-8")

    assert vec_tps > 10_000

