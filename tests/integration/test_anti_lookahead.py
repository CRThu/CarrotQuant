"""
防未来函数 Chaos 混沌注入验证测试 (Anti Look-Ahead Chaos Verification)

在回测数据集后半段 (t+1 ... T) 注入极端价格与成交量噪声，
断言当前步 (0 ... t) 的交易决策与未注入噪声前 100% 字节级完全一致。
"""

import pytest
import numpy as np
from carrotquant import strategy, BarContext, Engine
from carrotquant.data.column_loader import MarketDataContainer


def test_anti_lookahead_chaos_injection():
    # 1. 构建干净的标准数据集
    np.random.seed(12345)
    n_steps = 100
    n_stocks = 3
    timestamps = np.array([f"2024-01-{i+1:02d}" for i in range(n_steps)])
    symbols = ["000001.SZ", "600000.SH", "300750.SZ"]

    clean_close = 10.0 + np.cumsum(np.random.randn(n_steps, n_stocks) * 0.2, axis=0)
    clean_open = clean_close * 0.99
    clean_high = clean_close * 1.02
    clean_low = clean_close * 0.98
    clean_vol = np.full((n_steps, n_stocks), 1000.0)
    clean_amt = clean_close * clean_vol

    clean_data = MarketDataContainer(
        timestamps=timestamps,
        symbols=symbols,
        open_price=clean_open,
        high_price=clean_high,
        low_price=clean_low,
        close_price=clean_close,
        volume=clean_vol,
        amount=clean_amt,
    )

    # 2. 定义双均线交叉策略
    @strategy
    def ma_cross_strategy(ctx: BarContext):
        if ctx.step < 10:
            return

        for i in range(ctx.n_stocks):
            # 获取严格截止到当前步 t 的历史切片
            c_hist = ctx.close_history[:, i]
            ma5 = np.mean(c_hist[-5:])
            ma10 = np.mean(c_hist[-10:])

            if ma5 > ma10 and ctx.positions[i] == 0:
                ctx.buy(i, 100)
            elif ma5 < ma10 and ctx.positions[i] > 0:
                ctx.sell(i, ctx.positions[i])

    # 运行标准回测
    engine = Engine(initial_cash=100_000.0)
    clean_results = engine.run(strategy=ma_cross_strategy, data=clean_data)

    # 3. 构造注入 Chaos 极端噪声的数据集 (在后 50% 步的数据中注入 1000 倍随机波幅噪声)
    noisy_close = clean_close.copy()
    noisy_close[50:, :] += np.random.randn(50, n_stocks) * 1000.0
    noisy_open = noisy_close * 0.99
    noisy_high = noisy_close * 1.05
    noisy_low = noisy_close * 0.95
    noisy_vol = clean_vol.copy()
    noisy_amt = noisy_close * noisy_vol

    noisy_data = MarketDataContainer(
        timestamps=timestamps,
        symbols=symbols,
        open_price=noisy_open,
        high_price=noisy_high,
        low_price=noisy_low,
        close_price=noisy_close,
        volume=noisy_vol,
        amount=noisy_amt,
    )

    # 在后半段步长到达之前（即 t <= 49），两者的交易决策必须 100% 字节级一致
    # 我们可以通过拦截 t=49 前的交易日志断言完全相同
    clean_logs_before_50 = clean_results.trade_logs.filter(pl_col_step_less_than_50(clean_results.trade_logs))
    
    noisy_results = engine.run(strategy=ma_cross_strategy, data=noisy_data)
    noisy_logs_before_50 = noisy_results.trade_logs.filter(pl_col_step_less_than_50(noisy_results.trade_logs))

    # 断言 t <= 49 的交易笔数与具体日志完全相等
    assert len(clean_logs_before_50) == len(noisy_logs_before_50)
    if len(clean_logs_before_50) > 0:
        for c1, c2 in zip(clean_logs_before_50.rows(), noisy_logs_before_50.rows()):
            assert c1 == c2


def pl_col_step_less_than_50(df):
    import polars as pl
    return pl.col("step_idx") < 50
