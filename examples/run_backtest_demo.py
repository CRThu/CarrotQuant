"""
CarrotQuant 真实数据全流程回测 Demo 脚本

使用说明:
演示如何装载 CarrotQuant 的真实 CSV / Parquet 行情与复权因子数据，
运行 Numba 零开销 @strategy 策略，并输出回测报告与 Polars 交易日志。
"""

import sys
from pathlib import Path
import numpy as np

# 将项目根目录添加到 sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from carrotquant import strategy, BarContext, Engine
from carrotquant.data import ColumnDataLoader


def main():
    print("=" * 70)
    print(" [CarrotQuant] 1m+ 高性能 Numba 全市场事件驱动回测引擎 Demo 演示")
    print("=" * 70)

    # 1. 设置真实数据路径 (Storage Root 标准结构)
    csv_kline_path = "data/csv/ashare.kline.1d.raw.baostock"
    csv_adj_path = "data/csv/ashare.adj_factor.baostock"
    
    parquet_kline_path = "data/test_data_root/parquet/ashare.kline.1d.raw.baostock"
    parquet_adj_path = "data/test_data_root/parquet/ashare.adj_factor.baostock"

    # 判断优先使用真实 CSV 数据还是 Parquet 数据
    if Path(parquet_kline_path).exists():
        print(f" -> [Step 1] 装载真实 Parquet 行情与复权因子数据: {parquet_kline_path}")
        data = ColumnDataLoader.load_parquet(
            path=parquet_kline_path,
            adj_factor_path=parquet_adj_path,
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
    elif Path(csv_kline_path).exists():
        print(f" -> [Step 1] 装载真实 CSV 行情与复权因子数据: {csv_kline_path}")
        data = ColumnDataLoader.load_csv(
            path=csv_kline_path,
            adj_factor_path=csv_adj_path,
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
    else:
        raise FileNotFoundError("未找到真实数据目录，请检查 data/ 路径。")

    print(" [OK] 行情装载与数据对齐成功!")
    print(f"   - 时间步 (T): {data.n_steps} Bars (时间开端: {data.timestamps[0]} ~ {data.timestamps[-1]})")
    print(f"   - 股票池 (N): {data.n_stocks} 只标的")
    print(f"   - 2D 矩阵内存形态: {data.close.shape}, C-Contiguous={data.close.flags.c_contiguous}")

    # 2. 定义双均线交叉选股策略 (使用 @strategy 装饰器)
    @strategy
    def dual_ma_cross_strategy(ctx: BarContext):
        if ctx.step < 20:  # 预留 20 天计算均线
            return

        for i in range(ctx.n_stocks):
            if not ctx.is_tradable[i]:
                continue

            # 读取该标的截至当前步 t 的后复权历史收盘价
            history_close = ctx.close_history[-20:, i]
            if np.isnan(history_close).any():
                continue

            ma5 = np.mean(history_close[-5:])
            ma20 = np.mean(history_close[-20:])

            # 金叉买入 100 股
            if ma5 > ma20 and ctx.positions[i] == 0:
                ctx.buy(stock_idx=i, amount=100)
            # 死叉清仓卖出
            elif ma5 < ma20 and ctx.positions[i] > 0:
                ctx.sell(stock_idx=i, amount=ctx.positions[i])

    # 3. 初始化回测引擎 (包含 A 股真实双边佣金 5 元限制、印花税、万一滑点)
    print("\n -> [Step 2] 初始化 CarrotQuant 事件驱动引擎...")
    engine = Engine(
        initial_cash=1_000_000.0,
        fee_rate=0.0003,      # 佣金万三
        min_fee=5.0,          # 最小佣金 5 元
        stamp_duty=0.0005,    # 卖出印花税千分之0.5
        slippage=0.0001,      # 交易滑点万一
    )

    # 4. 执行极速事件驱动回测
    print(" -> [Step 3] 执行回测...")
    results = engine.run(strategy=dual_ma_cross_strategy, data=data)

    # 5. 输出分析结果
    print("\n -> [Step 4] 回测结果分析与绩效报告:")
    print(results.summary())

    print("\n -> 交易日志预览 (Polars DataFrame Top 10):")
    print(results.trade_logs.head(10))

    print("\n [DONE] Demo 演示运行完成!")


if __name__ == "__main__":
    main()
