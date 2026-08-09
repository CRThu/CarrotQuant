# CarrotQuant

**CarrotQuant** 是基于 Python/Numba 的 1m+ 高性能通用全市场 (A股/美股/期货) 事件驱动与向量化量化回测引擎。

## 🌟 核心亮点
- **极致吞吐量**：基于 Numba JIT 打平内联与连续 2D C-Contiguous 内存布局，零堆分配开销 (3000万+ Ticks/s)。
- **通用多空机制**：`buy` / `sell` 天然支持做多与做空 (`pos += amount` 与 `pos -= amount`)，统一浮动资产计算 $PV = \text{Cash} + \sum \text{pos}_i \times \text{close}_i$。
- **标准价格架构**：`data.close` 为原始真实交割价，`data.adj.close` 为策略指标复权价。
- **盘口流动性限制**：支持设置 `max_volume_ratio`（例如 `0.1` 表示单笔交易上限为当前 Bar 10% 成交量）。
- **磁盘级分块流式加载**：支持 `partition_by="year"` 或 `"month"` 按 Hive 分区惰性扫描，由引擎自动无缝连贯回测，让普通电脑轻松跑完 10 年 1m 全市场数据。
- **统一极简单入口 API (`engine.run`)**：无论内存单 Container、磁盘分块 Stream，还是 JIT 信号矩阵，统一通过 `engine.run(...)` 单一方法启动。

## 🚀 快速开始

```python
from carrotquant import strategy, BarContext, Engine, ColumnDataLoader

# 1. 定义策略 (使用 @strategy 装饰)
@strategy
def dual_ma_strategy(ctx: BarContext):
    for i in range(ctx.n_symbols):
        if not ctx.is_tradable[i]:
            continue

        # 使用 ctx.adj.close_history 读取后复权历史收盘价
        c_hist = ctx.adj.close_history[-20:, i]
        ma5 = c_hist[-5:].mean()
        ma20 = c_hist[-20:].mean()

        # 金叉买入 100 股，死叉清仓卖出
        if ma5 > ma20 and ctx.positions[i] == 0:
            ctx.buy(symbol_idx=i, amount=100)
        elif ma5 < ma20 and ctx.positions[i] > 0:
            ctx.sell(symbol_idx=i, amount=ctx.positions[i])

# 2. 磁盘级惰性分块扫描 (按年/按月分块，极低内存)
data_stream = ColumnDataLoader.scan_parquet_chunks(
    path="data/parquet/ashare.kline.1m",
    partition_by="year"
)

# 3. 初始化并运行统一引擎 (设置印花税、佣金、万一滑点、盘口 10% 成交量比例)
engine = Engine(
    initial_cash=1_000_000.0,
    fee_rate=0.0003,
    min_fee=5.0,
    stamp_duty=0.0005,
    slippage=0.0001,
    max_volume_ratio=0.1,  # 盘口最多吃 10% 流动性
    matching_mode="close"  # 字符串指定按收盘价撮合
)

results = engine.run(strategy=dual_ma_strategy, data=data_stream)

# 4. 输出回测绩效与 Polars 交易日志
print(results.summary())
print(results.trade_logs)  # Polars DataFrame
```

