# CarrotQuant

**CarrotQuant** 是基于 Python/Numba 的 1m+ 高性能通用全市场 (A股/美股/期货) 事件驱动与向量化量化回测引擎。

## 🌟 核心亮点
- **极致吞吐量**：基于 Numba JIT 打平内联与连续 2D C-Contiguous 内存布局，零堆分配开销 (3000万+ Ticks/s)。
- **通用多空机制**：`buy` / `sell` 天然支持做多与做空 (`pos += amount` 与 `pos -= amount`)，统一浮动资产计算 $PV = \text{Cash} + \sum \text{pos}_i \times \text{close}_i$。
- **轻量动态复权架构**：`data.close` 为原始真实交割价，`data.adj.close` / `ctx.adj.close` 为动态懒求值复权视角。无复权需求或已有复权时零开销。
- **多表与自定义列按需/懒加载 (`LazyCustomFields`)**：自动支持多表字段与自定义特征列（如因子 `factor_b`、`pe_ttm`、`vwap` 等）。支持 `custom_columns` 显式筛选与字段级懒透视，访问时才生成 2D 矩阵，未访问零开销。
- **物理严格防未来切片**：策略通过 `ctx.get('factor_b')`（当前 $t$ 步切片）与 `ctx.get_history('factor_b')`（物理边界 `[:t+1, :]`）访问数据，绝无未来函数污染。
- **盘口流动性限制**：支持设置 `max_volume_ratio`（例如 `0.1` 表示单笔交易上限为当前 Bar 10% 成交量）。
- **限价单与撤单机制**：支持 `buy_limit` / `sell_limit` 限价单与 `cancel_order` 撤单，支持跨 Bar 订单保存与价格触达自动撮合。
- **做多/做空保证金率与融资融券扣费**：支持设置 `long_margin_ratio` / `short_margin_ratio`（保证金率校验），以及 `margin_interest_rate` / `borrow_interest_rate`（日频融资与融券利息扣除）。
- **统一极简单入口 API (`engine.run`)**：无论内存单 Container、磁盘分块 Stream，还是 JIT 信号矩阵，统一通过 `engine.run(...)` 单一方法启动。

## 🚀 快速开始

```python
from carrotquant import strategy, BarContext, Engine, ColumnDataLoader

# 1. 定义策略 (使用 @strategy 装饰器，支持自定义列 factor_b)
@strategy
def dual_ma_strategy(ctx: BarContext):
    # 读取自定义因子列 factor_b 当前快照 (N,) 与 历史切片 [:t+1, :]
    factor_b = ctx.get("factor_b")
    
    for i in range(ctx.n_symbols):
        if not ctx.is_tradable[i]:
            continue

        # 使用 ctx.adj.close_history 读取后复权历史收盘价
        c_hist = ctx.adj.close_history[-20:, i]
        ma5 = c_hist[-5:].mean()
        ma20 = c_hist[-20:].mean()

        # 结合因子与均线信号买卖
        if ma5 > ma20 and factor_b[i] > 0.5 and ctx.positions[i] == 0:
            ctx.buy(symbol_idx=i, amount=100)
        elif ma5 < ma20 and ctx.positions[i] > 0:
            ctx.sell(symbol_idx=i, amount=ctx.positions[i])

# 2. 磁盘级惰性分块扫描 (指定按需加载特征列 custom_columns)
data_stream = ColumnDataLoader.scan_parquet_chunks(
    path="data/parquet/ashare.kline.1m",
    custom_columns=["factor_b"],
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

