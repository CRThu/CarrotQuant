# CarrotQuant Engine (`carrotquant-engine`)

[![PyPI version](https://img.shields.io/pypi/v/carrotquant-engine.svg)](https://pypi.org/project/carrotquant-engine/)
[![Python Version](https://img.shields.io/badge/python-%3E%3D3.12-blue)](https://pypi.org/project/carrotquant-engine/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

**CarrotQuant Engine** 是基于 Python 与 Numba 的事件驱动与向量化量化回测引擎，支持全市场多品种的回测、撮合与绩效分析。

## 📦 安装指南 (Installation)

环境要求：**Python >= 3.12**（支持 Python 3.12 / 3.13 / 3.14+）。

```bash
# 使用 pip 安装
pip install carrotquant-engine

# 或使用 uv 安装
uv add carrotquant-engine
```

## 🛠️ 特性 (Features)
- **高性能计算内核**：基于 Numba JIT 与连续 2D C-Contiguous 内存布局，降低循环执行与内存分配开销。
- **多空双向撮合**：`buy` / `sell` 支持做多与做空 (`pos += amount` 与 `pos -= amount`)，统一浮动资产计算 $PV = \text{Cash} + \sum \text{pos}_i \times \text{close}_i$。
- **轻量动态复权**：`data.close` 为原始成交价，`data.adj.close` / `ctx.adj.close` 提供按需计算的复权视图。
- **多表与自定义字段支持 (`LazyCustomFields`)**：支持多表字段与自定义特征列（如因子 `factor`、`pe_ttm`、`vwap` 等），支持 `custom_columns` 筛选与按需生成 2D 矩阵。
- **防未来函数切片**：策略通过 `ctx.get('factor')`（当前 $t$ 步快照）与 `ctx.get_history('factor')`（物理边界 `[:t+1, :]`）访问数据，避免未来数据泄露。
- **流动性与撮合限制**：支持 `max_volume_ratio`（盘口成交量比例限制）、限价单 `buy_limit` / `sell_limit` 与 `cancel_order` 撤单机制。
- **保证金与融资融券费率**：支持设置 `long_margin_ratio` / `short_margin_ratio`（保证金率校验），以及 `margin_interest_rate` / `borrow_interest_rate`（日频利息计提）。
- **统一运行入口 (`engine.run`)**：支持内存数据 `MarketData`、磁盘分块流 `scan_parquet_chunks` 以及向量化信号矩阵。

## 🚀 快速开始

```python
# 安装包名为 carrotquant-engine，代码中统一导入 cq.engine
from cq.engine import strategy, BarContext, Engine, ColumnDataLoader

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

