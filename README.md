# CarrotQuant

**CarrotQuant** 是专为 A 股全市场（5000+ 标的）设计的高性能 Numba 事件驱动回测引擎。

## 🌟 核心亮点
- **极致吞吐量**：基于 Numba JIT 打平内联与连续 2D C-Contiguous 内存布局，零堆分配开销。
- **@strategy 零损耗抽象**：采用装饰器模式函数打平内联，物理切片 `[:t+1]` 彻底杜绝未来函数。
- **真实 A 股撮合机制**：包含佣金门槛（如 5 元限制）、印花税、动态 VWAP/TWAP/OPEN/CLOSE 撮合价格模式。
- **轻量按列加载**：基于 Polars / PyArrow 快速装载 Parquet 列数据。

## 🚀 快速开始

```python
from carrotquant import strategy, BarContext, Engine
from carrotquant.data import ColumnDataLoader

# 1. 定义策略 (使用 @strategy 装饰)
@strategy
def my_strategy(ctx: BarContext):
    for i in range(ctx.n_stocks):
        if ctx.is_tradable[i] and ctx.close[i] > 10.0:
            ctx.buy(stock_idx=i, amount=100)

# 2. 读取行情矩阵
data = ColumnDataLoader.load_parquet("data/test_data_root/parquet/ashare.kline.1d.raw.baostock")

# 3. 初始化并运行引擎
engine = Engine(initial_cash=1_000_000.0, fee_rate=0.0003, min_fee=5.0)
results = engine.run(strategy=my_strategy, data=data)

print(results.summary())
print(results.trade_logs)  # Polars DataFrame
```
