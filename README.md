# CarrotQuant

[![PyPI version](https://img.shields.io/pypi/v/carrotquant.svg)](https://pypi.org/project/carrotquant/)
[![Python Version](https://img.shields.io/badge/python-%3E%3D3.12-blue)](https://pypi.org/project/carrotquant/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

**CarrotQuant** 是高性能全栈量化交易、金融数据流水线与回测框架的主入口元包 (Umbrella Package)，聚合了数据中台与回测内核两大核心组件。

---

## 🏗️ 架构全景

```text
               pip install carrotquant (默认安装全套能力)
                                  │
         ┌────────────────────────┴────────────────────────┐
         ▼                                                 ▼
┌─────────────────────────────┐               ┌─────────────────────────────┐
│    carrotquant-engine       │               │      carrotquant-data       │
│  (Numba 极速回测与撮合引擎)   │               │  (金融数据增量同步与持久化)   │
│  - 事件驱动与向量化撮合     │               │  - Baostock / 东财 / 通达信 │
│  - 物理切片严格防未来函数   │               │  - 列式存储 (Parquet / CSV) │
│  - 多空交易与滑点/税费模型  │               │  - React Web 金融终端 & CLI │
└─────────────────────────────┘               └─────────────────────────────┘
```

---

## 📦 安装 (Installation)

环境要求：**Python >= 3.12**（支持 Python 3.12 / 3.13 / 3.14+）。

### 1. 默认安装 (推荐，开箱即用)
```bash
# 默认安装 carrotquant-engine 与 carrotquant-data 全套组件
pip install carrotquant

# 或使用 uv 安装
uv add carrotquant
```

### 2. 细分可选安装 (Extras)
```bash
# 仅安装核心回测引擎
pip install carrotquant[engine]

# 仅安装数据同步与 Web 终端
pip install carrotquant[data]

# 显式安装全套依赖
pip install carrotquant[all]
```

---

## 🚀 快速开始

### 1. 数据同步与 Web 终端管理
通过命令行工具 `cqdata`（安装 `carrotquant` 后自动就绪）：

```bash
# 启动本地 Web 数据终端与 REST API 服务
cqdata server --port 8888 --open

# 触发 A 股日线数据自动增量同步
cqdata sync -t ashare.kline.1d.raw.baostock
```

### 2. 数据读取与策略回测全流程
```python
# 方式 A：通过主元包分层结构化调用
import carrotquant as cq

# 1. (数据层) 读取本地清洗好的 Parquet/CSV 数据
df = cq.data.read(
    table_id="ashare.kline.1d.raw.baostock",
    symbols=["sh.600000", "sz.000001"],
    start_date="2023-01-01",
    end_date="2023-12-31"
)

# 2. (策略层) 定义事件驱动双均线策略
@cq.engine.strategy
def dual_ma_strategy(ctx: cq.engine.BarContext):
    for i in range(ctx.n_symbols):
        if not ctx.is_tradable[i]:
            continue

        # 读取后复权历史收盘价
        c_hist = ctx.adj.close_history[-20:, i]
        ma5 = c_hist[-5:].mean()
        ma20 = c_hist[-20:].mean()

        if ma5 > ma20 and ctx.positions[i] == 0:
            ctx.buy(symbol_idx=i, amount=100)
        elif ma5 < ma20 and ctx.positions[i] > 0:
            ctx.sell(symbol_idx=i, amount=ctx.positions[i])

# 3. (引擎层) 初始化并启动 Numba 高性能回测
data_stream = cq.engine.ColumnDataLoader.scan_parquet_chunks(
    path="data/parquet/ashare.kline.1d.raw.baostock",
    partition_by="year"
)

engine = cq.engine.Engine(
    initial_cash=1_000_000.0,
    fee_rate=0.0003,
    stamp_duty=0.0005,
    slippage=0.0001,
    matching_mode="close"
)

results = engine.run(strategy=dual_ma_strategy, data=data_stream)

# 4. (分析层) 输出绩效汇总与成交明细
print(results.summary())
print(results.trade_logs)  # Polars DataFrame
```

方式 B：按需直接使用标准子命名空间导入：
```python
from cq.data import read, ashare
from cq.engine import Engine, strategy, BarContext, ColumnDataLoader
```

---

## 🔗 生态子项目链接

- **回测引擎源码**：[carrotquant-engine](https://github.com/CRThu/carrotquant-engine)
- **数据管理源码**：[carrotquant-data](https://github.com/CRThu/carrotquant-data)

---

## 📝 许可证 (License)

本项目遵循 [Apache License 2.0](LICENSE)。
