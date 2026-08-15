# AGENTS.md - CarrotQuant Engine AI Agent 工程与架构指南

## 1. 项目定位与核心设计
`CarrotQuant Engine` (`carrotquant-engine`) 是基于 Python/Numba 的全市场 (A股/美股/期货) 事件驱动与向量化量化回测引擎。

核心设计：
- **内存与计算优化**：采用 2D NumPy C-Contiguous 内存布局与 SoA 预分配数组，核心撮合使用 Numba `@njit(nogil=True)`。
- **策略接口与数据防窥**：提供 `@strategy` 装饰器，运行时通过 `[:t+1]` 物理边界切片防止未来函数污染。
- **多空撮合机制**：`buy/sell` 支持多空双向交易，浮动资产计算公式为 $PV = \text{Cash} + \sum \text{pos}_i \times \text{close}_i$；支持滑点、印花税、最低佣金与 `max_volume_ratio` 盘口成交量上限限制。
- **价格分层**：`data.close` 为原始成交价（用于撮合与交割），`data.adj.close` 为动态复权价（用于计算技术指标）。
- **流式驱动**：支持单块内存 `MarketData` 及按年/月分块的磁盘级 `scan_parquet_chunks` 惰性数据流。

## 2. 代码分层与架构规范
- `carrotquant.data`: Parquet/CSV 列式读取、Hive 分区装载、`MarketData` 矩阵对齐与 `scan_parquet_chunks` 流式迭代。
- `carrotquant.engine`: JIT 撮合内核 (`matching.py`)、`MatchingMode` 枚举/参数解析与 SoA 状态管理 (`state.py`)。
- `carrotquant.strategy`: `@strategy` 装饰器与 `BarContext` 上下文切片 (`ctx.symbol_idx`, `ctx.adj.close_history`)。
- `carrotquant.indicators`: Numba 兼容的递推指标算子。
- `carrotquant.analytics`: 回测结果汇总、交易日志与 Polars 绩效度量。

## 3. 执行模式
- **统一入口 API (`engine.run`)**:
  - **事件驱动回调**: `engine.run(strategy=my_strat, data=data)`
  - **向量化矩阵模式**: `engine.run(signals=signals, amounts=amounts, data=data)`
  - **磁盘分块流式模式**: `engine.run(strategy=my_strat, data=ColumnDataLoader.scan_parquet_chunks(...))`

## 4. 开发与测试准则
- 所有内核算子必须经过 Numba `@njit(nogil=True)` 编译验证。
- 测试套件必须 100% 独立于外部磁盘物理 `data/` 路径，采用内存与 `tmp_path` 构造 Mock 数据测试。
- 必须通过 `test_anti_lookahead.py` 防未来函数 Chaos 混沌注入验证。
- 保持单元测试覆盖率高于 80%。

