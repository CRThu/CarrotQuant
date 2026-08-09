# AGENTS.md - CarrotQuant AI Agent 工程与架构指南

## 1. 项目定位与核心愿景
`CarrotQuant` 是一个基于 Python/Numba 的 1m+ 高性能通用全市场 (A股/美股/期货) 事件驱动量化回测引擎。
核心设计目标：
- 极致性能：2D NumPy C-Contiguous 连续内存块、SoA 预分配数组、Numba `@njit` 无堆分配打平内联。
- 零开销策略抽象：使用 `@strategy` 装饰器打平内联，物理边界防未来函数切片 (`[:t+1]`)。
- 通用交易特性：`buy/sell` 天然支持多空双向交易，统一浮动资产计算 $PV = \text{Cash} + \sum \text{pos}_i \times \text{close}_i$；支持滑点、印花税、双边佣金门槛、`max_volume_ratio` 盘口流动性上限撮合。
- 标准价格架构：`data.close` 为原始未复权交割价，`data.adj.close` 为策略指标复权价。
- 统一 Stream-Native 入口：统一通过优雅的 `engine.run(...)` API 驱动单块 `MarketData` 或磁盘级按年/按月惰性分块 Stream。

## 2. 代码分层与架构规范
- `carrotquant.data`: 负责 Parquet/CSV 列式按需读取、Standard Storage Root Hive 分区装载、`MarketData` 矩阵对齐与磁盘级 `scan_parquet_chunks` 惰性分块。
- `carrotquant.engine`: 纯 JIT 算子 (`matching.py` 与 `run_engine_jit_kernel`)、`MatchingMode` 枚举/字符串解析与 SoA 状态管理 (`state.py`)。
- `carrotquant.strategy`: `@strategy` 装饰器与 `BarContext` 切片视图 (`ctx.symbol_idx`, `ctx.adj.close_history`)。
- `carrotquant.indicators`: Numba 兼容的动态滑窗递推算子。
- `carrotquant.analytics`: 结果提交、日志解析与 Polars 绩效度量。

## 3. 统一执行模式与 Stream-Native 机制
- **统一入口 API (`engine.run`)**:
  - **Python 回调模式**: `engine.run(strategy=my_strat, data=data)`
  - **Fast Vectorized 极速矩阵模式**: `engine.run(signals=signals, amounts=amounts, data=data)` (3000万+ Ticks/s)
  - **磁盘分块流式模式**: `engine.run(strategy=my_strat, data=ColumnDataLoader.scan_parquet_chunks(...))`

## 4. 开发与测试准则
- 所有内核算子必须经过 Numba `@njit(nogil=True)` 编译验证。
- 测试套件必须 100% 独立于外部磁盘物理 `data/` 路径，采用内存与 `tmp_path` 构造 Mock 数据测试。
- 必须通过 `test_anti_lookahead.py` 防未来函数 Chaos 混沌注入验证。
- 保持单元测试覆盖率高于 80%。

