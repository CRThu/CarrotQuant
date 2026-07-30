# AGENTS.md - CarrotQuant AI Agent 工程与架构指南

## 1. 项目定位与核心愿景
`CarrotQuant` 是一个基于 Python/Numba 的 1m+ 高性能全市场事件驱动量化回测引擎。
核心设计目标：
- 极致性能：2D NumPy C-Contiguous 连续内存块、SoA 预分配数组、Numba `@njit` 无堆分配打平内联。
- 零开销策略抽象：使用 `@strategy` 装饰器打平内联，物理边界防未来函数切片。
- A股交易特性：原生支持印花税、双边佣金门槛 (如 5元限制)、VWAP/TWAP/OPEN/CLOSE 撮合机制及后复权/原始价动态解耦。
- 双引擎执行模式：同时提供**灵活 Python 回调模式 (`engine.run`)** 与 **极致 Fast JIT 全速模式 (`engine.run_fast`)** (3000万+ Ticks/s)。

## 2. 代码分层与架构规范
- `carrotquant.data`: 负责 Parquet/CSV 列式按需读取、Standard Storage Root Hive 分区加载、数据对齐及 Chunk 流式推流。
- `carrotquant.engine`: 纯 JIT 算子 (`matching.py` 与 `run_engine_jit_kernel`) 与 SoA 状态管理 (`state.py`)，无 Python 堆开销。
- `carrotquant.strategy`: `@strategy` 装饰器与 `BarContext` 切片视图 (`[:t+1]` 物理拦截)。
- `carrotquant.indicators`: Numba 兼容的动态滑窗递推算子。
- `carrotquant.analytics`: 结果提交、日志解析与 Polars 绩效度量。

## 3. 两种执行模式机制说明
- **Python 回调模式 (`engine.run`)**: 主 Bar 循环在 Python 层推送，每个 Bar 构造 `BarContext` 视图，兼顾调度的极大灵活性与方便 Python 断点调试。
- **Fast JIT 全速模式 (`engine.run_fast`)**: 把信号矩阵与下单数量矩阵作为 2D NumPy C-Array 传入，主 Bar 循环、信号读取、撮合计算、印花税/佣金扣除与资产估值在 `run_engine_jit_kernel` 中**100% 打包编译为 LLVM 纯机器码**，L1/L2 Cache 一通到底，实现 3000万+ Ticks/s 的极限速度。

## 4. 开发与测试准则
- 所有内核算子必须经过 Numba `@njit(fastmath=True, nogil=True)` 编译验证。
- 测试套件必须 100% 独立于外部磁盘物理 `data/` 路径，采用内存与 `tmp_path` 构造 Mock 数据测试。
- 必须通过 `test_anti_lookahead.py` 防未来函数 Chaos 混沌注入验证。
- 保持单元测试覆盖率高于 80%。
