import asyncio

import pytest
import os
import shutil
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from services.market_manager import market_manager
from models.market import SectorDownloadRequest
from models.task import TaskStatus
from core.config import settings

# 确保测试数据不污染生产环境，最好使用临时目录，或在测试后清理
# 这里我们测试真实流程，使用单独的测试表名或默认表名但测试后清理

@pytest.mark.asyncio
@pytest.mark.network
async def test_market_data_flow():
    """
    集成测试: 下载 -> 存储 -> 读取验证
    """
    print("\n[Test] Starting Market Data Flow Integration Test...")

    # 1. 准备请求 (测试一个小范围，例如 3 天)
    # 使用东方财富实际存在的板块，例如 "酿酒行业" (BK0477) 但 api 传名称即可
    # 为了测试稳定性，选取一个必定有数据的近期时间段
    now = datetime.now()
    if now.month == 1:
        # 如果是1月，测去年的12月
        start_date = f"{now.year-1}1201"
        end_date = f"{now.year-1}1205"
        test_year = now.year - 1
        test_month = 12
    else:
        # 否则测本月或上月
        start_date = f"{now.year}0101"
        end_date = f"{now.year}0105"
        test_year = now.year
        test_month = 1

    # 只下载一个热门板块，减少耗时
    req = SectorDownloadRequest(
        sectors=["酿酒行业"], 
        start_date=start_date, 
        end_date=end_date
    )
    
    # 2. 启动任务
    task_id = await market_manager.start_sector_download_task(req)
    assert task_id is not None
    print(f"[Test] Task Started: {task_id}")

    # 3. 轮询等待完成
    max_retries = 30
    for i in range(max_retries):
        task = market_manager.get_task(task_id)
        print(f"[Test] Wait {i}: Status={task.status}, Progress={task.progress}%")
        
        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.STOPPED]:
            break
        await asyncio.sleep(2)
    
    assert task.status == TaskStatus.COMPLETED, f"Task failed with message: {task.message}"
    print("[Test] Task Completed Successfully.")

    # 4. 验证存储结果
    # 期望路径: data/dfcft-a-bk/year=YYYY/YYYY-MM.parquet
    # 注意 storage.py 中写死了 table_name="dfcft-a-bk"
    file_path = Path(settings.DATA_DIR) / f"dfcft-a-bk/year={test_year}/{test_year}-{test_month:02d}.parquet"
    print(f"[Test] Checking file: {file_path}")
    
    assert file_path.exists(), "Parquet file was not created!"

    # 5. DuckDB 读取验证
    import duckdb
    conn = duckdb.connect()
    try:
        # 测试 hive_partitioning 读取
        # 即使我们只有 year=YYYY 目录，DuckDB 也能识别 year 列
        # 注意：文件名包含月份，但目录没有 month=MM，所以 DuckDB 不会自动识别 month 列为分区列
        # month 列必须在 Parquet 文件内部存在，或者我们文件名不包含信息...
        # 等等，之前的代码 create table 时 cast 并没有包含 month 列吗？
        # 看 storage.py: cast_columns 里没有 month。
        # 如果目录只有 year=...，那么读取出来的 DF 会有 year 列，但没有 month 列。
        # 这是一个 design choice。如果用户接受不需要 month 列，或者 month 列包含在 trade_date 里。
        
        # 读取验证
        df = conn.execute(f"SELECT * FROM '{file_path}'").df()
        print(f"[Test] Read {len(df)} rows from Parquet.")
        print(df.head(2))
        
        assert not df.empty, "Read dataframe is empty"
        assert 'trade_date' in df.columns
        assert 'sector_name' in df.columns
        # 验证类型
        assert pd.api.types.is_datetime64_ns_dtype(df['trade_date']) or isinstance(df['trade_date'].iloc[0], date)
        
        # 验证 year 分区值 (如果用 read_parquet 且指定 hive_partitioning)
        # 这里直接读文件可能不会自动附带 year 列，除非 scan 目录
        # 我们可以测试 scan 目录
        dataset_path = Path(settings.DATA_DIR) / "dfcft-a-bk"
        df_dataset = conn.execute(f"SELECT * FROM parquet_scan('{dataset_path}/**/*.parquet', hive_partitioning=1)").df()
        
        assert 'year' in df_dataset.columns, "Year partition column missing in dataset scan"
        assert len(df_dataset) >= len(df)
        
    finally:
        conn.close()
    
    print("[Test] Verification Passed.")
