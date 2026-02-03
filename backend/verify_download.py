import asyncio
import sys
from pathlib import Path

from services.market_manager import market_manager
from models.market import SectorDownloadRequest
from models.task import TaskStatus
from core.config import settings

async def main():
    print("Starting verification (Refactored)...")
    
    # 1. Test Component: Downloader
    print("\n[1] Testing EastMoneyDownloader component...")
    try:
        df = market_manager.downloader.fetch_sector_daily("半导体", "20240101", "20240110")
        print(f"Fetched {len(df)} rows. Columns: {df.columns.tolist()}")
    except Exception as e:
        print(f"Downloader failed: {e}")
        return

    # 2. Test Integration: Manager Service
    print("\n[2] Testing MarketManager Task...")
    req = SectorDownloadRequest(
        sectors=["半导体", "银行"], 
        start_date="20240101", 
        end_date="20240110"
    )
    
    task_id = await market_manager.start_sector_download_task(req)
    print(f"Task started: {task_id}")
    
    while True:
        task = market_manager.get_task(task_id)
        print(f"Task Status: {task.status}, Progress: {task.progress}%")
        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.STOPPED]:
            break
        await asyncio.sleep(1)
        
    if task.status != TaskStatus.COMPLETED:
        print(f"Task Failed or Stopped: {task.message}")
        return

    # 3. Verify Storage Result
    print("\n[3] Verifying Parquet File...")
    # Expected path: data/dfcft-a-bk/year=2024/month=1/2024-01.parquet
    parquet_path = Path(settings.DATA_DIR) / "dfcft-a-bk/year=2024/month=1/2024-01.parquet"
    
    if parquet_path.exists():
        import duckdb
        conn = duckdb.connect()
        df_saved = conn.execute(f"SELECT * FROM '{parquet_path}'").df()
        print(f"Successfully read {len(df_saved)} rows from {parquet_path}")
        print(df_saved[['trade_date', 'sector_name', 'close']].head())
        conn.close()
    else:
        print(f"Error: Parquet file not found at {parquet_path}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
