import asyncio
import sys
from pathlib import Path

from services.market_manager import market_manager
from models.market import MarketDownloadRequest, MarketTable
from models.task import TaskStatus
from core.config import settings

async def run_verify_task(req: MarketDownloadRequest):
    print(f"\n[Testing] Source: {req.source}, Type: {req.data_type}, Symbols: {req.symbols}")
    task_id = await market_manager.start_market_download_task(req)
    
    while True:
        task = market_manager.get_task(task_id)
        if not task: break
        print(f"Status: {task.status}, Progress: {task.progress}%, Msg: {task.message}")
        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.STOPPED]:
            break
        await asyncio.sleep(2)
        
    if task.status == TaskStatus.COMPLETED:
        print("Task Completed Successfully.")
        # Verify first month
        month_str = req.months[0]
        year = int(month_str[:4])
        month = int(month_str[4:])
        
        table_name = market_manager._route_table_name(req)
        parquet_path = Path(settings.DATA_DIR) / f"{table_name}/year={year}/{year}-{month:02d}.parquet"
        
        if parquet_path.exists():
            import duckdb
            conn = duckdb.connect()
            df = conn.execute(f"SELECT * FROM '{parquet_path}' LIMIT 5").df()
            print(f"Verified {parquet_path}:")
            print(df.head())
            conn.close()
        else:
            print(f"Error: Parquet file missing at {parquet_path}")
    else:
        print(f"Task Failed: {task.message}")

async def main():
    print("=== CarrotQuant Professional Refactoring Verification ===")
    
    # 1. EM Sector Raw
    await run_verify_task(MarketDownloadRequest(
        source="em", data_type="sector", symbols=["半导体"], months=["202401"], adjust="raw"
    ))
    
    # 2. EM Stock Adj (Professional Standard)
    await run_verify_task(MarketDownloadRequest(
        source="em", data_type="stock", symbols=["000001"], months=["202402"], adjust="adj"
    ))
    
    # 3. Sina Stock Adj
    await run_verify_task(MarketDownloadRequest(
        source="sina", data_type="stock", symbols=["600519"], months=["202403"], adjust="adj"
    ))

    # 4. Test Auto-scheduling (just symbol list fetch)
    print("\n[Testing] Auto-fetching (Sample Top 10) symbols for EM...")
    from services.downloader.eastmoney import EastMoneyDownloader
    dl = EastMoneyDownloader()
    syms = dl.get_all_symbols()
    test_syms = syms[:10]  # 仅取前 10 个以加快验证
    print(f"Fetched {len(syms)} stock symbols. Testing Sample: {test_syms}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
