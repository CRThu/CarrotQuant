import os
import sys
import shutil
import glob
import pandas as pd
import numpy as np
import datetime
from loguru import logger

# Add backend to path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from services.data import data_manager
import services.data
from models.market import MarketDataContainer, TableData, TABLE_REGISTRY

# Configure logger
logger.remove()
logger.add(sys.stderr, level="DEBUG")

def setup_dummy_data(test_dir):
    """
    Create a dummy parquet structure:
    """
    np.random.seed(42) # Fixed seed
    table_name = "cn_stock_em_daily_adj"
    table_dir = os.path.join(test_dir, table_name)
    year_dir = os.path.join(table_dir, "year=2024")
    os.makedirs(year_dir, exist_ok=True)
    
    # Create dummy dataframe
    dates = pd.date_range(start="2024-01-01", end="2024-01-05") # 5 days
    symbols = ["000001", "000002"]

    data = []
    for d in dates:
        for s in symbols:
            # Simulate missing data for alignment check
            if s == "000002" and d == dates[-1]:
                continue # Skip last day for 000002
                
            data.append({
                "trade_date": d.strftime("%Y-%m-%d"),
                "stock_code": s,
                "open": float(np.random.rand()),
                "close": float(np.random.rand()),
                "high": float(np.random.rand()),
                "low": float(np.random.rand()),
                "volume": int(np.random.randint(100, 1000)),
                "amount": float(np.random.rand() * 1000),
                "pct_change": float(np.random.rand()),
                "turnover": float(np.random.rand())
            })
            
    df = pd.DataFrame(data)
    df.to_parquet(os.path.join(year_dir, "data.parquet"))
    logger.info(f"Created dummy data at {year_dir}")
    return df

def cleanup_dummy_data(test_dir):
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
        logger.info(f"Cleaned up {test_dir}")

def test_loading():
    # 1. Setup
    test_root = os.path.join(os.path.dirname(__file__), "temp_data")
    cleanup_dummy_data(test_root) # Ensure clean start
    
    try:
        setup_dummy_data(test_root)
        
        # 2. Patch DATA_ROOT
        logger.info(f"Patching DATA_ROOT to {test_root}")
        from core.config import settings
        original_data_dir = settings.DATA_DIR
        settings.DATA_DIR = test_root
        
        # 3. Test Metadata
        logger.info("Testing get_storage_metadata...")
        metadata = data_manager.get_storage_metadata()
        
        # 4. Test Load Data
        logger.info("Testing load_market_data...")
        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2024, 1, 5)
        
        container = data_manager.load_market_data(
            table_names=["cn_stock_em_daily_adj"],
            start_date=start_date,
            end_date=end_date,
            symbols=["000001", "000002"]
        )
        
        # 5. Verify Container (Flat Access)
        close_table = container["cn_stock_em_daily_adj_close"]
        vol_table = container["cn_stock_em_daily_adj_volume"]
        
        logger.debug(f"Timeline: {close_table.timeline}")
        logger.debug(f"Symbols: {close_table.symbols}")
        logger.debug(f"Close Matrix:\n{close_table.data}")
        
        # Check volume (zero fill)
        val_vol_missing = vol_table["2024-01-05", "000002"]
        logger.info(f"Missing Volume Value (000002, 2024-01-05): {val_vol_missing}")
        assert val_vol_missing == 0.0 
        
        # Check close (ffill)
        val_close_missing = close_table["2024-01-05", "000002"]
        val_close_prev = close_table["2024-01-04", "000002"]
        logger.info(f"Missing Close Value: {val_close_missing}, Prev: {val_close_prev}")
        assert val_close_missing == val_close_prev
        
        logger.success("Verification Passed!")
        
    except Exception as e:
        logger.error(f"Verification Failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        settings.DATA_DIR = original_data_dir 
        cleanup_dummy_data(test_root)

if __name__ == "__main__":
    test_loading()
