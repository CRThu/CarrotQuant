import pytest


from services.downloader.eastmoney import EastMoneyDownloader
import pandas as pd
from loguru import logger

@pytest.mark.network
def test_fetch_sector_daily():
    """Test fetching daily data for a specific sector."""
    downloader = EastMoneyDownloader()
    # Test with a known sector, e.g., "及其他" (Others) which usually has data
    # or "半导体" (Semiconductors)
    sector_name = "半导体" 
    start_date = "20240110"
    end_date = "20240205"
    
    try:
        df = downloader.fetch_sector_daily(sector_name, start_date, end_date)
        
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        
        expected_columns = [
            "trade_date", "open", "close", "high", "low", 
            "volume", "amount", "amplitude", "pct_change", 
            "change_amount", "turnover", "sector_name"
        ]
        for col in expected_columns:
            assert col in df.columns
            
        assert df['sector_name'].iloc[0] == sector_name
        
    except Exception as e:
        pytest.fail(f"fetch_sector_daily raised an exception: {e}")


@pytest.mark.network
def test_get_all_sectors():
    """Test fetching the list of all sectors."""
    downloader = EastMoneyDownloader()
    
    try:
        sectors = downloader.get_all_sectors()
        
        assert isinstance(sectors, list)
        assert len(sectors) > 0
        assert isinstance(sectors[0], str)
        
    except Exception as e:
        pytest.fail(f"get_all_sectors raised an exception: {e}")
