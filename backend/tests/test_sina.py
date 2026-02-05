import pytest
import pandas as pd
from services.downloader.sina import SinaDownloader

@pytest.mark.network
def test_sina_fetch_stock_daily():
    """测试新浪日线下载。"""
    downloader = SinaDownloader()
    symbol = "600519"
    start_date = "20240301"
    end_date = "20240305"
    
    try:
        df = downloader.fetch_stock_daily(symbol, start_date, end_date, adjust="adj")
        
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        
        expected_columns = [
            "trade_date", "open", "close", "high", "low", 
            "volume", "amount", "amplitude", "pct_change", 
            "change_amount", "turnover", "stock_code"
        ]
        for col in expected_columns:
            assert col in df.columns
            
        assert df['stock_code'].iloc[0] == symbol
    except Exception as e:
        pytest.fail(f"Sina fetch_stock_daily failed: {e}")

@pytest.mark.network
def test_sina_get_all_symbols():
    """测试新浪代码列表获取。"""
    downloader = SinaDownloader()
    try:
        symbols = downloader.get_all_symbols()
        assert isinstance(symbols, list)
        assert len(symbols) > 4000
    except Exception as e:
        pytest.fail(f"Sina get_all_symbols failed: {e}")
