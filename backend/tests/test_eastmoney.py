import pytest
from unittest.mock import patch


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
def test_fetch_stock_daily():
    """Test fetching daily data for a specific stock."""
    downloader = EastMoneyDownloader()
    symbol = "000001"
    start_date = "20240101"
    end_date = "20240110"
    
    try:
        df = downloader.fetch_stock_daily(symbol, start_date, end_date)
        
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
        pytest.fail(f"fetch_stock_daily raised an exception: {e}")

@pytest.mark.network
def test_get_all_symbols():
    """Test fetching the list of all stock symbols."""
    downloader = EastMoneyDownloader()
    
    try:
        symbols = downloader.get_all_symbols()
        
        assert isinstance(symbols, list)
        assert len(symbols) > 4000
        assert isinstance(symbols[0], str)
    except Exception as e:
        pytest.fail(f"get_all_symbols raised an exception: {e}")

@pytest.mark.network
def test_fetch_stock_info():
    """测试获取股票基础信息 (快照)"""
    downloader = EastMoneyDownloader()
    df = downloader.fetch_stock_info()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "stock_code" in df.columns
    assert "stock_name" in df.columns
    # 验证纯净化：不应包含行情字段
    forbidden = ["open", "close", "high", "low", "volume", "amount"]
    for col in forbidden:
        assert col not in df.columns

@pytest.mark.network
def test_fetch_sector_info():
    """测试获取行业板块基础信息"""
    downloader = EastMoneyDownloader()
    df = downloader.fetch_sector_info()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "sector_name" in df.columns
    assert "open" not in df.columns # 验证纯净化

@pytest.mark.network
def test_fetch_concept_info():
    """测试获取概念板块基础信息"""
    downloader = EastMoneyDownloader()
    df = downloader.fetch_concept_info()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "concept_name" in df.columns

@pytest.mark.network
def test_fetch_stock_sector_map():
    """测试获取股票-行业映射 (Mock 限制板块数量以节省时间)"""
    downloader = EastMoneyDownloader()
    
    # 我们确定这两个板块存在且数据稳定
    test_sectors = ["半导体", "银行"]

    # 注意：Mock 路径必须是方法被调用的模块路径
    with patch.object(EastMoneyDownloader, 'get_all_sectors', return_value=test_sectors):
        df = downloader.fetch_stock_sector_map()
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "stock_code" in df.columns
        assert "sector_name" in df.columns
        
        # 验证结果中包含我们指定的板块
        unique_sectors = df['sector_name'].unique().tolist()
        for s in test_sectors:
            assert s in unique_sectors
