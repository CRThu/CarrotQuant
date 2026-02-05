import pytest
import asyncio
from unittest.mock import MagicMock, patch
from services.market_manager import MarketDataManager
from models.market import MarketDownloadRequest

@pytest.fixture
def manager():
    with patch('services.market_manager.DuckDBStorage'):
        return MarketDataManager()

@pytest.mark.asyncio
async def test_route_table_name(manager):
    """测试表名生成路由。"""
    req1 = MarketDownloadRequest(source="em", data_type="stock", adjust="adj")
    assert manager._route_table_name(req1) == "cn_stock_em_daily_adj"
    
    req2 = MarketDownloadRequest(source="sina", data_type="stock", adjust="raw")
    assert manager._route_table_name(req2) == "cn_stock_sina_daily_raw"

@pytest.mark.asyncio
async def test_auto_scheduling_symbols(manager):
    """测试 symbols 为空时的自动补全逻辑。"""
    # Mock downloader
    mock_dl = MagicMock()
    mock_dl.get_all_symbols.return_value = ["000001", "000002"]
    manager.downloaders["em"] = mock_dl
    
    request = MarketDownloadRequest(source="em", data_type="stock", symbols=None)
    
    with patch.object(manager, '_run_monthly_split_download', return_value=None):
        task_id = await manager.start_market_download_task(request)
        
        assert task_id in manager.tasks
        # 验证是否调用了 get_all_symbols
        mock_dl.get_all_symbols.assert_called_once()
        assert "2 个标的" in manager.tasks[task_id].message

@pytest.mark.asyncio
async def test_invalid_source(manager):
    """测试非法源。"""
    request = MarketDownloadRequest(source="invalid", data_type="stock")
    with pytest.raises(ValueError, match="不受支持的数据源"):
        await manager.start_market_download_task(request)
