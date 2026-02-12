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
async def test_auto_scheduling_symbols(manager):
    """测试 symbols 为空时的自动补全逻辑。"""
    # Mock downloader
    mock_dl = MagicMock()
    mock_dl.get_all_symbols.return_value = ["000001", "000002"]
    manager.downloaders["em"] = mock_dl
    
    # 使用具体的表名发起请求
    request = MarketDownloadRequest(table_name="cn_stock_em_daily_adj", symbols=None, months=["202501"])
    
    with patch.object(manager, '_run_partition_download', return_value=None):
        task_id = await manager.start_market_download_task(request)
        
        assert task_id in manager.tasks
        # 验证是否调用了 get_all_symbols
        mock_dl.get_all_symbols.assert_called_once()
        assert "cn_stock_em_daily_adj" in manager.tasks[task_id].message

@pytest.mark.asyncio
async def test_invalid_table(manager):
    """测试非法表名。"""
    request = MarketDownloadRequest(table_name="invalid_table")
    with pytest.raises(ValueError, match="不受支持的表名"):
        await manager.start_market_download_task(request)
