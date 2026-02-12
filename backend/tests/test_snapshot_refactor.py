import sys
import os
import pandas as pd
from datetime import date

# 动态调整路径，确保能找到 models 和 core
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from models.market import TableData, TABLE_REGISTRY
from services.data import data_manager

def test_table_data_methods():
    print("Testing TableData methods...")
    # 测试一对一映射
    data_1to1 = {"000001": "平安银行", "000002": "万科A"}
    td = TableData(name="test_1to1", data=data_1to1)
    
    assert td.get_value("000001") == "平安银行"
    assert td.get_value("000003", "未知") == "未知"
    assert td.get_list("000001") == ["平安银行"]
    assert td.get_list("000003") == []
    
    # 测试一对多映射
    data_1toM = {"板块A": ["000001", "000002"], "板块B": ["000003"]}
    td_m = TableData(name="test_1toM", data=data_1toM)
    
    assert td_m.get_list("板块A") == ["000001", "000002"]
    assert td_m.get_list("板块C") == []
    print("TableData methods test passed!")

def test_registry_config():
    print("Testing TABLE_REGISTRY config...")
    assert "cn_stock_em" in TABLE_REGISTRY
    assert TABLE_REGISTRY["cn_stock_em"]["storage_type"] == "snapshot"
    assert TABLE_REGISTRY["cn_stock_em_daily_adj"]["storage_type"] == "partition"
    print("TABLE_REGISTRY config test passed!")

def test_metadata_audit_o1():
    print("Testing O(1) metadata audit...")
<<<<<<< Updated upstream
    # 模拟环境：创建一个虚拟的 Parquet 文件
=======
    # 模拟环境：创建一个虚拟的快照 Parquet 文件（无 trade_date 列）
>>>>>>> Stashed changes
    data_dir = "backend/tests/mock_data"
    os.makedirs(os.path.join(data_dir, "cn_stock_em"), exist_ok=True)
    parquet_path = os.path.join(data_dir, "cn_stock_em", "cn_stock_em.parquet")
    
    df = pd.DataFrame({
<<<<<<< Updated upstream
        "trade_date": [date(2025, 1, 1), date(2025, 1, 1)],
=======
>>>>>>> Stashed changes
        "stock_code": ["000001", "000002"],
        "stock_name": ["平安银行", "万科A"]
    })
    df.to_parquet(parquet_path)
    
    # 临时修改 settings.DATA_DIR
    from core.config import settings
    old_data_dir = settings.DATA_DIR
    settings.DATA_DIR = os.path.abspath(data_dir)
    
    try:
        data_manager.initialize()
        meta = data_manager.get_storage_metadata()
        
        assert "cn_stock_em" in meta
        assert meta["cn_stock_em"]["storage_type"] == "snapshot"
        assert meta["cn_stock_em"]["row_count"] == 2
<<<<<<< Updated upstream
=======
        assert meta["cn_stock_em"]["start_date"] is None
        assert meta["cn_stock_em"]["end_date"] is None
>>>>>>> Stashed changes
        assert "cn_stock_em.parquet" in meta["cn_stock_em"]["path"]
        print("Metadata audit test passed!")
    finally:
        settings.DATA_DIR = old_data_dir
        # 清理
        import shutil
        shutil.rmtree(data_dir)

if __name__ == "__main__":
    try:
        test_table_data_methods()
        test_registry_config()
        test_metadata_audit_o1()
        print("\nAll tests passed successfully!")
    except Exception as e:
        print(f"\nTest failed: {e}")
        sys.exit(1)
