"""
tests/test_meta_import.py

验证 carrotquant 主元包的顶层结构化导入与子包 (cq.engine & cq.data) 协同能力。
"""

import carrotquant
import cq.engine
import cq.data


def test_meta_version():
    """验证元包版本号存在且符合语义版本"""
    assert hasattr(carrotquant, "__version__")
    assert carrotquant.__version__ == "1.1.0"


def test_engine_subpackage_available():
    """验证通过 carrotquant.engine 与 cq.engine 访问回测引擎核心算子"""
    assert hasattr(carrotquant, "engine")
    assert hasattr(carrotquant.engine, "Engine")
    assert hasattr(carrotquant.engine, "strategy")
    assert hasattr(carrotquant.engine, "BarContext")
    assert hasattr(carrotquant.engine, "ColumnDataLoader")
    assert hasattr(carrotquant.engine, "MarketData")
    assert hasattr(cq.engine, "Engine")
    assert hasattr(cq.engine, "__version__")


def test_data_subpackage_available():
    """验证通过 carrotquant.data 与 cq.data 访问数据中台核心算子"""
    assert hasattr(carrotquant, "data")
    assert hasattr(carrotquant.data, "read")
    assert hasattr(carrotquant.data, "sync")
    assert hasattr(carrotquant.data, "list_tables")
    assert hasattr(carrotquant.data, "ashare")
    assert hasattr(carrotquant.data, "aindex")
    assert hasattr(cq.data, "read")
    assert hasattr(cq.data, "__version__")
