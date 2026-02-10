
from datetime import date
from typing import List, Optional, Dict
import numpy as np
from pydantic import BaseModel, field_validator

class MarketTable:
    """数据表名常量 (SQL 兼容命名: {市场}_{品种}_{来源}_{频率}_{复权})"""
    CN_STOCK_EM_DAILY_ADJ = "cn_stock_em_daily_adj"     # 东财个股-日线-后复权
    CN_STOCK_EM_DAILY_RAW = "cn_stock_em_daily_raw"     # 东财个股-日线-原始
    CN_SECTOR_EM_DAILY_ADJ = "cn_sector_em_daily_adj"   # 东财板块-日线-后复权 (通常板块不复权，但统一结构)
    CN_SECTOR_EM_DAILY_RAW = "cn_sector_em_daily_raw"   # 东财板块-日线-原始
    CN_STOCK_SINA_DAILY_ADJ = "cn_stock_sina_daily_adj" # 新浪个股-日线-后复权

# 全局 Schema 注册表：定义 DuckDB 加载模式与清洗规则
# load_mode: matrix (行情矩阵), mapping (字段映射)
TABLE_REGISTRY = {
    "cn_stock_em_daily_adj": {
        "load_mode": "matrix",
        "id_col": "stock_code",
        "fields": ["open", "close", "high", "low", "volume", "amount", "pct_change", "turnover"],
        "ffill_cols": ["open", "close", "high", "low"],
        "zerofill_cols": ["volume", "amount", "turnover"]
    },
    "cn_sector_em_daily_raw": {
        "load_mode": "matrix",
        "id_col": "sector_name",
        "fields": ["open", "close", "high", "low", "volume", "amount", "pct_change", "turnover"],
        "ffill_cols": ["open", "close", "high", "low"],
        "zerofill_cols": ["volume", "amount", "turnover"]
    },
    "stock_sector_map": {
        "load_mode": "mapping",
        "id_col": "stock_code",
        "val_col": "sector_name"
    },
    # 存储规范：用于 DuckDB COPY 时的类型转换与字段选择
    "sector": [
        "CAST(trade_date AS DATE) AS trade_date",
        "CAST(sector_name AS VARCHAR) AS sector_name",
        "CAST(open AS DOUBLE) AS open",
        "CAST(close AS DOUBLE) AS close",
        "CAST(high AS DOUBLE) AS high",
        "CAST(low AS DOUBLE) AS low",
        "CAST(volume AS BIGINT) AS volume",
        "CAST(amount AS DOUBLE) AS amount",
        "CAST(amplitude AS DOUBLE) AS amplitude",
        "CAST(pct_change AS DOUBLE) AS pct_change",
        "CAST(change_amount AS DOUBLE) AS change_amount",
        "CAST(turnover AS DOUBLE) AS turnover"
    ],
    "stock": [
        "CAST(trade_date AS DATE) AS trade_date",
        "CAST(stock_code AS VARCHAR) AS stock_code",
        "CAST(open AS DOUBLE) AS open",
        "CAST(close AS DOUBLE) AS close",
        "CAST(high AS DOUBLE) AS high",
        "CAST(low AS DOUBLE) AS low",
        "CAST(volume AS BIGINT) AS volume",
        "CAST(amount AS DOUBLE) AS amount",
        "CAST(amplitude AS DOUBLE) AS amplitude",
        "CAST(pct_change AS DOUBLE) AS pct_change",
        "CAST(change_amount AS DOUBLE) AS change_amount",
        "CAST(turnover AS DOUBLE) AS turnover"
    ]
}

class MarketDownloadRequest(BaseModel):
    """
    行情下载请求参数 (统一支持个股与板块)
    """
    symbols: Optional[List[str]] = None  # 为空时自动获取全市场/全板块
    months: List[str] = ["202501"]        # YYYYMM 格式
    source: str = "em"                   # 数据源: em (东财), sina (新浪)
    data_type: str = "sector"            # 数据类型: sector (板块), stock (个股)
    adjust: str = "adj"                  # 复权类型: adj (后复权), raw (不复权). 彻底废弃 qfq

    @field_validator("adjust")
    @classmethod
    def validate_adjust(cls, v):
        if v not in ["adj", "raw"]:
            raise ValueError("adjust 必须为 'adj' (后复权) 或 'raw' (不复权)")
        return v

class MarketQueryRequest(BaseModel):
    """
    多表查询请求参数
    """
    table_names: List[str]
    start_date: date
    end_date: date
    symbols: Optional[List[str]] = None

class TableData:
    """
    平铺数据单元 (Representing a single matrix Field or a Mapping)
    支持标签式查询: data["2024-01-01", "000001"]
    """
    def __init__(self, 
                 name: str, 
                 timeline: Optional[List[str]] = None, 
                 symbols: Optional[List[str]] = None, 
                 data: any = None):
        self.name = name
        self.timeline = timeline  # x-axis (dates)
        self.symbols = symbols    # y-axis (assets)
        self.data = data          # np.ndarray (2D Matrix) or dict (Mapping)
        
        # 索引计算缓存
        self.date_to_idx = {d: i for i, d in enumerate(timeline)} if timeline else {}
        self.symbol_to_idx = {s: i for i, s in enumerate(symbols)} if symbols else {}

    def __getitem__(self, key):
        """
        三元/二元索引支持:
        矩阵轨：data["2024-01-01", "000001"] -> 返回数值
        映射轨：data["000001"] -> 返回板块名
        """
        if isinstance(self.data, dict):
            # 映射轨道访问
            return self.data.get(key)
        
        if isinstance(key, tuple) and len(key) == 2:
            t_label, s_label = key
            t_idx = self.date_to_idx.get(t_label)
            s_idx = self.symbol_to_idx.get(s_label)
            if t_idx is None or s_idx is None:
                return np.nan
            return self.data[t_idx, s_idx]
            
        raise IndexError(f"TableData '{self.name}' 访问方式错误 (Key: {key})")

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "timeline": self.timeline,
            "symbols": self.symbols,
            "data": self.data.tolist() if isinstance(self.data, np.ndarray) else self.data
        }

class MarketDataContainer:
    """
    平铺式容器 (Flat Namespace Container)
    Key 为 {table}_{field} 或 {table}
    """
    def __init__(self, tables: Dict[str, TableData]):
        self.tables = tables

    def __getitem__(self, key: str) -> TableData:
        if key not in self.tables:
            raise KeyError(f"数据节点 '{key}' 未加载")
        return self.tables[key]

    def to_dict(self) -> dict:
        return {name: table.to_dict() for name, table in self.tables.items()}

