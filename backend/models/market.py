
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

# 全局 Schema 权威注册中心：定义系统中所有可能出现字段的“权威类型”
DATA_SCHEMA = {
    "trade_date": "DATE",           # 交易日期
    "stock_code": "VARCHAR",        # 股票代码
    "stock_name": "VARCHAR",        # 股票名称
    "sector_name": "VARCHAR",       # 板块名称
    "concept_name": "VARCHAR",      # 概念名称
    "industry_name": "VARCHAR",     # 行业名称
    "open": "DOUBLE",               # 开盘价
    "close": "DOUBLE",              # 收盘价
    "high": "DOUBLE",               # 最高价
    "low": "DOUBLE",                # 最低价
    "volume": "DOUBLE",             # 成交量 (单位: 股)
    "amount": "DOUBLE",             # 成交额 (单位: 元)
    "amplitude": "DOUBLE",          # 振幅 (%)
    "pct_change": "DOUBLE",         # 涨跌幅 (%)
    "change_amount": "DOUBLE",      # 涨跌额
    "turnover": "DOUBLE",           # 换手率 (%)
    "outstanding_share": "DOUBLE"   # 流通股本 (股)
}

# 全局模型注册表：定义 DuckDB 加载模式、存储类型以及下载配置
# load_mode: matrix (行情矩阵), mapping (字段映射)
# storage_type: snapshot (快照), partition (分区)
TABLE_REGISTRY = {
    # --- 分区表 (Partition Tables) ---
    "cn_stock_em_daily_adj": {
        "load_mode": "matrix",
        "storage_type": "partition",
        "id_col": "stock_code",
        "fields": ["open", "close", "high", "low", "volume", "amount", "amplitude", "pct_change", "change_amount", "turnover"],
        "ffill_cols": ["open", "close", "high", "low"],
        "zerofill_cols": ["volume", "amount", "turnover"],
        "download_config": {"source": "em", "handler": "fetch_stock_daily", "adjust": "adj"}
    },
    "cn_stock_em_daily_raw": {
        "load_mode": "matrix",
        "storage_type": "partition",
        "id_col": "stock_code",
        "fields": ["open", "close", "high", "low", "volume", "amount", "amplitude", "pct_change", "change_amount", "turnover"],
        "ffill_cols": ["open", "close", "high", "low"],
        "zerofill_cols": ["volume", "amount", "turnover"],
        "download_config": {"source": "em", "handler": "fetch_stock_daily", "adjust": "raw"}
    },
    "cn_stock_sina_daily_adj": {
        "load_mode": "matrix",
        "storage_type": "partition",
        "id_col": "stock_code",
        "fields": ["open", "close", "high", "low", "volume", "amount", "outstanding_share", "turnover"],
        "ffill_cols": ["open", "close", "high", "low"],
        "zerofill_cols": ["volume", "amount", "turnover"],
        "download_config": {"source": "sina", "handler": "fetch_stock_daily", "adjust": "adj"}
    },
    "cn_stock_sina_daily_raw": {
        "load_mode": "matrix",
        "storage_type": "partition",
        "id_col": "stock_code",
        "fields": ["open", "close", "high", "low", "volume", "amount", "outstanding_share", "turnover"],
        "ffill_cols": ["open", "close", "high", "low"],
        "zerofill_cols": ["volume", "amount", "turnover"],
        "download_config": {"source": "sina", "handler": "fetch_stock_daily", "adjust": "raw"}
    },
    "cn_sector_em_daily_raw": {
        "load_mode": "matrix",
        "storage_type": "partition",
        "id_col": "sector_name",
        "fields": ["open", "close", "high", "low", "volume", "amount", "amplitude", "pct_change", "change_amount", "turnover"],
        "ffill_cols": ["open", "close", "high", "low"],
        "zerofill_cols": ["volume", "amount", "turnover"],
        "download_config": {"source": "em", "handler": "fetch_sector_daily", "adjust": "raw"}
    },
    "cn_sector_em_daily_adj": {
        "load_mode": "matrix",
        "storage_type": "partition",
        "id_col": "sector_name",
        "fields": ["open", "close", "high", "low", "volume", "amount", "amplitude", "pct_change", "change_amount", "turnover"],
        "ffill_cols": ["open", "close", "high", "low"],
        "zerofill_cols": ["volume", "amount", "turnover"],
        "download_config": {"source": "em", "handler": "fetch_sector_daily", "adjust": "adj"}
    },
    "stock_sector_map": {
        "load_mode": "mapping",
        "storage_type": "partition",
        "id_col": "stock_code",
        "val_col": "sector_name",
        "download_config": {"source": "em", "handler": "fetch_stock_sector_map"}
    },
    # --- 快照表 (Snapshot Tables) ---
    "cn_stock_em": {
        "load_mode": "mapping",
        "storage_type": "snapshot",
        "id_col": "stock_code",
        "val_col": "stock_name",
        "download_config": {"source": "em", "handler": "fetch_stock_info"}
    },
    "cn_sector_em": {
        "load_mode": "mapping",
        "storage_type": "snapshot",
        "id_col": "sector_name",
        "val_col": "sector_name",
        "download_config": {"source": "em", "handler": "fetch_sector_info"}
    },
    "cn_concept_em": {
        "load_mode": "mapping",
        "storage_type": "snapshot",
        "id_col": "concept_name",
        "val_col": "concept_name",
        "download_config": {"source": "em", "handler": "fetch_concept_info"}
    }
}

class MarketDownloadRequest(BaseModel):
    """
    行情下载请求参数 (以表名为核心)
    """
    table_name: str                      # 目标表名 (如: cn_stock_em_daily_adj)
    symbols: Optional[List[str]] = None  # 为空时自动获取全市场/全板块
    months: Optional[List[str]] = None   # YYYYMM 格式 (Snapshot 表可为空)

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

    def get_value(self, key, default=None):
        """
        获取单值 (用于 1-to-1 映射)
        """
        if not isinstance(self.data, dict):
            return default
        return self.data.get(key, default)

    def get_list(self, key) -> list:
        """
        获取列表 (用于 1-to-many 映射)
        确保永不返回 None，方便直接 for 循环
        """
        if not isinstance(self.data, dict):
            return []
        res = self.data.get(key, [])
        return res if isinstance(res, list) else [res]

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

