
from datetime import date
from typing import List, Optional
from pydantic import BaseModel, field_validator

class MarketTable:
    """数据表名常量 (SQL 兼容命名: {市场}_{品种}_{来源}_{频率}_{复权})"""
    CN_STOCK_EM_DAILY_ADJ = "cn_stock_em_daily_adj"     # 东财个股-日线-后复权
    CN_STOCK_EM_DAILY_RAW = "cn_stock_em_daily_raw"     # 东财个股-日线-原始
    CN_SECTOR_EM_DAILY_ADJ = "cn_sector_em_daily_adj"   # 东财板块-日线-后复权 (通常板块不复权，但统一结构)
    CN_SECTOR_EM_DAILY_RAW = "cn_sector_em_daily_raw"   # 东财板块-日线-原始
    CN_STOCK_SINA_DAILY_ADJ = "cn_stock_sina_daily_adj" # 新浪个股-日线-后复权

# 全局 Schema 注册表：定义 DuckDB CAST SQL 字段
# 确保所有来源的数据在磁盘上物理结构一致
# 注意：Parquet 内部不含 year 列，由 Hive 分区路径提供
TABLE_REGISTRY = {
    "sector": [
        "CAST(trade_date AS DATE) AS trade_date",
        "CAST(sector_name AS VARCHAR) AS sector_name",
        "CAST(open AS DOUBLE) AS open",
        "CAST(close AS DOUBLE) AS close",
        "CAST(high AS DOUBLE) AS high",
        "CAST(low AS DOUBLE) AS low",
        "CAST(volume AS UBIGINT) AS volume",
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
        "CAST(volume AS UBIGINT) AS volume",
        "CAST(amount AS DOUBLE) AS amount",
        "CAST(amplitude AS DOUBLE) AS amplitude",
        "CAST(pct_change AS DOUBLE) AS pct_change",
        "CAST(change_amount AS DOUBLE) AS change_amount",
        "CAST(turnover AS DOUBLE) AS turnover"
    ]
}

class MarketDownloadRequest(BaseModel):
    """
    行情下载请求参数 (支持全市场自动调度)
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

class MarketDataSchema(BaseModel):
    """
    通用日线数据模型 (仅用于参考/文档)
    """
    trade_date: date
