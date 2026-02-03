
from datetime import date
from typing import List, Optional
from pydantic import BaseModel

class SectorDownloadRequest(BaseModel):
    """
    板块下载请求参数
    """
    sectors: Optional[List[str]] = None
    start_date: str = "20250101"
    end_date: str = "20250131"

class MarketDataSchema(BaseModel):
    """
    板块/个股日线数据模型
    """
    trade_date: date
    sector_name: str
    open: float
    close: float
    high: float
    low: float
    volume: int
    amount: float
    amplitude: float
    pct_change: float
    change_amount: float
    turnover: float
