
from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Optional

class BaseDownloader(ABC):
    """
    Abstract interface for market data downloaders.
    """
    
    @abstractmethod
    def fetch_sector_daily(self, sector_name: str, start_date: str, end_date: str, adjust: str = "adj") -> pd.DataFrame:
        """
        Fetch daily data for a sector.
        Args:
            sector_name: Name of the sector.
            start_date: YYYYMMDD
            end_date: YYYYMMDD
            adjust: adj (后复权), raw (原始)
        Returns:
            DataFrame with standardized columns.
        """
        pass
        
    @abstractmethod
    def fetch_stock_daily(self, symbol: str, start_date: str, end_date: str, adjust: str = "adj") -> pd.DataFrame:
        """
        获取个股日线数据。
        Args:
            symbol: 股票代码 (如 000001)
            start_date: YYYYMMDD
            end_date: YYYYMMDD
            adjust: adj (后复权), raw (原始)
        Returns:
            标准化列名的 DataFrame。
        """
        pass

    @abstractmethod
    def fetch_stock_info(self) -> pd.DataFrame:
        """获取 A 股基础信息快照"""
        pass

    @abstractmethod
    def fetch_sector_info(self) -> pd.DataFrame:
        """获取行业板块基础信息"""
        pass

    @abstractmethod
    def fetch_concept_info(self) -> pd.DataFrame:
        """获取概念板块基础信息"""
        pass

    @abstractmethod
    async def fetch_stock_sector_map(self) -> pd.DataFrame:
        """获取股票与行业的映射关系"""
        pass
