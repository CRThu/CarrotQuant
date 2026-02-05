
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
    def get_all_sectors(self) -> List[str]:
        """
        获取所有板块列表。
        """
        pass

    @abstractmethod
    def get_all_symbols(self) -> List[str]:
        """
        获取所有股票代码列表。
        """
        pass
