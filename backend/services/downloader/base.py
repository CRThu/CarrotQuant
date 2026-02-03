
from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Optional

class BaseDownloader(ABC):
    """
    Abstract interface for market data downloaders.
    """
    
    @abstractmethod
    def fetch_sector_daily(self, sector_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch daily data for a sector.
        Args:
            sector_name: Name of the sector.
            start_date: YYYYMMDD
            end_date: YYYYMMDD
        Returns:
            DataFrame with standardized columns.
        """
        pass
        
    @abstractmethod
    def get_all_sectors(self) -> List[str]:
        """
        Get list of all available sectors.
        """
        pass
