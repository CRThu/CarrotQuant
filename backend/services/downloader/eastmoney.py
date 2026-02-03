
from typing import List
import akshare as ak
import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from services.downloader.base import BaseDownloader

class EastMoneyDownloader(BaseDownloader):
    """
    Implementation of BaseDownloader using AkShare (EastMoney).
    """
    
    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_fixed(2), 
        retry=retry_if_exception_type(Exception)
    )
    def fetch_sector_daily(self, sector_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            # akshare.stock_board_industry_hist_em returns columns in Chinese
            df = ak.stock_board_industry_hist_em(
                symbol=sector_name,
                start_date=start_date,
                end_date=end_date,
                period="日k",
                adjust="qfq"
            )
            
            # Standardization mapping
            rename_map = {
                "日期": "trade_date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
                "振幅": "amplitude",
                "涨跌幅": "pct_change",
                "涨跌额": "change_amount",
                "换手率": "turnover"
            }
            df = df.rename(columns=rename_map)
            
            # Post-processing
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            df['sector_name'] = sector_name
            return df
        except Exception as e:
            logger.error(f"Error fetching EastMoney data for {sector_name}: {e}")
            raise e

    def get_all_sectors(self) -> List[str]:
        try:
            df = ak.stock_board_industry_name_em()
            return df['板块名称'].tolist()
        except Exception as e:
            logger.error(f"Error fetching EastMoney sector list: {e}")
            return []
