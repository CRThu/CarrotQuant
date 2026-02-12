
import akshare as ak
import pandas as pd
from loguru import logger
from typing import List
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from services.downloader.base import BaseDownloader

class SinaDownloader(BaseDownloader):
    """
    新浪数据下载器实现。
    """
    
    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_fixed(2), 
        retry=retry_if_exception_type(Exception)
    )
    def fetch_stock_daily(self, symbol: str, start_date: str, end_date: str, adjust: str = "adj") -> pd.DataFrame:
        try:
            # 映射复权逻辑
            # 注意：ak.stock_zh_a_daily 原生支持 qfq, hfq
            ak_adjust = "hfq" if adjust == "adj" else ""
            
            df = ak.stock_zh_a_daily(
                symbol=f"sh{symbol}" if symbol.startswith('6') else f"sz{symbol}",
                start_date=start_date,
                end_date=end_date,
                adjust=ak_adjust if ak_adjust else "no" # ak 接口要求 'no' 代表不复权
            )
            
            # 映射 CarrotQuant 术语
            rename_map = {
                "date": "trade_date",
                "open": "open",
                "close": "close",
                "high": "high",
                "low": "low",
                "volume": "volume",
                "amount": "amount",
                "turnover": "turnover",
                "outstanding_share": "outstanding_share"
            }
            df = df.rename(columns=rename_map)
            
            # 后处理
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            df['stock_code'] = symbol
            return df
        except Exception as e:
            logger.error(f"Error fetching Sina stock data for {symbol} (adjust={adjust}): {e}")
            raise e

    def fetch_sector_daily(self, sector_name: str, start_date: str, end_date: str, adjust: str = "adj") -> pd.DataFrame:
        # 新浪暂不支持板块数据，返回空
        logger.warning("SinaDownloader does not support sector data.")
        return pd.DataFrame()

    def fetch_stock_info(self) -> pd.DataFrame:
        """Sina 暂不作为基础信息源"""
        logger.warning("SinaDownloader does not support fetch_stock_info.")
        return pd.DataFrame()

    def fetch_sector_info(self) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_concept_info(self) -> pd.DataFrame:
        return pd.DataFrame()

    async def fetch_stock_sector_map(self) -> pd.DataFrame:
        return pd.DataFrame()
