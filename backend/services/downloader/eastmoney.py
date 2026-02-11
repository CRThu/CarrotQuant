
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
    def fetch_sector_daily(self, sector_name: str, start_date: str, end_date: str, adjust: str = "adj") -> pd.DataFrame:
        try:
            # 东财板块暂不支持复权选择，默认为 raw(不复权) 处理，但接口保留参数
            df = ak.stock_board_industry_hist_em(
                symbol=sector_name,
                start_date=start_date,
                end_date=end_date,
                period="日k",
                adjust="qfq" if adjust == "adj" else "" # 板块暂无 hfq
            )
            
            # 标准化映射
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
            
            # 后处理
            df['volume'] = df['volume'].astype("float64") * 100.0 # 统一单位为“股”
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            df['sector_name'] = sector_name
            return df
        except Exception as e:
            logger.error(f"Error fetching EastMoney sector data for {sector_name}: {e}")
            raise e

    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_fixed(2), 
        retry=retry_if_exception_type(Exception)
    )
    def fetch_stock_daily(self, symbol: str, start_date: str, end_date: str, adjust: str = "adj") -> pd.DataFrame:
        try:
            # 映射 CarrotQuant 术语到 AkShare 术语
            # adj -> hfq (后复权), raw -> "" (不复权)
            ak_adjust = "hfq" if adjust == "adj" else ""
            
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=ak_adjust
            )
            
            # 标准化映射
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
            
            # 后处理
            df['volume'] = df['volume'].astype("float64") * 100.0 # 统一单位为“股”
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            df['stock_code'] = symbol
            return df
        except Exception as e:
            logger.error(f"Error fetching EastMoney stock data for {symbol} (adjust={adjust}): {e}")
            raise e

    def get_all_sectors(self) -> List[str]:
        try:
            df = ak.stock_board_industry_name_em()
            return df['板块名称'].tolist()
        except Exception as e:
            logger.error(f"Error fetching EastMoney sector list: {e}")
            return []

    def get_all_symbols(self) -> List[str]:
        """获取全量 A 股代码"""
        try:
            df = ak.stock_zh_a_spot_em()
            return df['代码'].tolist()
        except Exception as e:
            logger.error(f"Error fetching EastMoney all symbols: {e}")
            return []
