
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
                adjust="hfq" if adjust == "adj" else ""
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
            
            # 严格筛选：仅保留在 DATA_SCHEMA 中定义的字段
            from models.market import DATA_SCHEMA
            valid_cols = [c for c in df.columns if c in DATA_SCHEMA]
            return df[valid_cols]
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
            
            # 严格筛选：仅保留在 DATA_SCHEMA 中定义的字段
            from models.market import DATA_SCHEMA
            valid_cols = [c for c in df.columns if c in DATA_SCHEMA]
            return df[valid_cols]
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

    def fetch_stock_info(self) -> pd.DataFrame:
        """
        获取 A 股基础信息快照 (代码、名称)
        严禁包含任何价格、成交量等行情字段
        """
        try:
            df = ak.stock_zh_a_spot_em()
            # 仅保留基础字段
            df = df[['代码', '名称']]
            df.columns = ['stock_code', 'stock_name']
            return df
        except Exception as e:
            logger.error(f"Error fetching stock info: {e}")
            return pd.DataFrame()

    def fetch_sector_info(self) -> pd.DataFrame:
        """
        获取行业板块基础信息
        """
        try:
            df = ak.stock_board_industry_name_em()
            df = df[['板块名称']]
            df.columns = ['sector_name']
            return df
        except Exception as e:
            logger.error(f"Error fetching sector info: {e}")
            return pd.DataFrame()

    def fetch_concept_info(self) -> pd.DataFrame:
        """
        获取概念板块基础信息
        """
        try:
            df = ak.stock_board_concept_name_em()
            df = df[['板块名称']]
            df.columns = ['concept_name']
            return df
        except Exception as e:
            logger.error(f"Error fetching concept info: {e}")
            return pd.DataFrame()

    async def fetch_stock_sector_map(self) -> pd.DataFrame:
        """
        获取股票与行业的映射关系 (一对一)
        """
        try:
            sectors = self.get_all_sectors()
            results = []
            for i, s in enumerate(sectors):
                 logger.debug(f"[EastMoney] 正在抓取行业成员 ({i+1}/{len(sectors)}): {s}")
                 cons = ak.stock_board_industry_cons_em(symbol=s)
                 if cons.empty:
                     continue
                 cons = cons[['代码', '名称']].copy()
                 cons['sector_name'] = s
                 results.append(cons)
                 # 流控避让
                 await asyncio.sleep(0.1)
            
            if not results:
                return pd.DataFrame()
                
            full_df = pd.concat(results)
            full_df = full_df.rename(columns={'代码': 'stock_code'})
            # 严肃过滤：仅保留代码和行业名称，剔除任何行情字段
            return full_df[['stock_code', 'sector_name']].drop_duplicates()
        except Exception as e:
            logger.error(f"Error fetching stock-sector map: {e}")
            return pd.DataFrame()
