
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

    def _filter_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        [防火墙] 根据 DATA_SCHEMA 严格过滤列
        剔除所有未注册的 rec, 序号, index 等冗余列
        """
        if df.empty:
            return df
            
        from models.market import DATA_SCHEMA
        # 1. 仅保留在 Schema 中定义的列
        valid_cols = [c for c in df.columns if c in DATA_SCHEMA]
        
        # 2. 如果没有任何有效列，返回空 DF (防止写入全空表)
        if not valid_cols:
            logger.warning(f"数据清洗后无有效列 (原列: {df.columns.tolist()})")
            return pd.DataFrame()
            
        return df[valid_cols]

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
            return self._filter_schema(df)
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
            return self._filter_schema(df)
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
            return self._filter_schema(df)
        except Exception as e:
            logger.error(f"Error fetching concept info: {e}")
            return pd.DataFrame()

    async def fetch_stock_sector_map(self, progress_callback=None) -> pd.DataFrame:
        """
        获取股票与行业的映射关系 (一对多)
        鲁棒循环：前置审计 + 异常隔离 + info 级进度日志
        """
        import asyncio
        sectors = self.get_all_sectors()
        if len(sectors) == 0:
            raise RuntimeError("行业板块列表为空，无法抓取映射关系")

        results = []
        failed = []
        total = len(sectors)
        
        for i, s in enumerate(sectors):
            msg = f"正在抓取行业成员 ({i+1}/{total}): {s}"
            logger.info(msg)
            if progress_callback:
                progress_callback(round((i + 1) / total * 100, 2), msg)
                
            try:
                cons = ak.stock_board_industry_cons_em(symbol=s)
                if cons.empty:
                    continue
                # 保留代码和名称
                cons = cons[['代码', '名称']].copy()
                cons.columns = ['stock_code', 'stock_name']
                cons['sector_name'] = s
                results.append(cons)
            except Exception as e:
                logger.warning(f"抓取行业 '{s}' 成员失败: {e}")
                failed.append(s)
                continue
            await asyncio.sleep(0.1)

        if failed:
            logger.warning(f"共 {len(failed)} 个行业抓取失败: {failed[:10]}")
        if not results:
            return pd.DataFrame()

        full_df = pd.concat(results)
        # 去重并清洗
        full_df = full_df.drop_duplicates()
        return self._filter_schema(full_df)

    async def fetch_stock_concept_map(self, progress_callback=None) -> pd.DataFrame:
        """
        获取股票与概念的映射关系 (一对多)
        鲁棒循环：前置审计 + 异常隔离 + info 级进度日志
        """
        import asyncio
        try:
            concept_df = ak.stock_board_concept_name_em()
            concepts = concept_df['板块名称'].tolist()
        except Exception as e:
            raise RuntimeError(f"获取概念列表失败: {e}")

        if len(concepts) == 0:
            raise RuntimeError("概念板块列表为空，无法抓取映射关系")

        results = []
        failed = []
        total = len(concepts)
        
        for i, c in enumerate(concepts):
            msg = f"正在抓取概念成员 ({i+1}/{total}): {c}"
            logger.info(msg)
            if progress_callback:
                progress_callback(round((i + 1) / total * 100, 2), msg)
                
            try:
                cons = ak.stock_board_concept_cons_em(symbol=c)
                if cons.empty:
                    continue
                # 保留代码和名称
                cons = cons[['代码', '名称']].copy()
                cons.columns = ['stock_code', 'stock_name']
                cons['concept_name'] = c
                results.append(cons)
            except Exception as e:
                logger.warning(f"抓取概念 '{c}' 成员失败: {e}")
                failed.append(c)
                continue
            await asyncio.sleep(0.1)

        if failed:
            logger.warning(f"共 {len(failed)} 个概念抓取失败: {failed[:10]}")
        if not results:
            return pd.DataFrame()

        full_df = pd.concat(results)
        # 去重并清洗
        full_df = full_df.drop_duplicates()
        return self._filter_schema(full_df)
