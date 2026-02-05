
import asyncio
import uuid
from datetime import datetime
from calendar import monthrange
from typing import Dict, List, Optional
import pandas as pd
from loguru import logger

from core.config import settings
from core.storage import DuckDBStorage
from services.downloader.base import BaseDownloader
from services.downloader.eastmoney import EastMoneyDownloader
from services.downloader.sina import SinaDownloader
from models.task import DownloadTask, TaskStatus
from models.market import MarketDownloadRequest, MarketTable

class MarketDataManager:
    """行情数据下载与存储协调服务 (重构版: 多源+路由)"""
    
    def __init__(self):
        self.downloaders: Dict[str, BaseDownloader] = {
            "em": EastMoneyDownloader(),
            "sina": SinaDownloader()
        }
        self.storage = DuckDBStorage(settings.DATA_DIR)
        self.tasks: Dict[str, DownloadTask] = {}
        self.stop_events: Dict[str, asyncio.Event] = {}
        
    async def start_market_download_task(self, request: MarketDownloadRequest) -> str:
        """启动市场数据下载任务 (支持全市场自动调度)"""
        task_id = str(uuid.uuid4())
        
        # 1. 路由与验证
        downloader = self.downloaders.get(request.source)
        if not downloader:
            raise ValueError(f"不受支持的数据源: {request.source}")
            
        symbols = request.symbols
        if not symbols:
            # 全市场自动获取
            if request.data_type == "sector":
                logger.info(f"[{request.source}] 未指定板块，准备抓取全市场板块列表...")
                symbols = downloader.get_all_sectors()
            else:
                logger.info(f"[{request.source}] 未指定个股，准备抓取全市场 A 股列表...")
                symbols = downloader.get_all_symbols()
            
        if not symbols:
            raise ValueError("未能获取到下载标的列表")
            
        task = DownloadTask(
            task_id=task_id, 
            status=TaskStatus.PENDING, 
            message=f"[{request.source}/{request.data_type}/{request.adjust}] 计划下载 {len(symbols)} 个标的"
        )
        self.tasks[task_id] = task
        self.stop_events[task_id] = asyncio.Event()
        
        # 2. 启动异步处理
        asyncio.create_task(
            self._run_monthly_split_download(task_id, downloader, symbols, request)
        )
        
        return task_id

    async def _run_monthly_split_download(self, task_id: str, downloader: BaseDownloader, symbols: List[str], request: MarketDownloadRequest):
        task = self.tasks[task_id]
        task.status = TaskStatus.RUNNING
        task.updated_at = datetime.now()
        stop_event = self.stop_events[task_id]
        
        # 路由确定规范化表名: {市场}_{品种}_{来源}_{频率}_{复权}
        table_name = self._route_table_name(request)
        
        logger.info(f"任务 {task_id} 启动 | 源: {request.source} | 类型: {request.data_type} | 复权: {request.adjust} | 标的数: {len(symbols)}")
        
        try:
            total_months = len(request.months)
            for idx, month_str in enumerate(request.months):
                if stop_event.is_set():
                    self._mark_stopped(task)
                    return
                
                # 1. 解析日期
                try:
                    s_str, e_str, year, month = self._get_date_range(month_str)
                except ValueError as e:
                    logger.error(f"日期格式错误 {month_str}: {e}")
                    continue

                # 2. 更新进度状态
                task.message = f"正在下载 {year}年{month}月 ({idx+1}/{total_months})"
                task.updated_at = datetime.now()
                logger.info(f"[{request.source}] 正在处理 {year}-{month} | 目标表: {table_name}")
                
                # 3. 符号循环单元 (透传 adjust)
                monthly_buffer = await self._download_monthly_chunk(
                    downloader, symbols, request.data_type, s_str, e_str, request.adjust, stop_event
                )
                
                # 4. 存储层
                if monthly_buffer:
                    full_df = pd.concat(monthly_buffer)
                    await asyncio.to_thread(
                        self.storage.save_month, 
                        df=full_df, 
                        table_name=table_name, 
                        year=year, 
                        month=month
                    )
                
                task.progress = round(((idx + 1) / total_months) * 100, 2)
            
            task.status = TaskStatus.COMPLETED
            task.message = f"全部下载完成 (共 {total_months} 月)"
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.message = f"任务出错: {str(e)}"
            logger.exception(f"任务 {task_id} 异常")
        finally:
            task.updated_at = datetime.now()
            self.stop_events.pop(task_id, None)

    async def _download_monthly_chunk(self, downloader: BaseDownloader, symbols: List[str], data_type: str, s_str: str, e_str: str, adjust: str, stop_event: asyncio.Event) -> List[pd.DataFrame]:
        """单月标的循环下载逻辑 (带 0.1s 流控)"""
        buffer = []
        for i, symbol in enumerate(symbols):
            if stop_event.is_set():
                break
            
            try:
                logger.debug(f"[{downloader.__class__.__name__}] 抓取 [{data_type}/{adjust}] {symbol} ({s_str}-{e_str})")
                
                if data_type == "sector":
                    df = await asyncio.to_thread(downloader.fetch_sector_daily, symbol, s_str, e_str, adjust)
                else:
                    df = await asyncio.to_thread(downloader.fetch_stock_daily, symbol, s_str, e_str, adjust)
                
                if not df.empty:
                    buffer.append(df)
            except Exception as e:
                logger.error(f"标的 {symbol} 下载失败: {e}")
            
            # 5. 专业流控: 0.1s 延迟
            await asyncio.sleep(0.1)
            
        return buffer

    def _get_date_range(self, month_str: str):
        """YYYYMM -> (s_str, e_str, year, month)"""
        if len(month_str) != 6 or not month_str.isdigit():
            raise ValueError("Format must be YYYYMM")
        year = int(month_str[:4])
        month = int(month_str[4:])
        _, last_day = monthrange(year, month)
        return f"{year}{month:02d}01", f"{year}{month:02d}{last_day}", year, month

    def _route_table_name(self, request: MarketDownloadRequest) -> str:
        """
        根据源、类型、复权动态路由规范表名
        命名规范: {市场}_{品种}_{来源}_{频率}_{复权}
        """
        market = "cn" # 目前主要为中国市场
        freq = "daily"
        return f"{market}_{request.data_type}_{request.source}_{freq}_{request.adjust}"

    def _mark_stopped(self, task):
        task.status = TaskStatus.STOPPED
        task.message = "用户已停止"
        task.updated_at = datetime.now()

    def get_task(self, task_id: str) -> Optional[DownloadTask]:
        return self.tasks.get(task_id)

    def stop_task(self, task_id: str) -> bool:
        if task_id in self.stop_events:
            self.stop_events[task_id].set()
            return True
        return False

market_manager = MarketDataManager()
