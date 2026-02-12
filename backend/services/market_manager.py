
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
        """启动市场数据下载任务 (完全由 TABLE_REGISTRY 驱动)"""
        task_id = str(uuid.uuid4())
        
        # 1. 验证表是否存在
        from models.market import TABLE_REGISTRY
        if request.table_name not in TABLE_REGISTRY:
            raise ValueError(f"不受支持的表名: {request.table_name}")
            
        config = TABLE_REGISTRY[request.table_name]
        download_cfg = config.get("download_config")
        if not download_cfg:
            raise ValueError(f"表 {request.table_name} 未配置下载参数")

        # 2. 获取下载器与配置
        source = download_cfg["source"]
        downloader = self.downloaders.get(source)
        if not downloader:
            raise ValueError(f"不受支持的数据源: {source}")
            
        storage_type = config.get("storage_type", "partition")
        symbols = request.symbols
        
        # 3. 自动计算 Symbols (如果未指定且为分区表)
        if not symbols and storage_type == "partition":
             if "sector" in request.table_name:
                 symbols = downloader.get_all_sectors()
             else:
                 symbols = downloader.get_all_symbols()

        task = DownloadTask(
            task_id=task_id, 
            status=TaskStatus.PENDING, 
            message=f"[{request.table_name}] 计划启动任务 (Storage: {storage_type})"
        )
        self.tasks[task_id] = task
        self.stop_events[task_id] = asyncio.Event()
        
        # 4. 驱动异步处理
        if storage_type == "snapshot":
            asyncio.create_task(
                self._run_snapshot_download(task_id, downloader, request.table_name, download_cfg)
            )
        else:
            asyncio.create_task(
                self._run_partition_download(task_id, downloader, symbols, request.table_name, download_cfg, request.months)
            )
        
        return task_id

    async def _run_snapshot_download(self, task_id: str, downloader: BaseDownloader, table_name: str, download_cfg: dict):
        """执行快照下载"""
        task = self.tasks[task_id]
        task.status = TaskStatus.RUNNING
        task.updated_at = datetime.now()
        
        try:
            handler_name = download_cfg["handler"]
            handler = getattr(downloader, handler_name)
            
            task.message = f"正在拉取 {table_name} 全量快照..."
<<<<<<< Updated upstream
            # 兼容异步/同步方法 (如 fetch_stock_sector_map 是 async)
            if asyncio.iscoroutinefunction(handler):
                df = await handler()
            else:
                df = await asyncio.to_thread(handler)
            
            if df.empty:
                raise ValueError(f"下载器返回数据为空")
=======
            # 闭包用于实时回传进度
            def progress_updater(p, msg):
                task.progress = p
                task.message = msg

            # 兼容异步/同步方法 (如 fetch_stock_sector_map 是 async)
            if asyncio.iscoroutinefunction(handler):
                df = await handler(progress_callback=progress_updater)
            else:
                # 同步方法若支持 progress_callback 则传递，否则维持原状
                # 目前主要针对 mapping 类任务
                import inspect
                sig = inspect.signature(handler)
                if "progress_callback" in sig.parameters:
                    df = await asyncio.to_thread(handler, progress_callback=progress_updater)
                else:
                    df = await asyncio.to_thread(handler)
            
            if df.empty:
                raise ValueError(f"[{table_name}] 下载器返回数据为空")
>>>>>>> Stashed changes

            await asyncio.to_thread(self.storage.save_snapshot, df=df, table_name=table_name)
            
            task.progress = 100.0
            task.status = TaskStatus.COMPLETED
            task.message = f"快照下载完成"
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.message = f"任务出错: {str(e)}"
            logger.exception(f"任务 {task_id} 异常")
        finally:
            task.updated_at = datetime.now()
            self.stop_events.pop(task_id, None)

    async def _run_partition_download(self, task_id: str, downloader: BaseDownloader, symbols: List[str], table_name: str, download_cfg: dict, months: List[str]):
        """执行分区表下载"""
        task = self.tasks[task_id]
        task.status = TaskStatus.RUNNING
        task.updated_at = datetime.now()
        stop_event = self.stop_events[task_id]
        
        if not months:
            months = [datetime.now().strftime("%Y%m")]

        try:
            handler_name = download_cfg["handler"]
            adjust = download_cfg.get("adjust", "raw")
            total_months = len(months)

            for idx, month_str in enumerate(months):
                if stop_event.is_set():
                    self._mark_stopped(task)
                    return
                
                s_str, e_str, year, month = self._get_date_range(month_str)
                task.message = f"正在下载 {year}年{month}月 ({idx+1}/{total_months})"
                task.updated_at = datetime.now()
                
                # 符号循环抓取
                monthly_buffer = []
                for i, symbol in enumerate(symbols):
                    if stop_event.is_set(): break
                    try:
                        handler = getattr(downloader, handler_name)
                        df = await asyncio.to_thread(handler, symbol, s_str, e_str, adjust)
                        if not df.empty: monthly_buffer.append(df)
                    except Exception as e:
                        logger.error(f"标的 {symbol} 下载失败: {e}")
                    await asyncio.sleep(0.1)

                if monthly_buffer:
                    full_df = pd.concat(monthly_buffer)
                    await asyncio.to_thread(self.storage.save_month, df=full_df, table_name=table_name, year=year, month=month)
                
                task.progress = round(((idx + 1) / total_months) * 100, 2)
            
            task.status = TaskStatus.COMPLETED
            task.message = "分区数据下载完成"
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.message = f"任务出错: {str(e)}"
            logger.exception(f"任务 {task_id} 异常")
        finally:
            task.updated_at = datetime.now()
            self.stop_events.pop(task_id, None)

    def _get_date_range(self, month_str: str):
        """YYYYMM -> (s_str, e_str, year, month)"""
        if len(month_str) != 6 or not month_str.isdigit():
            raise ValueError("Format must be YYYYMM")
        year = int(month_str[:4])
        month = int(month_str[4:])
        _, last_day = monthrange(year, month)
        return f"{year}{month:02d}01", f"{year}{month:02d}{last_day}", year, month


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

    def get_all_tasks(self) -> List[DownloadTask]:
        """返回所有任务列表"""
        return list(self.tasks.values())

market_manager = MarketDataManager()
