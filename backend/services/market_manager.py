
import asyncio
import uuid
from datetime import datetime
from calendar import monthrange
from typing import Dict, List, Optional
import pandas as pd
from loguru import logger

from core.config import settings
from core.storage import DuckDBStorage
from services.downloader.eastmoney import EastMoneyDownloader
from models.task import DownloadTask, TaskStatus
from models.market import SectorDownloadRequest

class MarketDataManager:
    """行情数据下载与存储协调服务 (按月调度版)"""
    
    def __init__(self):
        self.downloader = EastMoneyDownloader()
        self.storage = DuckDBStorage(settings.DATA_DIR)
        self.tasks: Dict[str, DownloadTask] = {}
        self.stop_events: Dict[str, asyncio.Event] = {}
        
    async def start_sector_download_task(self, request: SectorDownloadRequest) -> str:
        """启动板块下载任务 (按月拆分)"""
        task_id = str(uuid.uuid4())
        
        sectors = request.sectors
        if not sectors:
            sectors = self.downloader.get_all_sectors()
            
        task = DownloadTask(
            task_id=task_id, 
            status=TaskStatus.PENDING, 
            message=f"计划下载 {len(sectors)} 个板块"
        )
        self.tasks[task_id] = task
        self.stop_events[task_id] = asyncio.Event()
        
        asyncio.create_task(
            self._run_monthly_split_download(task_id, sectors, request.start_date, request.end_date)
        )
        
        return task_id

    def _generate_month_ranges(self, start_date: str, end_date: str):
        """生成月份区间列表 [(2025, 1, '20250101', '20250131'), ...]"""
        try:
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
        except:
             # Fallback
            return []
            
        current = start.replace(day=1) 
        ranges = []
        
        while current <= end:
            year = current.year
            month = current.month
            
            # 计算当月最后一天
            _, last_day = monthrange(year, month)
            month_end = current.replace(day=last_day)
            
            # 强制使用整月区间 (1号到最后一天)
            # 配合 "当月全量覆盖" 策略，防止因下载部分日期导致覆盖后数据缺失
            s_str = current.strftime('%Y%m%d')
            e_str = month_end.strftime('%Y%m%d')
            
            ranges.append((year, month, s_str, e_str))
            
            # 下个月
            if month == 12:
                current = current.replace(year=year+1, month=1)
            else:
                current = current.replace(month=month+1)
        
        return ranges
        
    async def _run_monthly_split_download(self, task_id: str, sectors: List[str], start_date: str, end_date: str):
        task = self.tasks[task_id]
        task.status = TaskStatus.RUNNING
        task.updated_at = datetime.now()
        stop_event = self.stop_events[task_id]
        
        logger.info(f"任务 {task_id} 启动: {start_date} -> {end_date}")
        
        try:
            month_list = self._generate_month_ranges(start_date, end_date)
            total_months = len(month_list)
            
            for idx, (year, month, s_str, e_str) in enumerate(month_list):
                if stop_event.is_set():
                    self._mark_stopped(task)
                    return
                
                # 更新状态
                task.message = f"正在下载 {year}年{month}月 ({idx+1}/{total_months})"
                task.updated_at = datetime.now()
                
                logger.info(f"[{year}-{month}] 下载范围: {s_str} - {e_str}, 板块数: {len(sectors)}")
                
                # --- 单月处理逻辑 ---
                monthly_buffer = []
                
                # 并发/循环下载该月所有板块
                # 为了响应停止信号，我们在循环中 Check
                for i_sec, sector in enumerate(sectors):
                    if stop_event.is_set():
                        self._mark_stopped(task)
                        return
                    
                    try:
                        df = await asyncio.to_thread(
                            self.downloader.fetch_sector_daily, sector, s_str, e_str
                        )
                        if not df.empty:
                            monthly_buffer.append(df)
                    except Exception as e:
                        logger.error(f"板块 {sector} 下载失败 ({year}-{month}): {e}")
                        continue
                        
                    # 细粒度进度更新 (可选)
                    # task.progress = ...
                
                # 本月存储
                if monthly_buffer:
                    full_df = pd.concat(monthly_buffer)
                    await asyncio.to_thread(
                        self.storage.save_month, 
                        df=full_df, 
                        table_name="dfcft-a-bk", 
                        year=year, 
                        month=month
                    )
                
                # 更新大进度
                task.progress = round(((idx + 1) / total_months) * 100, 2)
            
            task.status = TaskStatus.COMPLETED
            task.message = "全部下载完成"
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.message = f"任务出错: {str(e)}"
            logger.exception(f"任务 {task_id} 异常")
        finally:
            task.updated_at = datetime.now()
            self.stop_events.pop(task_id, None)

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
