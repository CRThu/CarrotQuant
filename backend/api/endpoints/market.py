
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional

from models.task import DownloadTask
from models.task import DownloadTask
from models.market import MarketDownloadRequest, MarketQueryRequest
from services.market_manager import market_manager
from services.data import data_manager
from core.exceptions import DataNotFoundError

router = APIRouter()

# -- Tasks Endpoints --

@router.post("/tasks/download", response_model=DownloadTask)
async def create_market_download_task(request: MarketDownloadRequest):
    """
    创建/启动行情下载后台任务。
    """
    try:
        task_id = await market_manager.start_market_download_task(request)
        task = market_manager.get_task(task_id)
        if not task:
             raise HTTPException(status_code=500, detail="Task creation failed")
        return task
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/{task_id}", response_model=DownloadTask)
async def get_task_status(task_id: str):
    """
    Get the status of a download task.
    """
    task = market_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@router.post("/tasks/{task_id}/stop")
async def stop_task(task_id: str):
    """
    Stop a running task.
    """
    success = market_manager.stop_task(task_id)
    if not success:
        raise HTTPException(status_code=404, detail="Task not found or already stopped")
    return {"message": "Task stop signal sent"}

@router.get("/tasks", response_model=List[DownloadTask])
async def get_all_tasks():
    """
    获取所有活跃和历史任务列表。
    """
    return market_manager.get_all_tasks()

# -- Data Access Endpoints --

@router.get("/registry")
async def get_market_registry():
    """
    获取系统中所有已注册的数据表配置。
    """
    from models.market import TABLE_REGISTRY
    return TABLE_REGISTRY

@router.get("/tables")
async def get_market_tables():
    """
    Get list of available market data tables and their metadata (years).
    """
    return data_manager.get_storage_metadata()

@router.post("/query")
async def query_market_data(request: MarketQueryRequest):
    """
    Query multidimensional market data.
    """
    try:
        # Pydantic validates start_date/end_date as date objects
        container = data_manager.load_market_data(
            table_names=request.table_names,
            start_date=request.start_date,
            end_date=request.end_date,
            symbols=request.symbols
        )
        return container.to_dict()
    except DataNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        # Log error in real app
        raise HTTPException(status_code=500, detail=str(e))
