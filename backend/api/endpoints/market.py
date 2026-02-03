
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional

from models.task import DownloadTask
from models.market import SectorDownloadRequest
from services.market_manager import market_manager

router = APIRouter()

# -- Tasks Endpoints --

@router.post("/tasks/sectors", response_model=DownloadTask)
async def create_sector_download_task(request: SectorDownloadRequest):
    """
    Create/Start a background task to download sector data.
    """
    try:
        task_id = await market_manager.start_sector_download_task(request)
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
