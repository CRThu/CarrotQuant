
from datetime import datetime
from pydantic import BaseModel, Field

class TaskStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"

class DownloadTask(BaseModel):
    """
    通用下载任务状态模型
    """
    task_id: str
    task_type: str = "sector_download"
    status: str
    progress: float = 0.0
    message: str = ""
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
