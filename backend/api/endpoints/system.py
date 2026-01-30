from fastapi import APIRouter
from core.config import settings

router = APIRouter()

@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "ok",
        "app_name": settings.PROJECT_NAME,
        "debug_mode": settings.DEBUG
    }
