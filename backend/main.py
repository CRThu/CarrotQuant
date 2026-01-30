from contextlib import asynccontextmanager
from fastapi import FastAPI
from loguru import logger

from core.config import settings
from core.logging import setup_logging
from api.main import api_router
from services.scheduler import scheduler
from services.data import data_manager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    setup_logging()
    logger.info("Starting CarrotQuant Backend...")
    
    # Initialize services
    data_manager.initialize()
    scheduler.start()
    
    yield
    
    # Shutdown
    logger.info("Shutting down CarrotQuant Backend...")
    scheduler.stop()

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan
)

# Include API router
app.include_router(api_router, prefix=settings.API_V1_STR)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app", 
        host=settings.HOST, 
        port=settings.PORT, 
        reload=settings.DEBUG
    )