import sys
import os
from loguru import logger
from core.config import settings

def setup_logging():
    """Configure loguru logging."""
    # Ensure logs directory exists
    os.makedirs(settings.LOGS_DIR, exist_ok=True)
    
    # Remove default handler
    logger.remove()
    
    # Add console handler (Colorized)
    logger.add(
        sys.stderr,
        level="DEBUG" if settings.DEBUG else "INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    
    # Add file handler (Rotation by size)
    log_file = os.path.join(settings.LOGS_DIR, "carrot_backend.log")
    logger.add(
        log_file,
        rotation="10 MB",
        retention="1 week",
        level="INFO",
        encoding="utf-8"
    )

    logger.info("Logging initialized")
