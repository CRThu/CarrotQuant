import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "CarrotQuant Backend"
    API_V1_STR: str = "/api/v1"
    
    # Paths
    # BASE_DIR = backend/
    BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # REPO_ROOT = CarrotQuant/
    REPO_ROOT: str = os.path.dirname(BASE_DIR)
    
    # Config file path (CarrotQuant/.carrotquant/.env)
    DOT_CARROT_DIR: str = os.path.join(REPO_ROOT, ".carrotquant")
    
    # Logs & Data defaults
    LOGS_DIR: str = os.path.join(REPO_ROOT, "logs")
    DATA_DIR: str = os.path.join(REPO_ROOT, "data")

    # App Settings
    DEBUG: bool = True
    HOST: str = "127.0.0.1"
    PORT: int = 8000
    
    # Secrets / Tokens
    FRED_API_KEY: str | None = None

    model_config = SettingsConfigDict(
        env_file=(
            os.path.join(DOT_CARROT_DIR, ".env"),
            os.path.join(DOT_CARROT_DIR, "secrets.env"),
        ),
        env_ignore_empty=True,
        extra="ignore"
    )

settings = Settings()
