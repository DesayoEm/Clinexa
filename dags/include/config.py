from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
load_dotenv()

columns_to_read = ["studies.protocolSection"]

class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DATABASE_URL: str
    SHARD_STORAGE_DIR: str
    COMPACTED_STORAGE_DIR: str
    STATE_MGT_DIR: str
    BASE_URL: str
    PAGES_BASE_URL: str
    COMPOSE_FILE: str = "docker-compose.yml"
    COLUMNS_TO_READ: List  = columns_to_read
    DBT_DIR: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

config = Settings()
