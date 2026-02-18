import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
import os

load_dotenv()

@dataclass
class DatabaseConfig:
    """Configuration for Oracle database."""
    host: str
    port: int
    database: str
    user: str
    password: str
    timeout: int

class Config:
    DATABASE_TYPE: str = os.getenv("O_DATABASE_TYPE", "oracle")
    DATABASE_HOST: str = os.getenv("O_DATABASE_HOST")
    DATABASE_USER: str = os.getenv("O_DATABASE_USER")
    DATABASE_PASS: str = os.getenv("O_DATABASE_PASS")
    DATABASE_PORT: int = int(os.getenv("O_DATABASE_PORT", 1521))
    DATABASE_NAME: str = os.getenv("O_DATABASE_NAME")
    DB_TIMEOUT: int = int(os.getenv("O_DB_TIMEOUT", 30))
    REQUEST_TIMEOUT: int = int(os.getenv("O_REQUEST_TIMEOUT", 30))

    CORS_ALLOW_ORIGIN: str = os.getenv("CORS_ALLOW_ORIGIN", "*")
    HOST: str = os.getenv("HOST")
    PORT: int = int(os.getenv("PORT", 8000))
    RELOAD: bool = os.getenv("RELOAD", "false").lower() in ("1", "true", "yes")

    @classmethod
    def get_database_config(cls) -> DatabaseConfig:
        """Create DatabaseConfig from environment variables."""
        return DatabaseConfig(
            host=cls.DATABASE_HOST,
            port=cls.DATABASE_PORT,
            database=cls.DATABASE_NAME,
            user=cls.DATABASE_USER,
            password=cls.DATABASE_PASS,
            timeout=cls.DB_TIMEOUT,
        )

    @classmethod
    def get_allowed_origins(cls) -> list[str]:
        """Parse CORS allowed origins from config."""
        return [o.strip() for o in cls.CORS_ALLOW_ORIGIN.split(",") if o.strip()]