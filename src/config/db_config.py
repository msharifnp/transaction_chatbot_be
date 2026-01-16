import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

@dataclass
class DatabaseConfig:
    """Configuration for SQL Server database."""
    host: str
    port: int
    database: str
    schema: str
    table: str
    user : str
    password : str
    timeout: int
    
    @property
    def full_table_name(self) -> str:
        return f'"{self.schema}"."{self.table}"'

class Config:
        
    DATABASE_HOST: str = os.getenv("DATABASE_HOST")
    DATABASE_USER : str = os.getenv("DATABASE_USER")
    DATABASE_PASS : str = os.getenv("DATABASE_PASS")
    DATABASE_PORT : int = int(os.getenv("DATABASE_PORT", 5432))
    DATABASE_NAME: str = os.getenv("DATABASE_NAME")
    DATABASE_SCHEMA: str = os.getenv("DATABASE_SCHEMA")
    DATABASE_TABLE: str = os.getenv("DATABASE_TABLE")
    DB_TIMEOUT: int = int(os.getenv("DB_TIMEOUT"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT"))
    
    CORS_ALLOW_ORIGIN: str = os.getenv("CORS_ALLOW_ORIGIN", "*")
    
    
    HOST: str = os.getenv("HOST")
    PORT: int = int(os.getenv("PORT"))
    RELOAD: bool = os.getenv("RELOAD").lower() in ("1", "true", "yes")
    
    
    @classmethod
    def get_database_config(cls) -> DatabaseConfig:
        """Create DatabaseConfig from environment variables."""
        return DatabaseConfig(
           host=cls.DATABASE_HOST,
           port=cls.DATABASE_PORT,
           database=cls.DATABASE_NAME,
           schema=cls.DATABASE_SCHEMA,
           table=cls.DATABASE_TABLE,
           user=cls.DATABASE_USER,
           password=cls.DATABASE_PASS,
           timeout=cls.DB_TIMEOUT,)
    
    @classmethod
    def get_allowed_origins(cls) -> list[str]:
        """Parse CORS allowed origins from config."""
        return [o.strip() for o in cls.CORS_ALLOW_ORIGIN.split(",") if o.strip()]
    
    @classmethod
    def is_gemini_enabled(cls) -> bool:
        """Check if Gemini API is configured."""
        return bool(cls.GEMINI_API_KEY)