import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

@dataclass
class DatabaseConfig:
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
   
    @classmethod
    def get_database_config(cls) -> DatabaseConfig:
        return DatabaseConfig(
           host=cls.DATABASE_HOST,
           port=cls.DATABASE_PORT,
           database=cls.DATABASE_NAME,
           schema=cls.DATABASE_SCHEMA,
           table=cls.DATABASE_TABLE,
           user=cls.DATABASE_USER,
           password=cls.DATABASE_PASS,
           timeout=cls.DB_TIMEOUT,)
    
 