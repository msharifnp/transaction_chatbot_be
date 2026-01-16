import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
  
load_dotenv()

@dataclass
class RedisConfig:
    """Configuration for Redis connection."""
    host: str
    port: int
    db: int
    username: str
    password: str
    socket_timeout: int
    socket_connect_timeout: int
    decode_responses: bool

class Config:
        
    REDIS_HOST:str = os.getenv("REDIS_HOST")
    REDIS_PORT:int = int(os.getenv("REDIS_PORT",6379))
    REDIS_USER : str = os.getenv("REDIS_USER")
    REDIS_PASSWORD : str= os.getenv("REDIS_PASSWORD")
    INVOICE_REDIS_DB = int(os.getenv("REDIS_DB",1))
    REDIS_SOCKET_TIMEOUT: int = int(os.getenv("REDIS_SOCKET_TIMEOUT"))
    REDIS_CONNECT_TIMEOUT: int = int(os.getenv("REDIS_CONNECT_TIMEOUT"))
    REDIS_DECODE_RESPONSES: bool = os.getenv("REDIS_DECODE_RESPONSES").lower() in ("1", "true", "yes")
    
    @classmethod
    def get_redis_config(cls) -> RedisConfig:
        """Create Redis connection configuration from environment variables."""
        return RedisConfig(
            host=cls.REDIS_HOST,
            port= cls.REDIS_PORT,
            db= cls.INVOICE_REDIS_DB,
            username= cls.REDIS_USER,
            password= cls.REDIS_PASSWORD,
            decode_responses= cls.REDIS_DECODE_RESPONSES,
            socket_timeout= cls.REDIS_SOCKET_TIMEOUT,
            socket_connect_timeout= cls.REDIS_CONNECT_TIMEOUT,
            
        )
    
