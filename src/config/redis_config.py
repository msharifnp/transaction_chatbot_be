import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
  
load_dotenv()

@dataclass
class RedisConfig:
    host: str
    port: int
    db: int
    username: Optional[str]
    password: Optional[str]
    socket_timeout: int
    socket_connect_timeout: int
    decode_responses: bool
    max_connections: int = 50  
    health_check_interval: int = 30 

class Config:
    REDIS_HOST: str = os.getenv("REDIS_HOST")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT"))
    REDIS_USER: Optional[str] = os.getenv("REDIS_USER")
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD")
    REDIS_DB: int = int(os.getenv("REDIS_DB"))
    REDIS_SOCKET_TIMEOUT: int = int(os.getenv("REDIS_SOCKET_TIMEOUT"))
    REDIS_CONNECT_TIMEOUT: int = int(os.getenv("REDIS_CONNECT_TIMEOUT"))
    REDIS_DECODE_RESPONSES: bool = os.getenv("REDIS_DECODE_RESPONSES")
    REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS"))
    
    @classmethod
    def get_redis_config(cls) -> RedisConfig:
        return RedisConfig(
            host=cls.REDIS_HOST,
            port=cls.REDIS_PORT,
            db=cls.REDIS_DB,
            username=cls.REDIS_USER,
            password=cls.REDIS_PASSWORD,
            decode_responses=cls.REDIS_DECODE_RESPONSES,
            socket_timeout=cls.REDIS_SOCKET_TIMEOUT,
            socket_connect_timeout=cls.REDIS_CONNECT_TIMEOUT,
            max_connections=cls.REDIS_MAX_CONNECTIONS,
        )