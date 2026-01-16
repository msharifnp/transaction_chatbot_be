import os
from dotenv import load_dotenv

load_dotenv()


class Config:
       
    CORS_ALLOW_ORIGIN: str = os.getenv("CORS_ALLOW_ORIGIN", "*")   
    HOST: str = os.getenv("HOST")
    PORT: int = int(os.getenv("PORT"))
    RELOAD: bool = os.getenv("RELOAD").lower() in ("1", "true", "yes")
        
    @classmethod
    def get_allowed_origins(cls) -> list[str]:
        """Parse CORS allowed origins from config."""
        return [o.strip() for o in cls.CORS_ALLOW_ORIGIN.split(",") if o.strip()]
    