import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class GeminiConfig:
    api_key: str
    model: str
    temperature: float
    top_p: float
    top_k: int
    max_output_tokens: int
    strict: bool

class Config:  
   
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY")
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL")
    TEMPERATURE: float = float(os.getenv("TEMPERATURE"))
    TOP_P: float = float(os.getenv("TOP_P"))
    TOP_K: int = int(os.getenv("TOP_K"))
    MAX_OUTPUT_TOKENS: int = int(os.getenv("MAX_OUTPUT_TOKENS"))
    GEMINI_STRICT: bool = os.getenv("GEMINI_STRICT").lower() in ("1", "true", "yes")
    
    
    
    @classmethod
    def get_gemini_config(cls) -> GeminiConfig:
        """Create Gemini configuration from environment variables."""
        return GeminiConfig(
            api_key=cls.GEMINI_API_KEY,
            model=cls.GEMINI_MODEL,
            temperature=cls.TEMPERATURE,
            top_p=cls.TOP_P,
            top_k=cls.TOP_K,
            max_output_tokens=cls.MAX_OUTPUT_TOKENS,
            strict=cls.GEMINI_STRICT,
        )
    
    
    
    
    @classmethod
    def is_gemini_enabled(cls) -> bool:
        """Check if Gemini API is configured."""
        return bool(cls.GEMINI_API_KEY)