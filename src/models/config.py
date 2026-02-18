from dataclasses import dataclass
from typing import Optional

@dataclass
class ModelConfig:
    """Configuration for any model provider"""
    provider: str  # "Gemini", "OpenAI", "Anthropic"
    model_name: str
    api_key: str
    temperature: float = 0.7
    top_p: float = 0.9
    top_k: int = 40
    max_output_tokens: int = 90000
    extra_params: Optional[dict] = None