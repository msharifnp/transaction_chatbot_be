from dataclasses import dataclass
from typing import Optional

@dataclass
class ModelConfig:
    provider: str  
    model_name: str
    api_key: str
    temperature: float
    top_p: float 
    top_k: int 
    max_output_tokens: int 
    extra_params: Optional[dict] = None