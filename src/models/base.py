from abc import ABC, abstractmethod
from src.models.config import ModelConfig

class BaseModelProvider(ABC):
    
    def __init__(self, config: ModelConfig):
        self.config = config
        self.client = None
        self.enabled = False
        self._initialize()
    
    @abstractmethod
    def _initialize(self):
        pass
    
    @abstractmethod
    def generate_text(self, prompt: str, **kwargs) -> str:
        pass
    
    def is_available(self) -> bool:
        return self.enabled and self.client is not None