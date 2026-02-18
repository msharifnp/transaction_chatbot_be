from abc import ABC, abstractmethod
from src.models.config import ModelConfig

class BaseModelProvider(ABC):
    """Abstract base class for all model providers"""
    
    def __init__(self, config: ModelConfig):
        self.config = config
        self.client = None
        self.enabled = False
        self._initialize()
    
    @abstractmethod
    def _initialize(self):
        """Initialize the specific provider client"""
        pass
    
    @abstractmethod
    def generate_text(self, prompt: str, **kwargs) -> str:
        """Generate text using the provider's API"""
        pass
    
    def is_available(self) -> bool:
        """Check if provider is available"""
        return self.enabled and self.client is not None