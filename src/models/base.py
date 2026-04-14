from abc import ABC, abstractmethod
from typing import Any

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
    def generate_text(self, prompt: str, **kwargs) -> Any:
        pass

    def transcribe_audio(self, audio_bytes: bytes, **kwargs) -> Any:
        raise NotImplementedError(
            f"Audio transcription is not supported for provider={self.config.provider}"
        )
    
    def is_available(self) -> bool:
        return self.enabled and self.client is not None
