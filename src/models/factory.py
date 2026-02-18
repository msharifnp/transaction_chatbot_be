from typing import Type
from src.models.base import BaseModelProvider
from src.models.config import ModelConfig
from src.models.providers.anthropic import AnthropicProvider
from src.models.providers.gemini import GeminiProvider
from src.models.providers.openai import OpenAIProvider

class ModelProviderFactory:
    """Factory to create model providers"""
    
    _providers: dict[str, Type[BaseModelProvider]] = {
        "Gemini": GeminiProvider,
        "OpenAI": OpenAIProvider,
        "Anthropic": AnthropicProvider,
    }
    
    @classmethod
    def create(cls, config: ModelConfig) -> BaseModelProvider:
        """Create a provider instance from config"""
        provider_class = cls._providers.get(config.provider)
        
        if not provider_class:
            raise ValueError(f"Unknown provider: {config.provider}")
        
        return provider_class(config)
    
    @classmethod
    def register_provider(cls, name: str, provider_class: Type[BaseModelProvider]):
        """Register a new provider type"""
        cls._providers[name] = provider_class