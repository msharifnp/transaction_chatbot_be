from typing import Optional
from src.models.base import BaseModelProvider
from src.models.registry import ModelRegistry

class ModelService:
    """
    Service to access models by purpose for a specific tenant.
    This is the main interface that your AI services will use.
    """
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.registry = ModelRegistry()
    
    def get_model(self, purpose: str) -> BaseModelProvider:
        """Get model for specific purpose"""
        model = self.registry.get_model(self.tenant_id, purpose)
        
        if not model:
            raise ValueError(
                f"No model found for tenant={self.tenant_id}, purpose={purpose}"
            )
        
        if not model.is_available():
            raise RuntimeError(f"Model not available for purpose={purpose}")
        
        return model
    
    def generate(self, purpose: str, prompt: str, **kwargs) -> str:
        """Generate text using purpose-specific model"""
        model = self.get_model(purpose)
        return model.generate_text(prompt, **kwargs)
    
    def has_purpose(self, purpose: str) -> bool:
        """Check if a purpose is configured for this tenant"""
        return self.registry.get_model(self.tenant_id, purpose) is not None
    
    def is_available(self) -> bool:  # <--- ADDED THIS METHOD
        """
        Check if the service is ready (has any models loaded for this tenant).
        This fixes the AttributeError in GeminiService.
        """
        models = self.registry.get_all_for_tenant(self.tenant_id)
        return len(models) > 0
