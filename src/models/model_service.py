from src.models.base import BaseModelProvider
from src.models.registry import ModelRegistry

class ModelService:

    _registry = ModelRegistry()
    _instances: dict = {}

    @classmethod
    def for_tenant(cls, tenant_id: str) -> "ModelService":
        if tenant_id not in cls._instances:
            cls._instances[tenant_id] = cls(tenant_id)
        return cls._instances[tenant_id]

    @classmethod
    def invalidate_tenant(cls, tenant_id: str):
        cls._instances.pop(tenant_id, None)

    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id

    def get_model(self, purpose: str) -> BaseModelProvider:
        model = self._registry.get_model(self.tenant_id, purpose)
        if not model:
            raise ValueError(f"No model found for tenant={self.tenant_id}, purpose={purpose}")
        if not model.is_available():
            raise RuntimeError(f"Model not available for purpose={purpose}")
        return model

    def generate(self, purpose: str, prompt: str, **kwargs) -> str:
        model = self.get_model(purpose)
        return model.generate_text(prompt, **kwargs)

    def has_purpose(self, purpose: str) -> bool:
        return self._registry.get_model(self.tenant_id, purpose) is not None

    def is_available(self) -> bool:
        models = self._registry.get_all_for_tenant(self.tenant_id)
        return len(models) > 0