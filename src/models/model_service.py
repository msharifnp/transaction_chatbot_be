import logging
from time import perf_counter
from typing import Any, Dict, Tuple

from src.models.base import BaseModelProvider
from src.models.registry import ModelRegistry
from src.function.ai_token_usage_service import AiTokenUsageService

logger = logging.getLogger(__name__)

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
        self.token_usage_service = AiTokenUsageService()

    def get_model(self, purpose: str) -> BaseModelProvider:
        model = self._registry.get_model(self.tenant_id, purpose)
        if not model:
            raise ValueError(f"No model found for tenant={self.tenant_id}, purpose={purpose}")
        if not model.is_available():
            raise RuntimeError(f"Model not available for purpose={purpose}")
        return model

    def generate(self, purpose: str, prompt: str, **kwargs) -> Any:
        model = self.get_model(purpose)
        return model.generate_text(prompt, **kwargs)

    def transcribe(self, purpose: str, audio_bytes: bytes, **kwargs) -> Any:
        model = self.get_model(purpose)
        return model.transcribe_audio(audio_bytes, **kwargs)

    def generate_with_usage(self, purpose: str, prompt: str, **kwargs) -> Tuple[str, Dict[str, Any]]:
        usage_context = kwargs.pop("usage_context", None)
        model = self.get_model(purpose)

        started_at = perf_counter()
        response = model.generate_text(prompt, **kwargs)
        latency_ms = int((perf_counter() - started_at) * 1000)

        text, usage = self.extract_text_and_usage(response)
        self._persist_usage(
            model=model,
            purpose=purpose,
            usage_context=usage_context,
            usage=usage,
            latency_ms=latency_ms,
        )
        return text, usage

    @staticmethod
    def extract_text_and_usage(response: Any) -> Tuple[str, Dict[str, Any]]:
        if isinstance(response, dict):
            text = response.get("text", "") or ""
            raw_usage = response.get("usage") or {}
        else:
            text = response or ""
            raw_usage = {}

        usage = {
            "prompt_tokens": raw_usage.get("prompt_tokens"),
            "completion_tokens": raw_usage.get("completion_tokens"),
            "thoughts_tokens": raw_usage.get("thoughts_tokens"),
            "cache_tokens": raw_usage.get("cache_tokens", raw_usage.get("cached_tokens")),
            "total_tokens": raw_usage.get("total_tokens"),
        }

        if usage["cache_tokens"] is None:
            usage["cache_tokens"] = 0

        return text, usage

    def _persist_usage(
        self,
        model: BaseModelProvider,
        purpose: str,
        usage_context: Dict[str, Any] | None,
        usage: Dict[str, Any],
        latency_ms: int,
    ) -> None:
        if not usage_context:
            return

        tenant_id = usage_context.get("TenantId") or usage_context.get("tenant_id") or self.tenant_id
        user_id = usage_context.get("UserId") or usage_context.get("user_id")
        session_id = usage_context.get("SessionId") or usage_context.get("session_id")

        if not tenant_id or not user_id or not session_id:
            logger.warning(
                "[AI_TOKEN_USAGE] Skipping usage persistence due to missing context "
                "(tenant=%s, user=%s, session=%s)",
                tenant_id,
                user_id,
                session_id,
            )
            return

        try:
            self.token_usage_service.store_usage(
                tenant_id=tenant_id,
                user_id=user_id,
                session_id=session_id,
                purpose=purpose,
                prompt_tokens=usage.get("prompt_tokens"),
                completion_tokens=usage.get("completion_tokens"),
                thoughts_tokens=usage.get("thoughts_tokens"),
                cache_tokens=usage.get("cache_tokens"),
                total_tokens=usage.get("total_tokens"),
                model_name=model.config.model_name,
                provider=model.config.provider,
                latency_ms=latency_ms,
            )
        except Exception as e:
            logger.warning("[AI_TOKEN_USAGE] Failed to persist usage: %s", e, exc_info=True)

    def has_purpose(self, purpose: str) -> bool:
        return self._registry.get_model(self.tenant_id, purpose) is not None

    def is_available(self) -> bool:
        models = self._registry.get_all_for_tenant(self.tenant_id)
        return len(models) > 0
