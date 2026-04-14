from __future__ import annotations
import json
import logging
from typing import Any, Dict, List, Optional
from src.utils.utils import safe_json_from_model, retry_with_backoff
from src.models.model_service import ModelService
from src.ai.promt.forecast_spec_prompt import build_forecast_spec_prompt

logger = logging.getLogger(__name__)


class ForecastSpecGenerator:

    PURPOSE = "Technical"

    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)

    def _safe_generate(self, prompt: str, TenantId: str, UserId: str, SessionId: str) -> str:
        text, usage = self.model_service.generate_with_usage(
            self.PURPOSE,
            prompt,
            usage_context={
                "TenantId": TenantId,
                "UserId": UserId,
                "SessionId": SessionId,
            },
        )
        logger.info(
            f"[tenant={TenantId}][session={SessionId}] "
            f"Tokens -> Prompt: {usage.get('prompt_tokens')}, "
            f"Completion: {usage.get('completion_tokens')}, "
            f"Thoughts: {usage.get('thoughts_tokens')}, "
            f"Cache: {usage.get('cache_tokens')}, "
            f"total: {usage.get('total_tokens')}"
        )
        if not text or not text.strip():
            raise ValueError(f"[tenant={TenantId}, session={SessionId}] Empty response received from model.")
        return text

    def generate_spec(
        self,
        user_query: str,
        available_columns: List[str],
        field_types: Dict[str, str],
        sample_rows: List[Dict[str, Any]],
        TenantId: str,
        UserId: str,
        SessionId: str,
    ) -> Dict[str, Any]:

        if not self.enabled:
            raise RuntimeError(f"[FORECAST_SPEC_GEN] Purpose '{self.PURPOSE}' not enabled for tenant={TenantId}")

        prompt = build_forecast_spec_prompt(
            user_query=user_query,
            available_columns=available_columns,
            field_types=field_types,
            sample_rows=sample_rows,
        )

        try:
            text = retry_with_backoff(
                lambda: self._safe_generate(prompt, TenantId, UserId, SessionId),
                max_retries=3,
                initial_delay=1,
            )

            spec = safe_json_from_model(text)

            if not isinstance(spec, dict):
                raise ValueError(f"Spec must be a JSON object, got: {type(spec)}")

            logger.info(f"[FORECAST_SPEC_GEN][tenant={TenantId}][session={SessionId}] Success. Spec: {json.dumps(spec)}")
            return spec

        except Exception as e:
            logger.error(f"[FORECAST_SPEC_GEN][tenant={TenantId}][session={SessionId}] Failed: {e}", exc_info=True)
            raise RuntimeError(f"Forecast spec generation failed: {e}")
