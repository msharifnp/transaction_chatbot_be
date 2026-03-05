from __future__ import annotations
import json
import logging
from typing import Any, Dict, List
from src.utils.utils import safe_json_from_model, retry_with_backoff
from src.models.model_service import ModelService
from src.ai.promt.summary_spec_prompt import build_summary_spec_prompt

logger = logging.getLogger(__name__)


class SummarySpecGenerator:

    PURPOSE = "Technical"

    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)

    def _safe_generate(self, prompt: str, TenantId: str, SessionId: str) -> str:
        text = self.model_service.generate(self.PURPOSE, prompt)
        if not text or not text.strip():
            raise ValueError(f"[tenant={TenantId}][session={SessionId}] Empty response received from model.")
        return text.strip()

    def generate_spec(
        self,
        user_query: str,
        available_columns: List[str],
        field_types: Dict[str, str],
        sample_rows: List[Dict],
        TenantId: str,
        SessionId: str,
    ) -> Dict[str, Any]:

        if not self.enabled:
            raise RuntimeError(f"[SUMMARY_SPEC_GEN] Purpose '{self.PURPOSE}' not enabled for tenant={TenantId}")

        prompt = build_summary_spec_prompt(
            user_query=user_query,
            available_columns=available_columns,
            field_types=field_types,
            sample_rows=sample_rows,
        )

        try:
            text = retry_with_backoff(
                lambda: self._safe_generate(prompt, TenantId, SessionId),
                max_retries=3,
                initial_delay=1,
            )

            spec = safe_json_from_model(text)

            if "aggregations" not in spec:
                raise ValueError("Missing 'aggregations' key in spec")

            if not isinstance(spec["aggregations"], list):
                raise ValueError("'aggregations' must be a list")

            for agg in spec["aggregations"]:
                if "category" not in agg:
                    raise ValueError(f"Aggregation missing 'category': {agg}")

            logger.info(f"[SUMMARY_SPEC_GEN][tenant={TenantId}][session={SessionId}] Success. {len(spec['aggregations'])} aggregations generated.")
            return spec

        except Exception as e:
            logger.warning(f"[SUMMARY_SPEC_GEN][tenant={TenantId}][session={SessionId}] LLM failed, using default spec. Error: {e}", exc_info=True)
            return self._get_default_spec(available_columns, field_types)

  