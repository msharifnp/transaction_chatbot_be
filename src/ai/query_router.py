import json
from typing import Dict
from src.utils.utils import retry_with_backoff, safe_json_from_model
from src.models.model_service import ModelService
from src.ai.promt.query_router_promt import build_routing_prompt
import logging

logger = logging.getLogger(__name__)


class QueryRouter:

    PURPOSE = "Summary"

    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)

        if not self.enabled:
            logger.warning(f"[ROUTER] Purpose '{self.PURPOSE}' is not enabled in model service.")

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

    def intelligent_route(self, user_query: str, tenant_id: str, user_id: str, session_id: str) -> Dict:

        if not self.enabled:
            logger.warning(f"[ROUTER][tenant={tenant_id}] Routing skipped - purpose not enabled.")
            return {
                "mode": "message",
                "message": "Query routing is currently unavailable.",
                "reasoning": "router_disabled",
                "should_proceed": False,
                "router_error": True,
            }

        prompt = build_routing_prompt(user_query)

        try:
            text = retry_with_backoff(
                lambda: self._safe_generate(prompt, tenant_id, user_id, session_id),
                max_retries=3,
                initial_delay=1,
            )
            logger.info(f"[ROUTER][tenant={tenant_id}][session={session_id}] Raw model output: {text}")

            result = safe_json_from_model(text)

            mode = result.get("mode")
            if not mode:
                logger.warning(f"[ROUTER][tenant={tenant_id}] Model response missing 'mode' key - defaulting to 'message'.")
                mode = "message"
                result["mode"] = mode

            result.setdefault("reasoning", "No reasoning provided by LLM.")
            result["should_proceed"] = mode != "message"

            logger.info(f"[ROUTER][tenant={tenant_id}] Mode: {mode}")
            logger.info(f"[ROUTER][tenant={tenant_id}] Reasoning: {result['reasoning']}")

            return result

        except Exception as e:
            logger.error(f"[ROUTER][tenant={tenant_id}][session={session_id}] Routing error: {e}", exc_info=True)
            return {
                "mode": "message",
                "message": "I'm unable to process your request at the moment. Please try again later.",
                "reasoning": "router_llm_failure",
                "should_proceed": False,
                "router_error": True,
                "router_error_detail": str(e),
            }
