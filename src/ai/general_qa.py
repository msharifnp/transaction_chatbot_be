from __future__ import annotations
import logging
from typing import Dict, List
from src.utils.utils import retry_with_backoff, choose_optimal_format
from src.models.model_service import ModelService
from src.ai.promt.general_qa_prompt import build_general_qa_prompt

logger = logging.getLogger(__name__)

class QAService:

    PURPOSE = "Summary"

    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)

    def _safe_generate(self, prompt: str, TenantId: str, SessionId: str) -> str:
        text = self.model_service.generate(self.PURPOSE, prompt)
        if not text or not text.strip():
            raise ValueError(f"[tenant={TenantId}][session={SessionId}] Empty response received from model.")
        return text.strip()

    def generate_general_qa(
        self,
        user_query: str,
        rows: List[Dict],
        TenantId: str,
        SessionId: str,
    ) -> str:

        if not self.enabled:
            logger.warning(f"[QA][tenant={TenantId}] Purpose not enabled.")
            return "AI analysis is currently disabled."

        if not rows:
            logger.warning(f"[QA][tenant={TenantId}] No rows provided.")
            return "No data available to answer the question."

        logger.info(f"[QA][tenant={TenantId}][session={SessionId}] Starting with {len(rows)} rows.")

        try:
            format_type, formatted_data = choose_optimal_format(rows, "general_qa")
            data_block = (
                f"DATA ({'CSV' if format_type == 'csv' else 'JSON'}):\n"
                f"```{format_type}\n{formatted_data}\n```"
            )
            logger.info(f"[QA][tenant={TenantId}] Formatted as {format_type.upper()} ({len(formatted_data)} chars).")

            prompt = build_general_qa_prompt(
                user_query=user_query,
                data_block=data_block,
            )

            result_text = retry_with_backoff(
                lambda: self._safe_generate(prompt, TenantId, SessionId),
                max_retries=3,
                initial_delay=1,
            )

            if not result_text:
                return "No answer could be generated from the data."

            logger.info(f"[QA][tenant={TenantId}][session={SessionId}] Success ({len(result_text)} chars).")
            return result_text

        except Exception as e:
            logger.error(f"[QA][tenant={TenantId}][session={SessionId}] Error: {e}", exc_info=True)
            return f"Error answering the question: {str(e)}"