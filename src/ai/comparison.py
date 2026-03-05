from __future__ import annotations
import logging
from typing import Dict, List
from src.utils.utils import retry_with_backoff
from src.models.model_service import ModelService
from src.ai.promt.comparison_prompt import build_comparison_prompt
import logging

logger = logging.getLogger(__name__)

class ComparisonServices:

    PURPOSE = "Summary"

    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)

    def _safe_generate(self, prompt: str, TenantId: str, SessionId: str) -> str:
        text = self.model_service.generate(self.PURPOSE, prompt)
        if not text or not text.strip():
            raise ValueError(f"[tenant={TenantId}][session={SessionId}] Empty response received from model.")
        return text.strip()

    def generate_comparison(
        self,
        latest_invoice: Dict,
        previous_month_invoice: Dict,
        last_6_months: List[Dict],
        TenantId: str,
        SessionId: str,
    ) -> str:

        if not self.enabled:
            logger.warning(f"[COMPARISON][tenant={TenantId}] Purpose not enabled.")
            return "AI comparison is currently disabled."

        if not latest_invoice or not previous_month_invoice:
            logger.warning(f"[COMPARISON][tenant={TenantId}] Missing invoice data.")
            return "No invoice data available for comparison."

        logger.info(f"[COMPARISON][tenant={TenantId}][session={SessionId}] Starting comparison.")

        try:
            count = len(last_6_months) or 1
            avg_grand_total = sum(inv.get("GrandTotal", 0) for inv in last_6_months) / count
            avg_net_total   = sum(inv.get("NetTotal", 0) for inv in last_6_months) / count
            avg_tax         = sum(inv.get("TotalTax", 0) for inv in last_6_months) / count
            avg_rental      = sum(inv.get("RentalCharge", 0) or 0 for inv in last_6_months) / count

            prompt = build_comparison_prompt(
                latest_invoice=latest_invoice,
                previous_month_invoice=previous_month_invoice,
                avg_grand_total=avg_grand_total,
                avg_net_total=avg_net_total,
                avg_tax=avg_tax,
                avg_rental=avg_rental,
            )

            result_text = retry_with_backoff(
                lambda: self._safe_generate(prompt, TenantId, SessionId),
                max_retries=3,
                initial_delay=1,
            )

            if not result_text:
                return "No comparison could be generated."

            logger.info(f"[COMPARISON][tenant={TenantId}][session={SessionId}] Success ({len(result_text)} chars).")
            return result_text

        except Exception as e:
            logger.error(f"[COMPARISON][tenant={TenantId}][session={SessionId}] Error: {e}", exc_info=True)
            return f"Error generating comparison: {str(e)}"