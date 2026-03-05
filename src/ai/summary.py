import json
from typing import Dict, List
from src.config.field_constant import FIELD_TYPES
from src.utils.utils import retry_with_backoff,get_summary_spec, choose_optimal_format
from src.models.model_service import ModelService
from src.ai.summary_spec_generator import SummarySpecGenerator
from src.aggregator.summary_aggregator import aggregate_for_summary
from src.ai.promt.summary_promt import build_summary_prompt
import logging
logger = logging.getLogger(__name__)


class SummaryService:

    PURPOSE = "Summary"

    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)

    def _safe_generate(self, prompt: str, TenantId: str, SessionId: str) -> str:
        text = self.model_service.generate(self.PURPOSE, prompt)
        if not text or not text.strip():
            raise ValueError(f"[tenant={TenantId}][session={SessionId}] Empty response received from model.")
        return text.strip()

    def generate_summary(
        self,
        user_query: str,
        rows: List[Dict],
        TenantId: str,
        SessionId: str,
    ) -> str:

        if not self.enabled:
            logger.warning(f"[SUMMARY][tenant={TenantId}] Purpose not enabled.")
            return "AI summary generation is currently disabled."

        if not rows:
            logger.warning(f"[SUMMARY][tenant={TenantId}] No rows provided.")
            return "No data available for summary."

        logger.info(f"[SUMMARY][tenant={TenantId}][session={SessionId}] Starting with {len(rows)} rows.")

        try:
            spec = get_summary_spec()
            logger.info(f"[SUMMARY][tenant={TenantId}] Spec: {len(spec.get('aggregations', []))} categories — {spec.get('include_categories', [])}")

            aggregated_summary = aggregate_for_summary(
                rows=rows,
                spec=spec,
                field_types=FIELD_TYPES,
            )

            if "error" in aggregated_summary and aggregated_summary.get("total_records", 0) == 0:
                return f"Aggregation error: {aggregated_summary['error']}"

            total_records = aggregated_summary.get("total_records", 0)
            logger.info(f"[SUMMARY][tenant={TenantId}] Aggregated {total_records} records across {list(aggregated_summary.get('aggregations', {}).keys())}.")

            summary_json = json.dumps(aggregated_summary, indent=2)
            data_block = f"AGGREGATED SUMMARY DATA (JSON):\n```json\n{summary_json}\n```"
            logger.info(f"[SUMMARY][tenant={TenantId}] Data formatted ({len(summary_json)} chars vs {len(str(rows))} original).")

            date_info = ""
            time_agg = aggregated_summary.get("aggregations", {}).get("time", {})
            if isinstance(time_agg, dict) and "date_range" in time_agg:
                date_range = time_agg["date_range"]
                if date_range.get("min_date") and date_range.get("max_date"):
                    date_info = f"\n- Date Range: {date_range['min_date']} to {date_range['max_date']}"

            prompt = build_summary_prompt(
                user_query=user_query,
                total_records=total_records,
                date_info=date_info,
                data_block=data_block,
            )

            result_text = retry_with_backoff(
                lambda: self._safe_generate(prompt, TenantId, SessionId),
                max_retries=3,
                initial_delay=1,
            )

            if not result_text:
                return "No summary could be generated from the data."

            logger.info(f"[SUMMARY][tenant={TenantId}][session={SessionId}] Success ({len(result_text)} chars). ")
                       

            return result_text

        except Exception as e:
            logger.error(f"[SUMMARY][tenant={TenantId}][session={SessionId}] Error: {e}", exc_info=True)
            return f"Error generating summary: {str(e)}"
        
    def generate_summary_1(
        self,
        user_query: str,
        TenantId: str,
        SessionId: str,
        rows: List[Dict],
    ) -> str:

        if not self.enabled:
            logger.warning(f"[SUMMARY_1][tenant={TenantId}] Purpose not enabled.")
            return "AI summary generation is currently disabled."

        if not rows:
            logger.warning(f"[SUMMARY_1][tenant={TenantId}] No rows provided.")
            return "No data available for summary."

        logger.info(f"[SUMMARY_1][tenant={TenantId}][session={SessionId}] Starting with {len(rows)} rows.")

        try:
            format_type, formatted_data = choose_optimal_format(rows, "summary")
            data_block = f"DATA ({'CSV' if format_type == 'csv' else 'JSON'}):\n```{format_type}\n{formatted_data}\n```"
            logger.info(f"[SUMMARY_1][tenant={TenantId}] Formatted as {format_type.upper()} ({len(formatted_data)} chars).")

            sample_cols = list(rows[0].keys()) if rows else []
            date_info = ""
            if "invoice_date" in sample_cols:
                try:
                    dates = [r.get("invoice_date") for r in rows if r.get("invoice_date")]
                    if dates:
                        date_info = f"\n- Date Range: {min(dates)} to {max(dates)}"
                except Exception:
                    pass

            logger.info(f"[SUMMARY_1][tenant={TenantId}] Records: {len(rows)}, Columns: {len(sample_cols)}{date_info}")

            prompt = build_summary_prompt(
                user_query=user_query,
                date_info=date_info,
                data_block=data_block,
            )

            result_text = retry_with_backoff(
                lambda: self._safe_generate(prompt, TenantId, SessionId),
                max_retries=3,
                initial_delay=1,
            )

            if not result_text:
                return "No summary could be generated from the data."

            logger.info(f"[SUMMARY_1][tenant={TenantId}][session={SessionId}] Success ({len(result_text)} chars).")
            return result_text

        except Exception as e:
            logger.error(f"[SUMMARY_1][tenant={TenantId}][session={SessionId}] Error: {e}", exc_info=True)
            return f"Error generating summary: {str(e)}"