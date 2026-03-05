from __future__ import annotations
import re
import json
import base64
import logging
from typing import Dict, List, Optional
from src.config.field_constant import FIELD_TYPES
from src.utils.utils import retry_with_backoff, choose_optimal_format, restore_original_columns
from src.models.model_service import ModelService
from src.ai.forecast_spec_generator import ForecastSpecGenerator
from src.aggregator.forecast_aggregator import prepare_forecast_data, validate_and_fix_forecast_spec
from src.ai.promt.forecast_prompt import build_forecast_summary_prompt, build_forecast_chart_prompt

logger = logging.getLogger(__name__)


class ForecastService:

    PURPOSE = "Summary"

    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)

    def _safe_generate(self, prompt: str, TenantId: str, SessionId: str) -> str:
        text = self.model_service.generate(self.PURPOSE, prompt)
        if not text or not text.strip():
            raise ValueError(f"[tenant={TenantId}][session={SessionId}] Empty response received from model.")
        return text.strip()

    def generate_forecast(
        self,
        user_query: str,
        rows: List[Dict],
        TenantId: str,
        SessionId: str,
        periods: int = 12,
    ) -> Dict | str:

        if not self.enabled:
            logger.warning(f"[FORECAST][tenant={TenantId}] Purpose not enabled.")
            return "AI forecasting is currently disabled."

        if not rows:
            logger.warning(f"[FORECAST][tenant={TenantId}] No rows provided.")
            return "No data available for forecasting."

        logger.info(f"[FORECAST][tenant={TenantId}][session={SessionId}] Starting with {len(rows)} rows.")

        try:
            spec_gen = ForecastSpecGenerator(self.model_service)
            spec = spec_gen.generate_spec(
                user_query=user_query,
                available_columns=list(rows[0].keys()),
                field_types=FIELD_TYPES,
                sample_rows=rows[:1],
                TenantId=TenantId,
                SessionId=SessionId,
            )
            logger.info(f"[FORECAST][tenant={TenantId}] Spec: {json.dumps(spec)}")

            forecast_data, validated_spec = prepare_forecast_data(
                rows=rows,
                spec=spec,
                field_types=FIELD_TYPES,
                max_groups=50,
            )
            logger.info(f"[FORECAST][tenant={TenantId}] Prepared {len(forecast_data)} records.")

            if not forecast_data:
                return "No valid data for forecasting. Please check your date and value columns."

            format_type, formatted_data = choose_optimal_format(forecast_data, "forecast")
            data_block = f"DATA ({'CSV' if format_type == 'csv' else 'JSON'}):\n```{format_type}\n{formatted_data}\n```"
            chart_data = restore_original_columns(forecast_data, validated_spec)
            logger.info(f"[FORECAST][tenant={TenantId}] Formatted as {format_type.upper()} ({len(formatted_data)} chars).")

            prompt = build_forecast_summary_prompt(
                user_query=user_query,
                forecast_data=forecast_data,
                validated_spec=validated_spec,
                periods=periods,
            )

            result_text = retry_with_backoff(
                lambda: self._safe_generate(prompt, TenantId, SessionId),
                max_retries=3,
                initial_delay=1,
            )

            if not result_text:
                return "No forecast could be generated from the data."

            logger.info(f"[FORECAST][tenant={TenantId}][session={SessionId}] Success ({len(result_text)} chars).")
            return {
                "text": result_text,
                "forecast_rows": chart_data,
            }

        except Exception as e:
            logger.error(f"[FORECAST][tenant={TenantId}][session={SessionId}] Error: {e}", exc_info=True)
            return f"Error generating forecast: {str(e)}"

    def generate_forecast_chart(
        self,
        user_query: str,
        forecast_rows: List[Dict],
        TenantId: str,
        SessionId: str,
        size: str = "960x560",
    ) -> Optional[Dict]:

        if not self.enabled:
            logger.warning(f"[FORECAST_CHART][tenant={TenantId}] Purpose not enabled.")
            return None

        if not forecast_rows:
            logger.warning(f"[FORECAST_CHART][tenant={TenantId}] No rows provided.")
            return None

        try:
            format_type, formatted_data = choose_optimal_format(forecast_rows, "chart")
            data_block = f"DATA ({'CSV' if format_type == 'csv' else 'JSON'}):\n```{format_type}\n{formatted_data}\n```"
            logger.info(f"[FORECAST_CHART][tenant={TenantId}] Formatted as {format_type.upper()} ({len(formatted_data)} chars).")

            prompt = build_forecast_chart_prompt(
                user_query=user_query,
                data_block=data_block,
                size=size,
            )

            raw = retry_with_backoff(
                lambda: self._safe_generate(prompt, TenantId, SessionId),
                max_retries=3,
                initial_delay=2,
            )

            if not raw:
                logger.warning(f"[FORECAST_CHART][tenant={TenantId}] Empty response from model.")
                return {"image_b64": None, "image_mime": None, "error": "Chart generation returned no content."}

            raw = re.sub(r'^```[\w]*\s*', '', raw, flags=re.MULTILINE)
            raw = re.sub(r'\s*```\s*$', '', raw, flags=re.MULTILINE)
            raw = raw.strip()

            svg_start = re.search(r'<svg[\s>]', raw, re.IGNORECASE)
            svg_end = re.search(r'</svg\s*>', raw, re.IGNORECASE)

            if not svg_start or not svg_end:
                logger.warning(f"[FORECAST_CHART][tenant={TenantId}] No valid SVG tags found. Preview: {raw[:500]}")
                return {"image_b64": None, "image_mime": None, "error": "Invalid SVG generated."}

            svg = raw[svg_start.start():svg_end.end()].strip()

            if len(svg) < 100 or not svg.lower().startswith('<svg'):
                logger.warning(f"[FORECAST_CHART][tenant={TenantId}] Malformed SVG (length: {len(svg)}).")
                return {"image_b64": None, "image_mime": None, "error": "Malformed SVG output."}

            b64 = base64.b64encode(svg.encode("utf-8")).decode("ascii")
            logger.info(f"[FORECAST_CHART][tenant={TenantId}][session={SessionId}] Success ({len(svg)} bytes).")

            return {"image_b64": b64, "image_mime": "image/svg+xml"}

        except Exception as e:
            logger.error(f"[FORECAST_CHART][tenant={TenantId}][session={SessionId}] Error: {e}", exc_info=True)
            return {"image_b64": None, "image_mime": None, "error": f"Chart generation error: {str(e)}"}