from __future__ import annotations
import re
import json
import base64
import logging
from typing import Dict, List, Optional
from src.config.field_constant import FIELD_TYPES
from src.utils.utils import retry_with_backoff, choose_optimal_format
from src.models.model_service import ModelService
from src.ai.chart_spec_generator import SpecGenerator
from src.aggregator.chart_aggregator import aggregate_rows
from src.ai.promt.chart_prompt import build_chart_prompt

logger = logging.getLogger(__name__)

class ChartService:

    PURPOSE = "Summary"

    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)

    def _safe_generate(self, prompt: str, TenantId: str, SessionId: str) -> str:
        text = self.model_service.generate(self.PURPOSE, prompt)
        if not text or not text.strip():
            raise ValueError(f"[tenant={TenantId}][session={SessionId}] Empty response received from model.")
        return text.strip()

    def generate_chart(
        self,
        user_query: str,
        rows: List[Dict],
        TenantId: str,
        SessionId: str,
        size: str = "960x560",
    ) -> Optional[Dict]:

        if not self.enabled:
            logger.warning(f"[CHART][tenant={TenantId}] Purpose not enabled.")
            return None

        if not rows:
            logger.warning(f"[CHART][tenant={TenantId}] No rows provided.")
            return None

        logger.info(f"[CHART][tenant={TenantId}][session={SessionId}] Starting with {len(rows)} rows.")

        try:
            spec_gen = SpecGenerator(self.model_service)
            spec = spec_gen.generate_spec(
                user_query=user_query,
                task="chart",
                available_columns=list(rows[0].keys()),
                field_types=FIELD_TYPES,
                sample_rows=rows[:1],
                TenantId=TenantId,
                SessionId=SessionId,
                chart_hint=None,
            )
            logger.info(f"[CHART][tenant={TenantId}] Spec: {json.dumps(spec)}")

            agg_rows = aggregate_rows(rows, spec, FIELD_TYPES, max_groups=100)
            logger.info(f"[CHART][tenant={TenantId}] Aggregated: {agg_rows}")

            if not agg_rows:
                return {"image_b64": None, "image_mime": None, "error": "No data after aggregation. Please adjust your query."}

            format_type, formatted_data = choose_optimal_format(agg_rows, "chart")
            data_block = f"DATA ({'CSV' if format_type == 'csv' else 'JSON'}):\n```{format_type}\n{formatted_data}\n```"
            logger.info(f"[CHART][tenant={TenantId}] Formatted as {format_type.upper()} ({len(formatted_data)} chars).")

            prompt = build_chart_prompt(
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
                logger.warning(f"[CHART][tenant={TenantId}] Empty response from model.")
                return {"image_b64": None, "image_mime": None, "error": "Chart generation returned no content."}

            raw = re.sub(r'^```[\w]*\s*', '', raw, flags=re.MULTILINE)
            raw = re.sub(r'\s*```\s*$', '', raw, flags=re.MULTILINE)
            raw = raw.strip()

            svg_start = re.search(r'<svg[\s>]', raw, re.IGNORECASE)
            svg_end = re.search(r'</svg\s*>', raw, re.IGNORECASE)

            if not svg_start or not svg_end:
                logger.warning(f"[CHART][tenant={TenantId}] No valid SVG tags. Preview: {raw[:300]}")
                return {"image_b64": None, "image_mime": None, "error": "Chart generation failed — invalid SVG."}

            svg = raw[svg_start.start():svg_end.end()].strip()

            if len(svg) < 100:
                logger.warning(f"[CHART][tenant={TenantId}] SVG too short ({len(svg)} chars).")
                return {"image_b64": None, "image_mime": None, "error": "Chart generation produced incomplete output."}

            if not svg.lower().startswith('<svg') or not svg.lower().endswith('</svg>'):
                logger.warning(f"[CHART][tenant={TenantId}] Malformed SVG.")
                return {"image_b64": None, "image_mime": None, "error": "Chart generation produced malformed SVG."}

            b64 = base64.b64encode(svg.encode("utf-8")).decode("ascii")
            logger.info(f"[CHART][tenant={TenantId}][session={SessionId}] Success ({len(svg)} bytes, {len(agg_rows)} points).")

            return {
                "image_b64": b64,
                "image_mime": "image/svg+xml",
                "aggregation_spec": spec,
                "data_points": len(agg_rows),
            }

        except Exception as e:
            logger.error(f"[CHART][tenant={TenantId}][session={SessionId}] Error: {e}", exc_info=True)
            return {"image_b64": None, "image_mime": None, "error": f"Chart generation error: {str(e)}"}