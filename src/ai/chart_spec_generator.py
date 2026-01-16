from __future__ import annotations
import json
from typing import Any, Dict, List, Optional
from src.utils.utils import safe_json_from_model,retry_with_backoff
from src.db.model_service import ModelService

class SpecGenerator:
   
    def __init__(self, model_service:ModelService):
        
        self.model_service = model_service
        self.enabled = model_service.is_available()  
    
    def generate_spec(
        self,
        user_query: str,
        task: str, 
        available_columns: List[str],
        field_types: Dict[str, str],
        sample_rows: List[Dict[str, Any]],
        chart_hint: Optional[str] = None,
    ) -> Dict[str, Any]:
        
        system_instruction = """You are a data aggregation specialist. Generate a JSON spec for aggregating invoice data.

CRITICAL RULES:
1. Output ONLY valid JSON - no markdown, no explanations, no code blocks
2. Use ONLY columns from available_columns
3. If user misspells a column, map to closest match
4. For metrics: use numeric columns or "count"
5. For group_by: use categorical/date columns
6. For money fields (GrandTotal, NetTotal, etc.): default to "sum" unless user asks for avg/min/max
7. Never group by long text fields (addresses, descriptions) unless explicitly requested
8. IMPORTANT: In metrics[].col, ALWAYS use the ORIGINAL input column name from AVAILABLE COLUMNS.
   - Do NOT invent aliases like total_GrandTotal, sum_GrandTotal, avg_GrandTotal.
   - The aggregation function is defined by metrics[].agg.
   - Example: SUM(GrandTotal) must be {"col":"GrandTotal","agg":"sum"} (NOT total_GrandTotal)

9. IMPORTANT: sort_by.col must reference:
   - the SAME original input column name used in metrics[].col, OR
   - "__rows__" when sorting by count.
   - Do NOT sort by invented alias names.


SPEC STRUCTURE:
{
  "task": "chart"
  "chart_type": "bar"|"line"|"pie"|"area"|"scatter"|null,
  "group_by": ["column_name"],
  "time_bucket": null|"day"|"week"|"month",
  "metrics": [{"col": "column_name", "agg": "sum"|"avg"|"min"|"max"|"count"}],
  "filters": [],
  "sort_by": {"col": "column_name", "desc": true}|null
  
}

EXAMPLES:

WRONG (do not do this):
{"metrics":[{"col":"total_GrandTotal","agg":"sum"}]}

RIGHT:
{"metrics":[{"col":"GrandTotal","agg":"sum"}]}

User: "total spending by provider"
→ {"task":"chart","chart_type":"bar","group_by":["ProviderName"],"metrics":[{"col":"GrandTotal","agg":"sum"}],"sort_by":{"col":"GrandTotal","desc":true}}

User: "average invoice by site"
→ {"task":"chart","chart_type":"bar","group_by":["SiteName"],"metrics":[{"col":"GrandTotal","agg":"avg"}],"sort_by":{"col":"GrandTotal","desc":true}}

User: "monthly trend"
→ {"task":"chart","chart_type":"line","group_by":["InvoiceDate"],"time_bucket":"month","metrics":[{"col":"GrandTotal","agg":"sum"}],"sort_by":null}

User: "count invoices by status"
→ {"task":"chart","chart_type":"bar","group_by":["InvoiceStatusType"],"metrics":[{"col":"__rows__","agg":"count"}],"sort_by":{"col":"__rows__","desc":true}}
"""

        # Format sample data for context
        sample_str = json.dumps(sample_rows[:2], indent=2) if sample_rows else "[]"
        
        user_message = f"""USER QUERY: "{user_query}"

AVAILABLE COLUMNS: {json.dumps(available_columns)}

FIELD TYPES: {json.dumps(field_types)}

SAMPLE DATA (first 2 rows):
{sample_str}

CHART HINT: {chart_hint or "auto-detect"}

Generate the aggregation spec JSON (output only the JSON object):"""

        # Combine into single prompt
        full_prompt = f"{system_instruction}\n\n{user_message}"
        
        try:
            def generate():
                text = self.model_service.generate_text(full_prompt)
                if not text or not text.strip():
                    raise RuntimeError("Empty response from Gemini")
                return text.strip()

            text = retry_with_backoff(generate) if retry_with_backoff else generate()

            # Remove code fences if model added them
            if text.startswith("```"):
                text = text.replace("```json", "").replace("```", "").strip()

            print(f"[SPEC_GEN] Model response: {text[:200]}...")

            spec = safe_json_from_model(text)

            if not isinstance(spec, dict):
                raise ValueError(f"Spec must be a JSON object, got: {type(spec)}")

            print(f"[SPEC_GEN_CHART] ✅ Generated spec: {json.dumps(spec, indent=2)}")
            return spec

        except Exception as e:
            print(f"[SPEC_GEN] ❌ Failed to generate spec: {e}")

            # # Safe fallback spec
            # return {
            #     "task": task,
            #     "chart_type": chart_hint or "bar",
            #     "group_by": [available_columns[0]] if available_columns else [],
            #     "time_bucket": None,
            #     "metrics": (
            #         [{"col": "grand_total", "agg": "sum"}]
            #         if "grand_total" in available_columns
            #         else [{"col": "__rows__", "agg": "count"}]
            #     ),
            #     "filters": [],
            #     "sort_by": None,
            # }

