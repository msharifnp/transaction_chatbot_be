from __future__ import annotations
import json
from typing import Any, Dict, List, Optional
from src.utils.utils import safe_json_from_model, retry_with_backoff
from src.models.model_service import ModelService

class SpecGenerator:
    
    PURPOSE = "Technical"
    
    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)
    
    def generate_spec(
        self,
        user_query: str,
        task: str, 
        available_columns: List[str],
        field_types: Dict[str, str],
        sample_rows: List[Dict[str, Any]],
        chart_hint: Optional[str] = None,
    ) -> Dict[str, Any]:
        
        # Build column context with types
        column_context = []
        for col in available_columns:
            col_type = field_types.get(col, "string")
            column_context.append(f"{col} ({col_type})")
        
        column_context_str = ", ".join(column_context[:20])  # Limit to first 20 for prompt brevity
        
        # Identify key numeric columns for suggestions
        numeric_cols = [col for col, typ in field_types.items() if typ == "number"]
        date_cols = [col for col, typ in field_types.items() if typ == "date"]
        string_cols = [col for col, typ in field_types.items() if typ == "string"]
        
        # Identify common aggregation columns
        amount_cols = [col for col in numeric_cols if any(kw in col.lower() for kw in ["amount", "total", "price", "cost", "value"])]
        quantity_cols = [col for col in numeric_cols if any(kw in col.lower() for kw in ["quantity", "count", "volume"])]
        
        system_instruction = f"""You are a data aggregation specialist for purchasing/procurement data. Generate a JSON spec for aggregating data.

DATA SCHEMA CONTEXT:
- Available columns: {column_context_str}
- Numeric columns (for aggregation): {', '.join(numeric_cols[:10])}
- Date columns (for time grouping): {', '.join(date_cols)}
- Categorical columns (for grouping): {', '.join(string_cols[:10])}

COMMON PURCHASING DATA PATTERNS:
- Amount/Cost fields: {', '.join(amount_cols[:5]) if amount_cols else 'None identified'}
- Quantity fields: {', '.join(quantity_cols[:5]) if quantity_cols else 'None identified'}
- Typical groupings: Supplier, Buyer, Item, Location, OrderType

CRITICAL RULES:
1. Output ONLY valid JSON - no markdown, no explanations, no code blocks
2. Use ONLY columns from available_columns
3. If user misspells a column, map to closest match from available columns
4. For metrics: use numeric columns or "__rows__" for count
5. For group_by: use categorical/date columns (avoid long text fields)
6. For amount/total/price fields: default to "sum" unless user asks for avg/min/max
7. For quantity fields: default to "sum" for totals, "avg" if user asks for average
8. Never group by long text fields (addresses, descriptions) unless explicitly requested

9. **CRITICAL - Column Naming**:
   - In metrics[].col, ALWAYS use the ORIGINAL input column name from available_columns
   - Do NOT invent aliases like total_GrandTotal, sum_GrandTotal, avg_GrandTotal
   - The aggregation function is defined by metrics[].agg
   - Example: SUM(TotalOpenAmount) → {{"col": "TotalOpenAmount", "agg": "sum"}}
   - Example: AVG(UnitPrice) → {{"col": "UnitPrice", "agg": "avg"}}
   - Example: COUNT(*) → {{"col": "__rows__", "agg": "count"}}

10. **CRITICAL - Sorting**:
    - sort_by.col must reference:
      * The SAME original input column name used in metrics[].col, OR
      * "__rows__" when sorting by count
    - Do NOT sort by invented alias names
    - Example: If metrics has {{"col": "TotalOpenAmount", "agg": "sum"}}, 
      then sort_by should be {{"col": "TotalOpenAmount", "desc": true}}

SPEC STRUCTURE:
{{
  "task": "{task}",
  "chart_type": "bar"|"line"|"pie"|"area"|"scatter"|null,
  "group_by": ["column_name"],
  "time_bucket": null|"day"|"week"|"month"|"quarter"|"year",
  "metrics": [{{"col": "column_name", "agg": "sum"|"avg"|"min"|"max"|"count"}}],
  "filters": [],
  "sort_by": {{"col": "column_name", "desc": true}}|null
}}

CHART TYPE SELECTION GUIDANCE:
- **bar**: Comparing categories (e.g., "spending by supplier", "orders by buyer")
- **line**: Time trends (e.g., "monthly spending trend", "delivery over time")
- **pie**: Proportions/distribution (e.g., "share by supplier", "percentage by type")
- **area**: Cumulative trends over time
- **scatter**: Correlation between two metrics (rarely used)

EXAMPLES:

WRONG (do not do this):
{{"metrics": [{{"col": "total_TotalOpenAmount", "agg": "sum"}}]}}
{{"metrics": [{{"col": "sum_UnitPrice", "agg": "sum"}}]}}
{{"sort_by": {{"col": "total_amount", "desc": true}}}} when metrics has TotalOpenAmount

RIGHT:
{{"metrics": [{{"col": "TotalOpenAmount", "agg": "sum"}}]}}
{{"metrics": [{{"col": "UnitPrice", "agg": "sum"}}]}}
{{"sort_by": {{"col": "TotalOpenAmount", "desc": true}}}}

PURCHASING DATA EXAMPLES:

User: "total spending by supplier"
→ {{"task": "chart", "chart_type": "bar", "group_by": ["SupplierName"], "metrics": [{{"col": "TotalOpenAmount", "agg": "sum"}}], "sort_by": {{"col": "TotalOpenAmount", "desc": true}}}}

User: "average order value by buyer"
→ {{"task": "chart", "chart_type": "bar", "group_by": ["Buyer"], "metrics": [{{"col": "TotalOpenAmount", "agg": "avg"}}], "sort_by": {{"col": "TotalOpenAmount", "desc": true}}}}

User: "monthly purchase order trend"
→ {{"task": "chart", "chart_type": "line", "group_by": ["DatePlaced"], "time_bucket": "month", "metrics": [{{"col": "TotalOpenAmount", "agg": "sum"}}], "sort_by": null}}

User: "count orders by status"
→ {{"task": "chart", "chart_type": "bar", "group_by": ["OrderType"], "metrics": [{{"col": "__rows__", "agg": "count"}}], "sort_by": {{"col": "__rows__", "desc": true}}}}

User: "quantity ordered by item"
→ {{"task": "chart", "chart_type": "bar", "group_by": ["Item"], "metrics": [{{"col": "DeliveryQuantity", "agg": "sum"}}], "sort_by": {{"col": "DeliveryQuantity", "desc": true}}}}

User: "supplier distribution"
→ {{"task": "chart", "chart_type": "pie", "group_by": ["SupplierName"], "metrics": [{{"col": "TotalOpenAmount", "agg": "sum"}}], "sort_by": {{"col": "TotalOpenAmount", "desc": true}}}}

User: "total received vs ordered by supplier"
→ {{"task": "chart", "chart_type": "bar", "group_by": ["SupplierName"], "metrics": [{{"col": "DeliveryQuantity", "agg": "sum"}}, {{"col": "ReceivedQuantity", "agg": "sum"}}], "sort_by": {{"col": "DeliveryQuantity", "desc": true}}}}

INTELLIGENCE GUIDELINES:
- For "spending/cost/total" queries: use amount columns (TotalOpenAmount, UnitPrice, etc.)
- For "volume/quantity" queries: use quantity columns (DeliveryQuantity, ReceivedQuantity, etc.)
- For "trend/over time" queries: use line chart with time_bucket
- For "comparison/by X" queries: use bar chart grouped by X
- For "distribution/share/breakdown" queries: use pie chart
- For multi-metric comparisons: use bar chart with multiple metrics
- Default to "sum" for amounts and quantities unless user specifies otherwise
"""

        # Format sample data for context
        sample_str = json.dumps(sample_rows[:2], indent=2) if sample_rows else "[]"
        
        user_message = f"""USER QUERY: "{user_query}"

AVAILABLE COLUMNS: {json.dumps(available_columns)}

FIELD TYPES: {json.dumps(field_types)}

SAMPLE DATA (first 2 rows):
{sample_str}

CHART HINT: {chart_hint or "auto-detect based on query pattern"}

Generate the aggregation spec JSON (output only the JSON object, no other text):"""

        # Combine into single prompt
        prompt = f"{system_instruction}\n\n{user_message}"
        
        try:
            def generate():
                text = self.model_service.generate(self.PURPOSE, prompt).strip()
                if not text or not text.strip():
                    raise RuntimeError("Empty response from Gemini")
                return text.strip()

            text = retry_with_backoff(generate, max_retries=3, initial_delay=2)

            # Remove code fences if model added them
            if text.startswith("```"):
                text = text.replace("```json", "").replace("```", "").strip()

            print(f"[SPEC_GEN] Model response: {text[:200]}...")

            spec = safe_json_from_model(text)

            if not isinstance(spec, dict):
                raise ValueError(f"Spec must be a JSON object, got: {type(spec)}")

            # Validate that columns in spec exist in available_columns
            self._validate_spec_columns(spec, available_columns, field_types)

            print(f"[SPEC_GEN] ✅ Generated spec: {json.dumps(spec, indent=2)}")
            return spec

        except Exception as e:
            print(f"[SPEC_GEN] ❌ Failed to generate spec: {e}")
            # Return a safe default spec
            return {
                "task": task,
                "chart_type": "bar",
                "group_by": [string_cols[0]] if string_cols else [],
                "metrics": [{"col": numeric_cols[0] if numeric_cols else "__rows__", "agg": "sum" if numeric_cols else "count"}],
                "sort_by": None
            }
    
    def _validate_spec_columns(self, spec: Dict[str, Any], available_columns: List[str], field_types: Dict[str, str]) -> None:
        """Validate that spec uses only available columns and correct types."""
        
        # Validate group_by columns
        if "group_by" in spec and spec["group_by"]:
            for col in spec["group_by"]:
                if col not in available_columns and col != "__rows__":
                    print(f"[SPEC_GEN] ⚠️ Warning: group_by column '{col}' not in available columns")
        
        # Validate metrics columns
        if "metrics" in spec and spec["metrics"]:
            for metric in spec["metrics"]:
                col = metric.get("col")
                if col and col not in available_columns and col != "__rows__":
                    print(f"[SPEC_GEN] ⚠️ Warning: metric column '{col}' not in available columns")
                
                # Check that numeric columns are used for numeric aggregations
                agg = metric.get("agg")
                if col in field_types and agg in ["sum", "avg", "min", "max"]:
                    if field_types[col] not in ["number", "numeric"]:
                        print(f"[SPEC_GEN] ⚠️ Warning: numeric aggregation '{agg}' on non-numeric column '{col}'")
        
        # Validate sort_by column
        if "sort_by" in spec and spec["sort_by"]:
            sort_col = spec["sort_by"].get("col")
            if sort_col and sort_col not in available_columns and sort_col != "__rows__":
                print(f"[SPEC_GEN] ⚠️ Warning: sort_by column '{sort_col}' not in available columns")