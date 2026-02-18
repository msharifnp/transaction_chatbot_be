# forecast_spec_generator.py
from __future__ import annotations
import json
from typing import Any, Dict, List, Optional
from src.utils.utils import safe_json_from_model, retry_with_backoff
from src.models.model_service import ModelService


class ForecastSpecGenerator:
    """Generates forecast data specs using LLM to interpret user queries."""
    
    PURPOSE = "Technical"
    
    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)
    
    def generate_spec(
        self,
        user_query: str,
        available_columns: List[str],
        field_types: Dict[str, str],
        sample_rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        
        # Identify column categories from schema
        date_cols = [col for col, typ in field_types.items() if typ == "date" and col in available_columns]
        numeric_cols = [col for col, typ in field_types.items() if typ == "number" and col in available_columns]
        string_cols = [col for col, typ in field_types.items() if typ == "string" and col in available_columns]
        
        # Identify specific column types
        amount_cols = [col for col in numeric_cols if any(kw in col.lower() for kw in ["amount", "total", "price", "cost", "value"])]
        quantity_cols = [col for col in numeric_cols if any(kw in col.lower() for kw in ["quantity", "count", "volume"])]
        
        # Identify common grouping columns
        supplier_cols = [col for col in string_cols if "supplier" in col.lower()]
        buyer_cols = [col for col in string_cols if "buyer" in col.lower()]
        item_cols = [col for col in string_cols if "item" in col.lower()]
        location_cols = [col for col in string_cols if any(kw in col.lower() for kw in ["location", "site", "ship"])]
        
        # Build column context
        column_context = f"""
AVAILABLE DATE COLUMNS (for time series):
{', '.join(date_cols) if date_cols else 'None identified'}

AVAILABLE VALUE COLUMNS (numeric - for forecasting):
- Amount/Cost fields: {', '.join(amount_cols[:10]) if amount_cols else 'None'}
- Quantity fields: {', '.join(quantity_cols[:10]) if quantity_cols else 'None'}
- All numeric: {', '.join(numeric_cols[:15])}

AVAILABLE GROUPING COLUMNS (categorical - optional):
- Supplier: {', '.join(supplier_cols) if supplier_cols else 'None'}
- Buyer: {', '.join(buyer_cols) if buyer_cols else 'None'}
- Item: {', '.join(item_cols) if item_cols else 'None'}
- Location: {', '.join(location_cols) if location_cols else 'None'}
- Other: {', '.join([c for c in string_cols if c not in supplier_cols + buyer_cols + item_cols + location_cols][:10])}
"""
        
        system_instruction = f"""You are a purchasing/procurement forecasting specialist. Generate a JSON spec for time-series forecasting.

{column_context}

CRITICAL RULES:
1. Output ONLY valid JSON - no markdown, no explanations, no code blocks
2. Use ONLY columns from available_columns
3. If user misspells a column, map to closest match from available columns
4. date_column MUST be a DATE type column from available date columns
5. value_columns MUST be numeric columns to forecast (amounts, quantities, etc.)
6. grouping_columns are OPTIONAL - use only if user wants separate forecasts per category
7. ALWAYS use ORIGINAL column names from AVAILABLE COLUMNS (no aliases, no prefixes)
8. Default forecast_periods to 12 if not specified
9. time_bucket: "month" for monthly data, "week" for weekly, "day" for daily, "quarter" for quarterly, null for auto-detect

COLUMN SELECTION GUIDELINES:

**Date Column Priority** (choose first available):
1. DatePlaced (purchase order date)
2. RequiredDate (required delivery date)
3. ConfirmedDate (confirmed delivery date)
4. ClosedDate (order close date)
5. Any other DATE column

**Value Column Priority** (based on query intent):
- "spending/cost/total/amount" → TotalOpenAmount, TotalReceiptAmount, UnitPrice
- "quantity/volume/units" → DeliveryQuantity, ReceivedQuantity, OpenQuantity
- "price" → UnitPrice
- "orders/count" → LineCount or use COUNT aggregation
- If user mentions specific field, use that exact field

**Common Grouping Columns**:
- SupplierName: Supplier-level forecasts
- Buyer: Buyer-level forecasts
- Item: Item-level forecasts
- ShipToLocation: Location-level forecasts
- OrderType: Order type forecasts (PO, BO, etc.)
- CompanyCode: Company-level forecasts
- Supplier + SupplierLocation: Detailed supplier forecasts

SPEC STRUCTURE:
{{
  "date_column": "column_name",
  "value_columns": ["column_name1", "column_name2"],
  "grouping_columns": ["category1", "category2"],
  "time_bucket": "month"|"week"|"day"|"quarter"|null,
  "forecast_periods": 12,
  "filters": []
}}

EXAMPLES:

User: "forecast next 6 months spending"
→ {{"date_column": "DatePlaced", "value_columns": ["TotalOpenAmount"], "grouping_columns": [], "time_bucket": "month", "forecast_periods": 6}}

User: "predict purchase volume by supplier"
→ {{"date_column": "DatePlaced", "value_columns": ["TotalOpenAmount"], "grouping_columns": ["SupplierName"], "time_bucket": "month", "forecast_periods": 12}}

User: "forecast quantities ordered and received"
→ {{"date_column": "DatePlaced", "value_columns": ["DeliveryQuantity", "ReceivedQuantity"], "grouping_columns": [], "time_bucket": "month", "forecast_periods": 12}}

User: "predict weekly orders by buyer"
→ {{"date_column": "DatePlaced", "value_columns": ["TotalOpenAmount"], "grouping_columns": ["Buyer"], "time_bucket": "week", "forecast_periods": 12}}

User: "forecast item demand by location"
→ {{"date_column": "RequiredDate", "value_columns": ["DeliveryQuantity"], "grouping_columns": ["Item", "ShipToLocation"], "time_bucket": "month", "forecast_periods": 12}}

User: "predict quarterly spending per supplier and item"
→ {{"date_column": "DatePlaced", "value_columns": ["TotalOpenAmount"], "grouping_columns": ["SupplierName", "Item"], "time_bucket": "quarter", "forecast_periods": 4}}

User: "forecast unit prices"
→ {{"date_column": "DatePlaced", "value_columns": ["UnitPrice"], "grouping_columns": [], "time_bucket": "month", "forecast_periods": 12}}

User: "predict open amounts by order type"
→ {{"date_column": "DatePlaced", "value_columns": ["TotalOpenAmount"], "grouping_columns": ["OrderType"], "time_bucket": "month", "forecast_periods": 12}}

INTELLIGENT MAPPING:
- "spending" → TotalOpenAmount or TotalReceiptAmount
- "orders" → COUNT of OrderNumber or TotalOpenAmount
- "deliveries" → ReceivedQuantity or DeliveryQuantity
- "demand" → DeliveryQuantity
- "backlog" → OpenQuantity
- "by supplier" → add SupplierName to grouping_columns
- "by buyer" → add Buyer to grouping_columns
- "by item" → add Item to grouping_columns
- "by location" → add ShipToLocation to grouping_columns
- "per [category]" → add that category to grouping_columns

FALLBACK DEFAULTS:
- If no date column specified: use first available DATE column
- If no value column specified: use TotalOpenAmount if available, else first numeric column
- If no time_bucket specified: default to "month"
- If no forecast_periods specified: default to 12
- Only add grouping_columns if explicitly requested by user
"""

        # Format sample data for context
        sample_str = json.dumps(sample_rows[:2], indent=2) if sample_rows else "[]"
        
        user_message = f"""USER QUERY: "{user_query}"

AVAILABLE COLUMNS: {json.dumps(available_columns)}

FIELD TYPES: {json.dumps(field_types)}

SAMPLE DATA (first 2 rows):
{sample_str}

Generate the forecast spec JSON (output only the JSON object, no explanations):"""

        # Combine into single prompt
        prompt = f"{system_instruction}\n\n{user_message}"
        
        try:
            def generate():
                text = self.model_service.generate(self.PURPOSE, prompt).strip()
                if not text or not text.strip():
                    raise RuntimeError("Empty response from model")
                return text.strip()

            text = retry_with_backoff(generate, max_retries=3, initial_delay=2)

            # Remove code fences if model added them
            if text.startswith("```"):
                text = text.replace("```json", "").replace("```", "").strip()

            print(f"[FORECAST_SPEC_GEN] Model response: {text[:200]}...")

            spec = safe_json_from_model(text)

            if not isinstance(spec, dict):
                raise ValueError(f"Spec must be a JSON object, got: {type(spec)}")

            # Validate the spec
            self._validate_spec(spec, available_columns, field_types, date_cols, numeric_cols)

            print(f"[FORECAST_SPEC_GEN] ✅ Generated spec: {json.dumps(spec, indent=2)}")
            return spec

        except Exception as e:
            print(f"[FORECAST_SPEC_GEN] ❌ Failed to generate spec: {e}")
            
            # Safe fallback spec using detected columns
            return self._generate_fallback_spec(
                available_columns, 
                field_types, 
                date_cols, 
                numeric_cols, 
                amount_cols
            )
    
    def _validate_spec(
        self, 
        spec: Dict[str, Any], 
        available_columns: List[str],
        field_types: Dict[str, str],
        date_cols: List[str],
        numeric_cols: List[str]
    ) -> None:
        """Validate the generated spec."""
        
        # Validate date_column
        date_col = spec.get("date_column")
        if date_col and date_col not in date_cols:
            print(f"[FORECAST_SPEC_GEN] ⚠️ Warning: date_column '{date_col}' is not a DATE type or not available")
        
        # Validate value_columns
        value_cols = spec.get("value_columns", [])
        for col in value_cols:
            if col not in numeric_cols:
                print(f"[FORECAST_SPEC_GEN] ⚠️ Warning: value_column '{col}' is not numeric or not available")
        
        # Validate grouping_columns
        grouping_cols = spec.get("grouping_columns", [])
        for col in grouping_cols:
            if col not in available_columns:
                print(f"[FORECAST_SPEC_GEN] ⚠️ Warning: grouping_column '{col}' not in available columns")
    
   