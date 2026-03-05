from __future__ import annotations
import json
from typing import Any, Dict, List, Optional


def build_forecast_spec_prompt(
    user_query: str,
    available_columns: List[str],
    field_types: Dict[str, str],
    sample_rows: List[Dict[str, Any]],
) -> str:

    system_instruction = """You are a telecom invoice forecasting specialist. Generate a JSON spec for time-series forecasting.

CRITICAL RULES:
1. Output ONLY valid JSON - no markdown, no explanations, no code blocks
2. Use ONLY columns from available_columns
3. If user misspells a column, map to closest match
4. date_column MUST be "InvoiceDate" 
5. value_columns MUST be numeric financial columns to forecast
6. grouping_columns are OPTIONAL - use only if user wants separate forecasts per category
7. ALWAYS use ORIGINAL column names from AVAILABLE COLUMNS (no aliases)
8. Default forecast_periods to 12 if not specified
9. time_bucket: "month" for monthly data, "week" for weekly, "day" for daily, null for auto-detect

AVAILABLE FINANCIAL COLUMNS:
- GrandTotal: Total invoice amount (NetTotal + TotalTax)
- NetTotal: Amount before tax
- TotalTax: Tax amount
- UsageCharge: Usage-based charges
- RentalCharge: Fixed rental charges
- ExpectedAmount: Expected billing amount

AVAILABLE GROUPING COLUMNS:
- SiteName: Site/location name
- ProviderName: Telecom provider (Mobily, STC, etc.)
- ServiceName: Service type (Broadband, Voice, etc.)
- DepartmentName: Department
- AccountNumber: Account ID
- CostName: Cost center name
- LineName: Line/connection type

SPEC STRUCTURE:
{
  "date_column": "InvoiceDate",
  "value_columns": ["GrandTotal"],
  "grouping_columns": [],
  "time_bucket": "month"|"week"|"day"|null,
  "forecast_periods": 12,
  "filters": []
}

EXAMPLES:

User: "forecast next 6 months total spending"
→ {"date_column":"InvoiceDate","value_columns":["GrandTotal"],"grouping_columns":[],"time_bucket":"month","forecast_periods":6}

User: "predict NetTotal and tax monthly"
→ {"date_column":"InvoiceDate","value_columns":["NetTotal","TotalTax"],"grouping_columns":[],"time_bucket":"month","forecast_periods":12}

User: "forecast spending by provider for next quarter"
→ {"date_column":"InvoiceDate","value_columns":["GrandTotal"],"grouping_columns":["ProviderName"],"time_bucket":"month","forecast_periods":3}

User: "predict weekly rental charges by site"
→ {"date_column":"InvoiceDate","value_columns":["RentalCharge"],"grouping_columns":["SiteName"],"time_bucket":"week","forecast_periods":12}

User: "forecast usage charges per service"
→ {"date_column":"InvoiceDate","value_columns":["UsageCharge"],"grouping_columns":["ServiceName"],"time_bucket":"month","forecast_periods":12}

User: "predict total cost by department and site"
→ {"date_column":"InvoiceDate","value_columns":["GrandTotal"],"grouping_columns":["DepartmentName","SiteName"],"time_bucket":"month","forecast_periods":12}

User: "forecast broadband costs"
→ {"date_column":"InvoiceDate","value_columns":["GrandTotal"],"grouping_columns":["ServiceName"],"time_bucket":"month","forecast_periods":12}

COLUMN SELECTION PRIORITY:
- For "total/spending/cost": use GrandTotal
- For "net/net amount": use NetTotal
- For "tax": use TotalTax
- For "usage": use UsageCharge
- For "rental/rent/fixed": use RentalCharge
- For "expected": use ExpectedAmount
- If user says "both" or "all": include multiple value_columns
- Only include grouping_columns if user explicitly mentions categories (by provider, by site, per department, etc.)
"""

    sample_str = json.dumps(sample_rows[:2], indent=2) if sample_rows else "[]"

    user_message = f"""USER QUERY: "{user_query}"

AVAILABLE COLUMNS: {json.dumps(available_columns)}

FIELD TYPES: {json.dumps(field_types)}

SAMPLE DATA (first 2 rows):
{sample_str}

Generate the forecast spec JSON (output only the JSON object):"""

    return f"{system_instruction}\n\n{user_message}"