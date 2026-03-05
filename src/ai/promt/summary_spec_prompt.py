from __future__ import annotations
import json
from typing import Any, Dict, List


def build_summary_spec_prompt(
    user_query: str,
    available_columns: List[str],
    field_types: Dict[str, str],
    sample_rows: List[Dict],
) -> str:

    return f"""You are a data aggregation specialist. Analyze the user's summary request and generate an aggregation specification.

USER REQUEST: "{user_query}"

AVAILABLE COLUMNS: {', '.join(available_columns)}

FIELD TYPES:
{json.dumps(field_types, indent=2)}

SAMPLE DATA (first 3 rows):
{json.dumps(sample_rows, indent=2)}

TASK: Generate a JSON specification for aggregating invoice data for executive summary analysis.

AGGREGATION CATEGORIES TO CONSIDER:

1. **Status Analysis** (if columns exist):
   - InvoiceStatusType, InvoiceApprovalStatus, VerificationResult, PaymentStatus
   - Aggregate: COUNT, SUM(GrandTotal)
   - Group by multiple status fields to show detailed breakdown
   - Purpose: Show invoice status distribution and process efficiency

2. **Provider Analysis**:
   - ProviderName, provider_name, service_provider, vendor
   - Aggregate: COUNT, SUM(GrandTotal), TOP 10 providers
   - Purpose: Identify vendor concentration and dependencies

3. **Cost Center / Location Analysis**:
   - CostName, cost_name, SiteName, site_name, city, emirate, region
   - Aggregate: COUNT, SUM(GrandTotal), TOP 10 locations
   - Purpose: Show spending distribution across departments/locations

4. **Service Analysis** (if relevant):
   - ServiceName, service_name, LineName, ConnectionName, service_type
   - Aggregate: COUNT, SUM(GrandTotal), TOP 10 services
   - Purpose: Understand service mix and potential duplications

5. **Financial Metrics** (ALWAYS INCLUDE):
   - GrandTotal, NetTotal, TotalTax, UsageCharge, RentalCharge, ExpectedAmount
   - Calculate: SUM, AVG, MIN, MAX, MEDIAN, COUNT
   - Purpose: Overall financial snapshot

6. **Time Analysis** (if date column exists):
   - InvoiceDate, BillReceiveDate, billing_period, month, period
   - Aggregate by: MONTH or QUARTER
   - Purpose: Identify trends and seasonality

7. **Risk Indicators** (CRITICAL - ENHANCED BREAKDOWN):
   - This category must provide SEPARATE breakdowns for:
   
   a) **Disputed Invoices** (InvoiceStatusType = 'Disputed' or 'System Disputed'):
      - Group by: InvoiceStatusType, InvoiceApprovalStatus, PaymentStatus, VerificationResult
      - Show detailed status combinations for disputed invoices
      
   b) **Accepted Invoices** (InvoiceStatusType = 'Accepted' or 'System Accepted'):
      - Group by: InvoiceStatusType, InvoiceApprovalStatus, PaymentStatus, VerificationResult
      - Show detailed status combinations for accepted invoices
      
   c) **Not Verified Issues**:
      - Filter: VerificationResult = 'Not Verified' or 'Unknown'
      - Count and total amount
      
   d) **Pending Approvals**:
      - Filter: InvoiceApprovalStatus = 'Initiated' or 'Pending'
      - Count and total amount
   
   Purpose: Detailed operational risk analysis with complete status visibility

OUTPUT FORMAT (JSON ONLY):
{{
  "aggregations": [
    {{
      "category": "status",
      "group_by": ["InvoiceStatusType", "InvoiceApprovalStatus", "VerificationResult"],
      "metrics": [
        {{"field": "GrandTotal", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "provider",
      "group_by": ["ProviderName"],
      "metrics": [
        {{"field": "GrandTotal", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "cost_center",
      "group_by": ["CostName", "SiteName"],
      "metrics": [
        {{"field": "GrandTotal", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "service",
      "group_by": ["ServiceName"],
      "metrics": [
        {{"field": "GrandTotal", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "financial",
      "metrics": [
        {{"field": "GrandTotal", "function": "sum"}},
        {{"field": "GrandTotal", "function": "avg"}},
        {{"field": "GrandTotal", "function": "min"}},
        {{"field": "GrandTotal", "function": "max"}},
        {{"field": "NetTotal", "function": "sum"}},
        {{"field": "TotalTax", "function": "sum"}},
        {{"field": "UsageCharge", "function": "sum"}},
        {{"field": "RentalCharge", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "time",
      "group_by": ["InvoiceDate"],
      "time_bucket": "month",
      "metrics": [
        {{"field": "GrandTotal", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "risk",
      "sub_aggregations": [
        {{
          "name": "disputed_breakdown",
          "description": "Detailed breakdown of disputed invoices",
          "filter_type": "disputed",
          "filter_field": "InvoiceStatusType",
          "filter_values": ["Disputed", "System Disputed"],
          "group_by": ["InvoiceStatusType", "InvoiceApprovalStatus", "PaymentStatus", "VerificationResult"],
          "metrics": [
            {{"field": "GrandTotal", "function": "sum"}},
            {{"field": "*", "function": "count"}}
          ]
        }},
        {{
          "name": "accepted_breakdown",
          "description": "Detailed breakdown of accepted invoices",
          "filter_type": "accepted",
          "filter_field": "InvoiceStatusType",
          "filter_values": ["Accepted", "System Accepted"],
          "group_by": ["InvoiceStatusType", "InvoiceApprovalStatus", "PaymentStatus", "VerificationResult"],
          "metrics": [
            {{"field": "GrandTotal", "function": "sum"}},
            {{"field": "*", "function": "count"}}
          ]
        }},
        {{
          "name": "not_verified",
          "description": "Invoices with verification issues",
          "filter_type": "not_verified",
          "filter_field": "VerificationResult",
          "filter_values": ["Not Verified", "Unknown"]
        }},
        {{
          "name": "pending_approval",
          "description": "Invoices pending approval",
          "filter_type": "pending_approval",
          "filter_field": "InvoiceApprovalStatus",
          "filter_values": ["Initiated", "Pending"]
        }}
      ]
    }}
  ],
  "include_categories": ["status", "provider", "cost_center", "service", "financial", "time", "risk"]
}}

STRICT RULES:
1. Only include categories where relevant columns exist in AVAILABLE COLUMNS
2. Use EXACT column names from AVAILABLE COLUMNS (case-sensitive)
3. Include top_n (5-10) for provider/cost_center/service to limit output size
4. ALWAYS include "financial" category (mandatory)
5. ALWAYS include "risk" category with sub_aggregations if status columns exist
6. For "risk" category, MUST include both "disputed_breakdown" and "accepted_breakdown" sub-aggregations
7. For time analysis, only include if a date column exists
8. Return ONLY valid JSON, no markdown formatting, no explanations
9. Do not include categories with missing columns
10. The "risk" category structure is CRITICAL - follow the sub_aggregations format exactly

Generate the aggregation spec now (JSON only):"""