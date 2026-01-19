import json
from typing import List, Dict, Any


class SummarySpecGenerator:
    """Generates aggregation specifications for summary reports using LLM."""
    
    def __init__(self, model_service):
        """
        Initialize the spec generator.
        
        Args:
            model_service: Service for calling the LLM (Gemini/Claude)
        """
        self.model_service = model_service
    
    def generate_spec(
        self,
        user_query: str,
        available_columns: List[str],
        field_types: Dict[str, str],
        sample_rows: List[Dict]
    ) -> Dict[str, Any]:
      
        prompt = f"""You are a data aggregation specialist. Analyze the user's summary request and generate an aggregation specification.

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

        try:
            response = self.model_service.generate_text(prompt)
            
            # Clean JSON response
            response = response.strip()
            
            # Remove markdown code fences if present
            if response.startswith("```json"):
                response = response[7:]
            elif response.startswith("```"):
                response = response[3:]
            
            if response.endswith("```"):
                response = response[:-3]
            
            response = response.strip()
            
            # Parse JSON
            spec = json.loads(response)
            
            # Validate spec structure
            if "aggregations" not in spec:
                raise ValueError("Missing 'aggregations' key in spec")
            
            if not isinstance(spec["aggregations"], list):
                raise ValueError("'aggregations' must be a list")
            
            # Validate each aggregation has required fields
            for agg in spec["aggregations"]:
                if "category" not in agg:
                    raise ValueError(f"Aggregation missing 'category': {agg}")
            
            print(f"[SPEC_GEN] ✅ Generated spec with {len(spec['aggregations'])} aggregations")
            
            return spec
            
        except json.JSONDecodeError as e:
            print(f"[SPEC_GEN] ❌ JSON parse error: {e}")
            print(f"[SPEC_GEN] Response preview: {response[:500]}")
            # Return default spec as fallback
            return self._get_default_spec(available_columns, field_types)
            
        except Exception as e:
            print(f"[SPEC_GEN] ❌ Error generating spec: {e}")
            # Return default spec as fallback
            return self._get_default_spec(available_columns, field_types)
    
    def get_default_spec(self, columns: List[str], field_types: Dict[str, str]) -> Dict[str, Any]:
        """
        Fallback default aggregation spec when LLM fails.
        
        Args:
            columns: Available column names
            field_types: Column type mappings
            
        Returns:
            Default aggregation specification with enhanced risk breakdown
        """
        spec = {"aggregations": [], "include_categories": []}
        
        # Status aggregation
        status_cols = []
        for col in ["InvoiceStatusType", "InvoiceApprovalStatus", "VerificationResult"]:
            if col in columns:
                status_cols.append(col)
        
        if status_cols:
            spec["aggregations"].append({
                "category": "status",
                "group_by": status_cols,
                "metrics": [
                    {"field": "GrandTotal", "function": "sum"},
                    {"field": "*", "function": "count"}
                ]
            })
            spec["include_categories"].append("status")
        
        # Provider aggregation
        provider_col = None
        for col in ["ProviderName", "provider_name", "service_provider", "vendor"]:
            if col in columns:
                provider_col = col
                break
        
        if provider_col:
            spec["aggregations"].append({
                "category": "provider",
                "group_by": [provider_col],
                "metrics": [
                    {"field": "GrandTotal", "function": "sum"},
                    {"field": "*", "function": "count"}
                ],
                "top_n": 10
            })
            spec["include_categories"].append("provider")
        
        # Cost center aggregation
        cost_cols = []
        for col in ["CostName", "SiteName", "cost_name", "site_name"]:
            if col in columns:
                cost_cols.append(col)
        
        if cost_cols:
            spec["aggregations"].append({
                "category": "cost_center",
                "group_by": cost_cols[:2],  # Max 2 columns
                "metrics": [
                    {"field": "GrandTotal", "function": "sum"},
                    {"field": "*", "function": "count"}
                ],
                "top_n": 10
            })
            spec["include_categories"].append("cost_center")
        
        # Service aggregation
        if "ServiceName" in columns:
            spec["aggregations"].append({
                "category": "service",
                "group_by": ["ServiceName"],
                "metrics": [
                    {"field": "GrandTotal", "function": "sum"},
                    {"field": "*", "function": "count"}
                ],
                "top_n": 10
            })
            spec["include_categories"].append("service")
        
        # Financial metrics (ALWAYS included)
        financial_metrics = [
            {"field": "GrandTotal", "function": "sum"},
            {"field": "GrandTotal", "function": "avg"},
            {"field": "GrandTotal", "function": "min"},
            {"field": "GrandTotal", "function": "max"},
            {"field": "*", "function": "count"}
        ]
        
        for col in ["NetTotal", "TotalTax", "UsageCharge", "RentalCharge"]:
            if col in columns:
                financial_metrics.append({"field": col, "function": "sum"})
        
        spec["aggregations"].append({
            "category": "financial",
            "metrics": financial_metrics
        })
        spec["include_categories"].append("financial")
        
        # Time aggregation
        date_col = None
        for col in ["InvoiceDate", "BillReceiveDate", "invoice_date", "billing_period"]:
            if col in columns:
                date_col = col
                break
        
        if date_col:
            spec["aggregations"].append({
                "category": "time",
                "group_by": [date_col],
                "time_bucket": "month",
                "metrics": [
                    {"field": "GrandTotal", "function": "sum"},
                    {"field": "*", "function": "count"}
                ]
            })
            spec["include_categories"].append("time")
        
        # Risk aggregation with enhanced breakdown
        has_status = "InvoiceStatusType" in columns
        has_approval = "InvoiceApprovalStatus" in columns
        has_verification = "VerificationResult" in columns
        has_payment = "PaymentStatus" in columns
        
        if has_status:
            group_fields = ["InvoiceStatusType"]
            if has_approval:
                group_fields.append("InvoiceApprovalStatus")
            if has_payment:
                group_fields.append("PaymentStatus")
            if has_verification:
                group_fields.append("VerificationResult")
            
            sub_aggs = [
                {
                    "name": "disputed_breakdown",
                    "description": "Detailed breakdown of disputed invoices",
                    "filter_type": "disputed",
                    "filter_field": "InvoiceStatusType",
                    "filter_values": ["Disputed", "System Disputed"],
                    "group_by": group_fields,
                    "metrics": [
                        {"field": "GrandTotal", "function": "sum"},
                        {"field": "*", "function": "count"}
                    ]
                },
                {
                    "name": "accepted_breakdown",
                    "description": "Detailed breakdown of accepted invoices",
                    "filter_type": "accepted",
                    "filter_field": "InvoiceStatusType",
                    "filter_values": ["Accepted", "System Accepted"],
                    "group_by": group_fields,
                    "metrics": [
                        {"field": "GrandTotal", "function": "sum"},
                        {"field": "*", "function": "count"}
                    ]
                }
            ]
            
            # Add verification filter if column exists
            if has_verification:
                sub_aggs.append({
                    "name": "not_verified",
                    "description": "Invoices with verification issues",
                    "filter_type": "not_verified",
                    "filter_field": "VerificationResult",
                    "filter_values": ["Not Verified", "Unknown"]
                })
            
            # Add approval filter if column exists
            if has_approval:
                sub_aggs.append({
                    "name": "pending_approval",
                    "description": "Invoices pending approval",
                    "filter_type": "pending_approval",
                    "filter_field": "InvoiceApprovalStatus",
                    "filter_values": ["Initiated", "Pending"]
                })
            
            spec["aggregations"].append({
                "category": "risk",
                "sub_aggregations": sub_aggs
            })
            spec["include_categories"].append("risk")
        
        print(f"[SPEC_GEN] ✅ Using default spec with {len(spec['aggregations'])} aggregations")
        
        return spec