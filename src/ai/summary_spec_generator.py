"""
summary_spec_generator.py
Generates aggregation specifications for summary reports using LLM.
"""

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
        """
        Generate aggregation specification based on user query and data structure.
        
        Args:
            user_query: User's summary request
            available_columns: List of column names in the dataset
            field_types: Mapping of column names to their types
            sample_rows: First 3 rows of data for context
            
        Returns:
            Aggregation specification as a dict
        """
        
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
   - invoice_status_type, invoice_approval_status, verification_result, payment_status
   - Aggregate: COUNT, SUM(grand_total), SUM(net_total)
   - Purpose: Show invoice status distribution and risk exposure

2. **Provider Analysis**:
   - providers_name, provider_name, service_provider, vendor
   - Aggregate: COUNT, SUM(grand_total), TOP 10 providers
   - Purpose: Identify vendor concentration and dependencies

3. **Cost Center / Location Analysis**:
   - cost_code, cost_name, site_name, site_location_code, city, emirate, region
   - Aggregate: COUNT, SUM(grand_total), TOP 10 locations
   - Purpose: Show spending distribution across departments/locations

4. **Service Analysis** (if relevant):
   - service_name, line_name, connection_name, service_type
   - Aggregate: COUNT, SUM(grand_total)
   - Purpose: Understand service mix and potential duplications

5. **Financial Metrics** (ALWAYS INCLUDE):
   - grand_total, net_total, total_tax, usage_charge, rental_charge, expected_amount
   - Calculate: SUM, AVG, MIN, MAX, COUNT
   - Purpose: Overall financial snapshot

6. **Time Analysis** (if date column exists):
   - invoice_date, billing_period, month, period
   - Aggregate by: MONTH or QUARTER
   - Purpose: Identify trends and seasonality

7. **Risk Indicators** (CRITICAL):
   - Disputes: invoice_status_type = 'Disputed' or 'System Disputed'
   - Verification issues: verification_result = 'Not Verified' or 'Unknown'
   - Approval delays: invoice_approval_status = 'Initiated' or 'Pending'
   - Purpose: Quantify operational risks

OUTPUT FORMAT (JSON ONLY):
{{
  "aggregations": [
    {{
      "category": "status",
      "group_by": ["invoice_status_type", "invoice_approval_status"],
      "metrics": [
        {{"field": "grand_total", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "provider",
      "group_by": ["providers_name"],
      "metrics": [
        {{"field": "grand_total", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ],
      "top_n": 10
    }},
    {{
      "category": "cost_center",
      "group_by": ["cost_name", "site_name"],
      "metrics": [
        {{"field": "grand_total", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ],
      "top_n": 10
    }},
    {{
      "category": "service",
      "group_by": ["service_name"],
      "metrics": [
        {{"field": "grand_total", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ],
      "top_n": 10
    }},
    {{
      "category": "financial",
      "metrics": [
        {{"field": "grand_total", "function": "sum"}},
        {{"field": "grand_total", "function": "avg"}},
        {{"field": "grand_total", "function": "min"}},
        {{"field": "grand_total", "function": "max"}},
        {{"field": "net_total", "function": "sum"}},
        {{"field": "total_tax", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "time",
      "group_by": ["invoice_date"],
      "time_bucket": "month",
      "metrics": [
        {{"field": "grand_total", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "risk",
      "metrics": [
        {{"field": "grand_total", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ],
      "filters": [
        {{"type": "disputed", "field": "invoice_status_type", "values": ["Disputed", "System Disputed"]}},
        {{"type": "not_verified", "field": "verification_result", "values": ["Not Verified", "Unknown"]}},
        {{"type": "pending", "field": "invoice_approval_status", "values": ["Initiated", "Pending"]}}
      ]
    }}
  ],
  "include_categories": ["status", "provider", "cost_center", "financial", "time", "risk"]
}}

STRICT RULES:
1. Only include categories where relevant columns exist in AVAILABLE COLUMNS
2. Use EXACT column names from AVAILABLE COLUMNS (case-sensitive)
3. Include top_n (5-10) for provider/cost_center/service to limit output size
4. ALWAYS include "financial" category (mandatory)
5. ALWAYS include "risk" category if status/verification columns exist
6. For time analysis, only include if a date column exists
7. Return ONLY valid JSON, no markdown formatting, no explanations
8. Do not include categories with missing columns

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
    
    def _get_default_spec(self, columns: List[str], field_types: Dict[str, str]) -> Dict[str, Any]:
        """
        Fallback default aggregation spec when LLM fails.
        
        Args:
            columns: Available column names
            field_types: Column type mappings
            
        Returns:
            Default aggregation specification
        """
        spec = {"aggregations": [], "include_categories": []}
        
        # Status aggregation
        if "invoice_status_type" in columns:
            status_agg = {
                "category": "status",
                "group_by": ["invoice_status_type"],
                "metrics": [
                    {"field": "grand_total", "function": "sum"},
                    {"field": "*", "function": "count"}
                ]
            }
            
            # Add approval status if available
            if "invoice_approval_status" in columns:
                status_agg["group_by"].append("invoice_approval_status")
            
            spec["aggregations"].append(status_agg)
            spec["include_categories"].append("status")
        
        # Provider aggregation
        provider_col = None
        for col in ["providers_name", "provider_name", "service_provider", "vendor"]:
            if col in columns:
                provider_col = col
                break
        
        if provider_col:
            spec["aggregations"].append({
                "category": "provider",
                "group_by": [provider_col],
                "metrics": [
                    {"field": "grand_total", "function": "sum"},
                    {"field": "*", "function": "count"}
                ],
                "top_n": 10
            })
            spec["include_categories"].append("provider")
        
        # Cost center aggregation
        cost_col = None
        for col in ["cost_name", "cost_code", "site_name", "site_location_code", "city"]:
            if col in columns:
                cost_col = col
                break
        
        if cost_col:
            spec["aggregations"].append({
                "category": "cost_center",
                "group_by": [cost_col],
                "metrics": [
                    {"field": "grand_total", "function": "sum"},
                    {"field": "*", "function": "count"}
                ],
                "top_n": 10
            })
            spec["include_categories"].append("cost_center")
        
        # Service aggregation
        if "service_name" in columns:
            spec["aggregations"].append({
                "category": "service",
                "group_by": ["service_name"],
                "metrics": [
                    {"field": "grand_total", "function": "sum"},
                    {"field": "*", "function": "count"}
                ],
                "top_n": 10
            })
            spec["include_categories"].append("service")
        
        # Financial metrics (ALWAYS included)
        financial_metrics = [
            {"field": "grand_total", "function": "sum"},
            {"field": "grand_total", "function": "avg"},
            {"field": "grand_total", "function": "min"},
            {"field": "grand_total", "function": "max"},
            {"field": "*", "function": "count"}
        ]
        
        if "net_total" in columns:
            financial_metrics.append({"field": "net_total", "function": "sum"})
        if "total_tax" in columns:
            financial_metrics.append({"field": "total_tax", "function": "sum"})
        
        spec["aggregations"].append({
            "category": "financial",
            "metrics": financial_metrics
        })
        spec["include_categories"].append("financial")
        
        # Time aggregation
        date_col = None
        for col in ["invoice_date", "billing_period", "date", "month"]:
            if col in columns:
                date_col = col
                break
        
        if date_col:
            spec["aggregations"].append({
                "category": "time",
                "group_by": [date_col],
                "time_bucket": "month",
                "metrics": [
                    {"field": "grand_total", "function": "sum"},
                    {"field": "*", "function": "count"}
                ]
            })
            spec["include_categories"].append("time")
        
        # Risk aggregation
        risk_filters = []
        
        if "invoice_status_type" in columns:
            risk_filters.append({
                "type": "disputed",
                "field": "invoice_status_type",
                "values": ["Disputed", "System Disputed"]
            })
        
        if "verification_result" in columns:
            risk_filters.append({
                "type": "not_verified",
                "field": "verification_result",
                "values": ["Not Verified", "Unknown"]
            })
        
        if "invoice_approval_status" in columns:
            risk_filters.append({
                "type": "pending",
                "field": "invoice_approval_status",
                "values": ["Initiated", "Pending"]
            })
        
        if risk_filters:
            spec["aggregations"].append({
                "category": "risk",
                "metrics": [
                    {"field": "grand_total", "function": "sum"},
                    {"field": "*", "function": "count"}
                ],
                "filters": risk_filters
            })
            spec["include_categories"].append("risk")
        
        print(f"[SPEC_GEN] ✅ Using default spec with {len(spec['aggregations'])} aggregations")
        
        return spec