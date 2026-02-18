import json
from typing import List, Dict, Any
from src.models.model_service import ModelService

class SummarySpecGenerator:
    """Generates aggregation specifications for summary reports using LLM."""
    
    PURPOSE = "Technical"
    
    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)
    
    def generate_spec(
        self,
        user_query: str,
        available_columns: List[str],
        field_types: Dict[str, str],
        sample_rows: List[Dict]
    ) -> Dict[str, Any]:
        
        # Categorize columns by type
        date_cols = [col for col, typ in field_types.items() if typ == "date" and col in available_columns]
        numeric_cols = [col for col, typ in field_types.items() if typ == "number" and col in available_columns]
        string_cols = [col for col, typ in field_types.items() if typ == "string" and col in available_columns]
        
        # Identify specific column categories
        amount_cols = [col for col in numeric_cols if any(kw in col.lower() for kw in ["amount", "total", "price", "cost", "value"])]
        quantity_cols = [col for col in numeric_cols if any(kw in col.lower() for kw in ["quantity", "count", "volume"])]
        
        # Build column context
        column_context = f"""
AVAILABLE COLUMNS BY TYPE:

Date Columns: {', '.join(date_cols) if date_cols else 'None'}
Amount/Cost Columns: {', '.join(amount_cols[:10]) if amount_cols else 'None'}
Quantity Columns: {', '.join(quantity_cols[:10]) if quantity_cols else 'None'}
All Numeric Columns: {', '.join(numeric_cols[:15])}
Categorical Columns: {', '.join(string_cols[:20])}
"""
      
        prompt = f"""You are a purchasing/procurement data aggregation specialist. Analyze the user's summary request and generate an aggregation specification.

USER REQUEST: "{user_query}"

{column_context}

FIELD TYPES:
{json.dumps(field_types, indent=2)}

SAMPLE DATA (first 3 rows):
{json.dumps(sample_rows, indent=2)}

TASK: Generate a JSON specification for aggregating purchasing/procurement data for executive summary analysis.

AGGREGATION CATEGORIES TO CONSIDER:

1. **Supplier Performance Analysis** (if supplier columns exist):
   - SupplierName, Supplier, SupplierLocation
   - Aggregate: COUNT, SUM(TotalOpenAmount), SUM(TotalReceiptAmount)
   - Group by: SupplierName (primary) and optionally SupplierLocation
   - TOP 10 suppliers by spend
   - Purpose: Identify vendor concentration, spending patterns, and supplier dependencies

2. **Buyer/Purchaser Analysis** (if buyer columns exist):
   - Buyer, purchasing_agent, procurement_officer
   - Aggregate: COUNT, SUM(TotalOpenAmount), AVG(TotalOpenAmount)
   - TOP 10 buyers by order volume
   - Purpose: Show purchasing activity distribution across buyers

3. **Location/Cost Center Analysis** (if location columns exist):
   - ShipToLocation, CompanyCode, cost_center, location
   - Aggregate: COUNT, SUM(TotalOpenAmount), SUM(TotalReceiptAmount)
   - TOP 10 locations by spend
   - Purpose: Show spending distribution across business units/locations

4. **Item/Product Analysis** (if item columns exist):
   - Item, ItemNumber, product_code, material
   - Aggregate: COUNT, SUM(DeliveryQuantity), SUM(TotalOpenAmount)
   - TOP 10-20 items by quantity or value
   - Purpose: Identify high-volume items and spending concentration

5. **Order Type/Status Analysis** (if relevant):
   - OrderType (PO, BO, etc.), order_status, approval_status
   - Aggregate: COUNT, SUM(TotalOpenAmount) by type
   - Group by: OrderType and optionally status fields
   - Purpose: Show order mix and processing status

6. **Financial Metrics** (ALWAYS INCLUDE):
   - TotalOpenAmount, TotalReceiptAmount, UnitPrice, DeliveryQuantity, ReceivedQuantity, OpenQuantity
   - Calculate: SUM, AVG, MIN, MAX, COUNT
   - Purpose: Overall financial and operational snapshot
   - Include: total spend, average order value, total quantities, open amounts

7. **Time Analysis** (if date column exists):
   - DatePlaced, RequiredDate, ConfirmedDate, ClosedDate
   - Aggregate by: MONTH, QUARTER, or YEAR
   - Metrics: SUM(TotalOpenAmount), COUNT orders
   - Purpose: Identify spending trends, seasonality, and order frequency

8. **Delivery Performance** (if delivery columns exist):
   - DeliveryQuantity vs ReceivedQuantity, OpenQuantity
   - RequiredDate vs ConfirmedDate (for on-time analysis)
   - Aggregate: SUM quantities, COUNT deliveries, AVG fulfillment rate
   - Purpose: Track delivery performance and backlog

9. **Operational Insights**:
   - LineCount (lines per order), order complexity
   - Order frequency by supplier/buyer
   - Average lead times (if date fields available)
   - Purpose: Process efficiency and operational metrics

OUTPUT FORMAT (JSON ONLY):
{{
  "aggregations": [
    {{
      "category": "supplier",
      "group_by": ["SupplierName"],
      "metrics": [
        {{"field": "TotalOpenAmount", "function": "sum"}},
        {{"field": "TotalReceiptAmount", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ],
      "top_n": 10
    }},
    {{
      "category": "buyer",
      "group_by": ["Buyer"],
      "metrics": [
        {{"field": "TotalOpenAmount", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ],
      "top_n": 10
    }},
    {{
      "category": "location",
      "group_by": ["ShipToLocation", "CompanyCode"],
      "metrics": [
        {{"field": "TotalOpenAmount", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ],
      "top_n": 10
    }},
    {{
      "category": "item",
      "group_by": ["Item"],
      "metrics": [
        {{"field": "DeliveryQuantity", "function": "sum"}},
        {{"field": "TotalOpenAmount", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ],
      "top_n": 20
    }},
    {{
      "category": "order_type",
      "group_by": ["OrderType"],
      "metrics": [
        {{"field": "TotalOpenAmount", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "financial",
      "metrics": [
        {{"field": "TotalOpenAmount", "function": "sum"}},
        {{"field": "TotalOpenAmount", "function": "avg"}},
        {{"field": "TotalOpenAmount", "function": "min"}},
        {{"field": "TotalOpenAmount", "function": "max"}},
        {{"field": "TotalReceiptAmount", "function": "sum"}},
        {{"field": "DeliveryQuantity", "function": "sum"}},
        {{"field": "ReceivedQuantity", "function": "sum"}},
        {{"field": "OpenQuantity", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "time",
      "group_by": ["DatePlaced"],
      "time_bucket": "month",
      "metrics": [
        {{"field": "TotalOpenAmount", "function": "sum"}},
        {{"field": "*", "function": "count"}}
      ]
    }},
    {{
      "category": "delivery",
      "metrics": [
        {{"field": "DeliveryQuantity", "function": "sum"}},
        {{"field": "ReceivedQuantity", "function": "sum"}},
        {{"field": "OpenQuantity", "function": "sum"}}
      ],
      "derived_metrics": [
        {{"name": "fulfillment_rate", "formula": "ReceivedQuantity / DeliveryQuantity"}},
        {{"name": "backlog_percentage", "formula": "OpenQuantity / DeliveryQuantity"}}
      ]
    }},
    {{
      "category": "operational",
      "metrics": [
        {{"field": "LineCount", "function": "avg"}},
        {{"field": "LineCount", "function": "sum"}}
      ]
    }}
  ],
  "include_categories": ["supplier", "buyer", "location", "item", "order_type", "financial", "time", "delivery", "operational"]
}}

STRICT RULES:
1. Only include categories where relevant columns exist in AVAILABLE COLUMNS
2. Use EXACT column names from AVAILABLE COLUMNS (case-sensitive: TenantId, OrderNumber, etc.)
3. Include top_n (5-20) for supplier/buyer/location/item to limit output size
4. ALWAYS include "financial" category (mandatory)
5. For time analysis, only include if a date column exists (DatePlaced, RequiredDate, etc.)
6. Return ONLY valid JSON, no markdown formatting, no explanations
7. Do not include categories with missing columns
8. Use purchasing-specific field names (TotalOpenAmount, not GrandTotal; SupplierName, not ProviderName)
9. For amounts: use TotalOpenAmount and TotalReceiptAmount
10. For quantities: use DeliveryQuantity, ReceivedQuantity, OpenQuantity
11. Adapt category names based on available data (e.g., use "blanket_order" category if BlanketOrder-specific columns exist)

COLUMN NAME MAPPINGS (adapt based on what's available):
- Supplier info: SupplierName, Supplier, SupplierLocation
- Amounts: TotalOpenAmount, TotalReceiptAmount, UnitPrice
- Quantities: DeliveryQuantity, ReceivedQuantity, OpenQuantity
- Dates: DatePlaced, RequiredDate, ConfirmedDate, ClosedDate
- Location: ShipToLocation, CompanyCode
- Items: Item, Revision
- Orders: OrderNumber, OrderType, OrderLine

Generate the aggregation spec now (JSON only):"""

        try:
            response = self.model_service.generate(self.PURPOSE, prompt).strip()
            
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
            
            print(f"[SUMMARY_SPEC_GEN] ✅ Generated spec with {len(spec['aggregations'])} aggregations")
            
            return spec
            
        except json.JSONDecodeError as e:
            print(f"[SUMMARY_SPEC_GEN] ❌ JSON parse error: {e}")
            print(f"[SUMMARY_SPEC_GEN] Response preview: {response[:500]}")
            # Return default spec as fallback
            return self.get_default_spec(available_columns, field_types, sample_rows)
            
        except Exception as e:
            print(f"[SUMMARY_SPEC_GEN] ❌ Error generating spec: {e}")
            # Return default spec as fallback
            return self.get_default_spec(available_columns, field_types, sample_rows)
    
    def get_default_spec(
    self, 
    user_query: str,
    available_columns: List[str], 
    field_types: Dict[str, str],
    sample_rows: List[Dict] = None
) -> Dict[str, Any]:
        """
        Fallback default aggregation spec when LLM fails.
        
        Args:
            user_query: User's query (for context, optional use)
            available_columns: Available column names
            field_types: Column type mappings
            sample_rows: Sample data rows (optional)
            
        Returns:
            Default aggregation specification for purchasing data
        """
        spec = {"aggregations": [], "include_categories": []}
        
        # Supplier aggregation
        supplier_col = None
        for col in ["SupplierName", "Supplier"]:
            if col in available_columns:
                supplier_col = col
                break
        
        if supplier_col:
            spec["aggregations"].append({
                "category": "supplier",
                "group_by": [supplier_col],
                "metrics": [
                    {"field": "TotalOpenAmount", "function": "sum"} if "TotalOpenAmount" in available_columns else {"field": "*", "function": "count"},
                    {"field": "TotalReceiptAmount", "function": "sum"} if "TotalReceiptAmount" in available_columns else {"field": "*", "function": "count"},
                    {"field": "*", "function": "count"}
                ],
                "top_n": 10
            })
            spec["include_categories"].append("supplier")
        
        # Buyer aggregation
        if "Buyer" in available_columns:
            spec["aggregations"].append({
                "category": "buyer",
                "group_by": ["Buyer"],
                "metrics": [
                    {"field": "TotalOpenAmount", "function": "sum"} if "TotalOpenAmount" in available_columns else {"field": "*", "function": "count"},
                    {"field": "*", "function": "count"}
                ],
                "top_n": 10
            })
            spec["include_categories"].append("buyer")
        
        # Location aggregation
        location_cols = []
        for col in ["ShipToLocation", "CompanyCode"]:
            if col in available_columns:
                location_cols.append(col)
        
        if location_cols:
            spec["aggregations"].append({
                "category": "location",
                "group_by": location_cols[:2],  # Max 2 columns
                "metrics": [
                    {"field": "TotalOpenAmount", "function": "sum"} if "TotalOpenAmount" in available_columns else {"field": "*", "function": "count"},
                    {"field": "*", "function": "count"}
                ],
                "top_n": 10
            })
            spec["include_categories"].append("location")
        
        # Item aggregation
        if "Item" in available_columns:
            spec["aggregations"].append({
                "category": "item",
                "group_by": ["Item"],
                "metrics": [
                    {"field": "DeliveryQuantity", "function": "sum"} if "DeliveryQuantity" in available_columns else {"field": "*", "function": "count"},
                    {"field": "TotalOpenAmount", "function": "sum"} if "TotalOpenAmount" in available_columns else {"field": "*", "function": "count"},
                    {"field": "*", "function": "count"}
                ],
                "top_n": 20
            })
            spec["include_categories"].append("item")
        
        # Order type aggregation
        if "OrderType" in available_columns:
            spec["aggregations"].append({
                "category": "order_type",
                "group_by": ["OrderType"],
                "metrics": [
                    {"field": "TotalOpenAmount", "function": "sum"} if "TotalOpenAmount" in available_columns else {"field": "*", "function": "count"},
                    {"field": "*", "function": "count"}
                ]
            })
            spec["include_categories"].append("order_type")
        
        # Financial metrics (ALWAYS included)
        financial_metrics = [{"field": "*", "function": "count"}]
        
        for col in ["TotalOpenAmount", "TotalReceiptAmount"]:
            if col in available_columns:
                financial_metrics.extend([
                    {"field": col, "function": "sum"},
                    {"field": col, "function": "avg"},
                    {"field": col, "function": "min"},
                    {"field": col, "function": "max"}
                ])
        
        for col in ["DeliveryQuantity", "ReceivedQuantity", "OpenQuantity", "UnitPrice"]:
            if col in available_columns:
                financial_metrics.append({"field": col, "function": "sum"})
        
        spec["aggregations"].append({
            "category": "financial",
            "metrics": financial_metrics
        })
        spec["include_categories"].append("financial")
        
        # Time aggregation
        date_col = None
        for col in ["DatePlaced", "RequiredDate", "ConfirmedDate", "ClosedDate"]:
            if col in available_columns:
                date_col = col
                break
        
        if date_col:
            spec["aggregations"].append({
                "category": "time",
                "group_by": [date_col],
                "time_bucket": "month",
                "metrics": [
                    {"field": "TotalOpenAmount", "function": "sum"} if "TotalOpenAmount" in available_columns else {"field": "*", "function": "count"},
                    {"field": "*", "function": "count"}
                ]
            })
            spec["include_categories"].append("time")
        
        # Delivery performance
        has_delivery_cols = any(col in available_columns for col in ["DeliveryQuantity", "ReceivedQuantity", "OpenQuantity"])
        if has_delivery_cols:
            delivery_metrics = []
            for col in ["DeliveryQuantity", "ReceivedQuantity", "OpenQuantity"]:
                if col in available_columns:
                    delivery_metrics.append({"field": col, "function": "sum"})
            
            spec["aggregations"].append({
                "category": "delivery",
                "metrics": delivery_metrics
            })
            spec["include_categories"].append("delivery")
        
        # Operational metrics
        if "LineCount" in available_columns:
            spec["aggregations"].append({
                "category": "operational",
                "metrics": [
                    {"field": "LineCount", "function": "avg"},
                    {"field": "LineCount", "function": "sum"}
                ]
            })
            spec["include_categories"].append("operational")
        
        print(f"[SUMMARY_SPEC_GEN] ✅ Using default spec with {len(spec['aggregations'])} aggregations")
        
        return spec