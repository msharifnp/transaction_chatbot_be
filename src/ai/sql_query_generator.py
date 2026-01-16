import os
import json
import re
from datetime import datetime
from typing import Dict, Optional
from zoneinfo import ZoneInfo
from src.config.field_constant import FIELD_TYPES, FIELD_ALIASES, CANON_VALUES, MONTH_NAMES, DEFAULT_SELECT_FIELDS
from src.utils.utils import safe_json_from_model, get_zone, extract_month_and_year
from src.config.db_config import DatabaseConfig
from src.db.model_service import ModelService


class SQLQueryGenerator:
       
    def __init__(self, config: DatabaseConfig, model_service:ModelService ):
        
        self.table_name = config.full_table_name
        self.model_service = model_service
        self.enabled = model_service.is_available()
        
    def generate_sql(self, user_query: str, TenantId: str) -> Dict:
       
        # Validate TenantId is provided
        if not TenantId or TenantId.strip() == "":
            raise ValueError("TenantId is required and cannot be empty")
        
        detected_month, detected_year = extract_month_and_year(user_query)
        
        # Build complete schema context with canonical values
        schema_context = {
            "table_name": self.table_name,
            "field_types": FIELD_TYPES,
            "field_aliases": FIELD_ALIASES,
            "canonical_values": CANON_VALUES,
            "default_select_fields": DEFAULT_SELECT_FIELDS
        }
        
        # Date filter hint
        now = datetime.now(get_zone())
        date_hint = ""
        explicit_filter = None
        
        if detected_month:
            year = detected_year or now.year
            month_name = list(MONTH_NAMES.keys())[detected_month - 1].title()
            start_date = f"{year}-{detected_month:02d}-01"
            end_date = f"{year}-{detected_month+1:02d}-01" if detected_month < 12 else f"{year+1}-01-01"
            explicit_filter = f"InvoiceDate >= '{start_date}' AND InvoiceDate < '{end_date}'"
            date_hint = f"\n⚠️ MONTH DETECTED: {month_name} {year}\n⚠️ USE THIS EXACT FILTER: {explicit_filter}"

        tenant_hint = f"\n⚠️ TENANT CONTEXT: TenantId = '{TenantId}' (MANDATORY IN ALL QUERIES)"
        
        prompt = f"""You are a PostgreSQL query generator for a multi-tenant invoice database.

TASK: Convert user request to the appropriate SQL query.

DATABASE SCHEMA:
{json.dumps(schema_context, indent=2)}

CURRENT CONTEXT:
- Current date: {now.strftime("%Y-%m-%d")}{date_hint}{tenant_hint}

🔒 CRITICAL TENANT ISOLATION RULES (NON-NEGOTIABLE):
1. "TenantId" = '{TenantId}' MUST be in EVERY query's WHERE clause
2. For queries with existing WHERE: WHERE <conditions> AND "TenantId" = '{TenantId}'
3. For queries without WHERE: WHERE "TenantId" = '{TenantId}'
4. For CTEs/subqueries reading from {self.table_name}: Include TenantId filter
5. NEVER generate SQL without TenantId filter
6. NEVER use any TenantId from user input.
7. The TenantId is ALWAYS provided - use it in every query
8. InvoiceStatusType has values: 'Disputed', 'System Disputed', 'Accepted', 'System Accepted', etc.
  * 'Disputed' ≠ 'System Disputed' (these are DIFFERENT values)
  * 'Accepted' ≠ 'System Accepted' (these are DIFFERENT values)
8. When user says "disputed and pending":
  * If they mean BOTH conditions: WHERE InvoiceStatusType = 'Disputed' AND InvoiceApprovalStatus = 'Pending'
  * If they mean EITHER condition: WHERE InvoiceStatusType = 'Disputed' OR InvoiceApprovalStatus = 'Pending'
  * DEFAULT to BOTH (AND) unless user clearly means "or"
  
9. When user says "disputed invoices", check context:
  * "disputed" alone → InvoiceStatusType = 'Disputed'
  * "system disputed" → InvoiceStatusType = 'System Disputed'
  * "all disputed" → InvoiceStatusType IN ('Disputed', 'System Disputed')

QUERY GENERATION RULES:

1. CHOOSE THE RIGHT QUERY TYPE based on user request:

   A. SIMPLE SELECT (user wants to see invoices/records):
      - "show invoices", "list invoices", "find invoices", "get invoices"
      - Use DEFAULT_SELECT_FIELDS when selecting full records
      - SELECT {', '.join(DEFAULT_SELECT_FIELDS)} FROM {self.table_name} WHERE "TenantId" = '{TenantId}' AND <filters>
      
   B. AGGREGATION (user wants totals, counts, sums, averages):
      - "total amount", "sum of GrandTotal", "count invoices", "average tax"
      - SELECT <dimension>, <aggregate> FROM {self.table_name} WHERE "TenantId" = '{TenantId}' AND <filters> GROUP BY <dimension>
      - Examples:
        * "sum of GrandTotal by site" → SELECT SiteName, SUM(GrandTotal) FROM {self.table_name} WHERE "TenantId" = '{TenantId}' GROUP BY SiteName
        * "count invoices per provider" → SELECT ProviderName, COUNT(*) FROM {self.table_name} WHERE "TenantId" = '{TenantId}' GROUP BY ProviderName
        * "average tax by status" → SELECT InvoiceStatusType, AVG(TotalTax) FROM {self.table_name} WHERE "TenantId" = '{TenantId}' GROUP BY InvoiceStatusType
      
   C. DISTINCT VALUES (user wants unique list):
      - "what sites", "list all providers", "which locations", "distinct statuses"
      - SELECT DISTINCT <column> FROM {self.table_name} WHERE "TenantId" = '{TenantId}' AND <filters>
      - Example: "what sites are there" → SELECT DISTINCT SiteName FROM {self.table_name} WHERE "TenantId" = '{TenantId}'
      
   D. SUPERLATIVES - MOST/LEAST/HIGHEST/LOWEST (user wants extreme values with ties):
      - "most expensive", "highest invoice", "lowest GrandTotal", "least tax"
      - ALWAYS use DENSE_RANK with rk = 1 to handle ties
      - Use DEFAULT_SELECT_FIELDS in subquery
      
      WITH ranked AS (
          SELECT {', '.join(DEFAULT_SELECT_FIELDS)},
                 DENSE_RANK() OVER (ORDER BY <measure> DESC) AS rk
          FROM {self.table_name}
          WHERE "TenantId" = '{TenantId}' AND <filters>
      )
      SELECT * FROM ranked WHERE rk = 1
      
   E. TOP/BOTTOM N (user wants ranked results with specific number):
      - "top 5", "highest 3", "bottom 10", "first 2"
      - Use DENSE_RANK with rk <= N to handle ties
      - Use DEFAULT_SELECT_FIELDS in subquery
      
      WITH ranked AS (
          SELECT {', '.join(DEFAULT_SELECT_FIELDS)},
                 DENSE_RANK() OVER (ORDER BY <measure> DESC) AS rk
          FROM {self.table_name}
          WHERE "TenantId" = '{TenantId}' AND <filters>
      )
      SELECT * FROM ranked WHERE rk <= <N>
      
   F. TOP N PER DIMENSION (user wants ranked results per group):
      - "top 5 invoices for each site", "highest invoice per provider"
      - Use DEFAULT_SELECT_FIELDS in subquery
      
      WITH ranked AS (
          SELECT {', '.join(DEFAULT_SELECT_FIELDS)},
                 DENSE_RANK() OVER (PARTITION BY <dimension> ORDER BY <measure> DESC) AS rk
          FROM {self.table_name}
          WHERE "TenantId" = '{TenantId}' AND <filters>
      )
      SELECT * FROM ranked WHERE rk <= <N>
      
   G. NTH HIGHEST/LOWEST (user wants 2nd highest, 3rd lowest, etc.):
      - "2nd highest GrandTotal", "3rd lowest per site"
      - Use DEFAULT_SELECT_FIELDS in subquery
      
      WITH ranked AS (
          SELECT {', '.join(DEFAULT_SELECT_FIELDS)},
                 DENSE_RANK() OVER (ORDER BY <measure> DESC) AS rk
          FROM {self.table_name}
          WHERE "TenantId" = '{TenantId}' AND <filters>
      )
      SELECT * FROM ranked WHERE rk = <N>
      

2. POSTGRESQL-SPECIFIC SYNTAX:
   - Use EXTRACT(YEAR FROM date_column) instead of YEAR(date_column)
   - Use EXTRACT(MONTH FROM date_column) instead of MONTH(date_column)
   - Use LIMIT instead of TOP
   - Use :: for type casting (column::DATE)
   - Window functions: DENSE_RANK() OVER (ORDER BY col DESC)
   - String comparisons are case-sensitive
   - Use single quotes for strings ('value')

3. FIELD VALUE MATCHING:
   - 🔍 ALWAYS check user's words against "canonical_values" in schema
   - ✅ Match user input to CLOSEST canonical value (e.g., "dubi" → "Dubai", "etisalt" → "Etisalat")
   - ✅ Use EXACT canonical values in WHERE clauses (SiteName = 'Dubai')
   - 🚫 NEVER use LIKE with wildcards (LIKE '%dubi%')
   - 🧠 Use context to disambiguate:
     * "dubai site" → SiteName = 'Dubai' (NOT ProviderName)
     * "du provider" → ProviderName = 'Du' (NOT SiteName)
     * "show provider invoices in dubai" → WHERE "TenantId" = '{TenantId}' AND SiteName = 'Dubai'
     * "show du invoices" → WHERE "TenantId" = '{TenantId}' AND ProviderName = 'Du'

4. FIELD MAPPING (use these canonical field names):
   - "grand total", "total", "amount" → GrandTotal
   - "rental", "rental charge" → RentalCharge
   - "tax", "total tax" → TotalTax
   - "provider", "vendor" → ProviderName
   - "site", "site name", "location" → SiteName
   - "date", "invoice date" → invoice_date
   - "status", "invoice status" → InvoiceStatusType
   - "account", "account number" → AccountNumber
   - "cost center", "cost code" → CostCode

5. COLUMN SELECTION RULES:
   - For simple SELECT queries: Use DEFAULT_SELECT_FIELDS
     Example: SELECT {', '.join(DEFAULT_SELECT_FIELDS)} FROM {self.table_name} WHERE "TenantId" = '{TenantId}'
   
   - For window function queries (TOP N, superlatives, NTH): Use DEFAULT_SELECT_FIELDS in CTE
     Example: WITH ranked AS (SELECT {', '.join(DEFAULT_SELECT_FIELDS)}, DENSE_RANK()... WHERE "TenantId" = '{TenantId}')
   
   - For aggregations: SELECT <grouping_columns>, <aggregate_functions>
     Example: SELECT SiteName, SUM(GrandTotal) WHERE "TenantId" = '{TenantId}'...
   
   - For DISTINCT: SELECT DISTINCT <column>
     Example: SELECT DISTINCT SiteName FROM {self.table_name} WHERE "TenantId" = '{TenantId}'

6. CRITICAL RULES FOR SUPERLATIVES:
   - "most expensive", "highest", "maximum" → Use DENSE_RANK with rk = 1 (NOT ORDER BY with LIMIT 1)
   - "least expensive", "lowest", "minimum" → Use DENSE_RANK with rk = 1
   - This handles ties correctly (multiple invoices with same max/min value)
   
   ❌ WRONG: SELECT * FROM {self.table_name} ORDER BY GrandTotal DESC LIMIT 1
   ✅ CORRECT: WITH ranked AS (SELECT ..., DENSE_RANK() OVER (ORDER BY GrandTotal DESC) AS rk FROM {self.table_name} WHERE "TenantId" = '{TenantId}') SELECT * FROM ranked WHERE rk = 1

QUERY EXAMPLES (ALL WITH MANDATORY TENANT FILTER):

User: "show all invoices"
SQL: SELECT {', '.join(DEFAULT_SELECT_FIELDS)} FROM {self.table_name} WHERE "TenantId" = '{TenantId}'

User: "invoices from Dubai"
SQL: SELECT {', '.join(DEFAULT_SELECT_FIELDS)} FROM {self.table_name} WHERE "TenantId" = '{TenantId}' AND SiteName = 'Dubai'

User: "most expensive invoice" OR "highest GrandTotal" OR "find the most expensive"
SQL: WITH ranked AS (
    SELECT {', '.join(DEFAULT_SELECT_FIELDS)}, DENSE_RANK() OVER (ORDER BY GrandTotal DESC) AS rk
    FROM {self.table_name}
    WHERE "TenantId" = '{TenantId}'
)
SELECT * FROM ranked WHERE rk = 1

User: "sum of grand total by site"
SQL: SELECT SiteName, SUM(GrandTotal) AS total_amount FROM {self.table_name} WHERE "TenantId" = '{TenantId}' GROUP BY SiteName

User: "count invoices per provider"
SQL: SELECT ProviderName, COUNT(*) AS invoice_count FROM {self.table_name} WHERE "TenantId" = '{TenantId}' GROUP BY ProviderName

User: "what sites are there"
SQL: SELECT DISTINCT SiteName FROM {self.table_name} WHERE "TenantId" = '{TenantId}'

User: "top 5 invoices by grand total"
SQL: WITH ranked AS (
    SELECT {', '.join(DEFAULT_SELECT_FIELDS)}, DENSE_RANK() OVER (ORDER BY GrandTotal DESC) AS rk
    FROM {self.table_name}
    WHERE "TenantId" = '{TenantId}'
)
SELECT * FROM ranked WHERE rk <= 5

User: "highest invoice per site"
SQL: WITH ranked AS (
    SELECT {', '.join(DEFAULT_SELECT_FIELDS)}, DENSE_RANK() OVER (PARTITION BY SiteName ORDER BY GrandTotal DESC) AS rk
    FROM {self.table_name}
    WHERE "TenantId" = '{TenantId}'
)
SELECT * FROM ranked WHERE rk = 1

User: "2nd highest grand total"
SQL: WITH ranked AS (
    SELECT {', '.join(DEFAULT_SELECT_FIELDS)}, DENSE_RANK() OVER (ORDER BY GrandTotal DESC) AS rk
    FROM {self.table_name}
    WHERE "TenantId" = '{TenantId}'
)
SELECT * FROM ranked WHERE rk = 2

User: "average tax by status"
SQL: SELECT InvoiceStatusType, AVG(TotalTax) AS avg_tax FROM {self.table_name} WHERE "TenantId" = '{TenantId}' GROUP BY InvoiceStatusType

User: "disputed invoices in August 2024"
SQL: SELECT {', '.join(DEFAULT_SELECT_FIELDS)} FROM {self.table_name} WHERE "TenantId" = '{TenantId}' AND InvoiceStatusType = 'Disputed' AND invoice_date >= '2024-08-01' AND invoice_date < '2024-09-01'

User: "top 3 invoices per provider"
SQL: WITH ranked AS (
    SELECT {', '.join(DEFAULT_SELECT_FIELDS)}, DENSE_RANK() OVER (PARTITION BY ProviderName ORDER BY GrandTotal DESC) AS rk
    FROM {self.table_name}
    WHERE "TenantId" = '{TenantId}'
)
SELECT * FROM ranked WHERE rk <= 3

FILTERING RULES:
✅ CORRECT: WHERE "TenantId" = '{TenantId}' AND SiteName = 'Dubai'
✅ CORRECT: WHERE "TenantId" = '{TenantId}' AND ProviderName = 'Etisalat'
✅ CORRECT: WHERE "TenantId" = '{TenantId}' AND InvoiceStatusType = 'Disputed'
✅ CORRECT: WHERE "TenantId" = '{TenantId}' AND SiteName IN ('Dubai', 'Abu Dhabi')
❌ WRONG: WHERE SiteName = 'Dubai' (missing TenantId)
❌ WRONG: WHERE SiteName LIKE '%dubi%'
❌ WRONG: WHERE ProviderName LIKE '%etis%'

DATE HANDLING:
{f"- Month filter: {explicit_filter}" if explicit_filter else "- Use YYYY-MM-DD format"}
- String values: single quotes ('Dubai')
- Numeric values: no quotes (5000)
- Dates: single quotes ('2024-08-01')
- TenantId: single quotes ('{TenantId}')

OUTPUT FORMAT - Return ONLY this JSON:
{{
  "sql": "SELECT ... FROM {self.table_name} WHERE "TenantId" = '{TenantId}' AND ...",
  "orderby": null OR "column asc|desc",
  "top": null OR <integer if LIMIT explicitly mentioned>
}}

USER REQUEST: "{user_query}"

Analyze the request and generate the appropriate SQL query with MANDATORY "TenantId" = '{TenantId}' filter:"""
        
        try:
            response_text = self.model_service.generate_text(prompt).strip()

            if not response_text:
                raise RuntimeError("Empty response from Gemini")

            result = safe_json_from_model(response_text)

            # Validate that TenantId is in the SQL
            if TenantId not in result.get('sql', ''):
                raise RuntimeError(
                    f"Generated SQL missing required TenantId filter: {TenantId}"
                )

            result.setdefault("top", None)
            result.setdefault("orderby", None)

            sql_upper = result['sql'].upper()
            if 'GROUP BY' in sql_upper or any(agg in sql_upper for agg in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']):
                query_type = "AGGREGATION"
            elif 'DENSE_RANK()' in sql_upper or 'ROW_NUMBER()' in sql_upper:
                query_type = "WINDOW_FUNCTION"
            elif 'DISTINCT' in sql_upper:
                query_type = "DISTINCT"
            else:
                query_type = "SIMPLE_SELECT"

            print(f"\n[SQL_GEN] ✅ Generated SQL:")
            print(f"[SQL_GEN] Type: {query_type}")
            print(f"[SQL_GEN] Tenant: {TenantId}")

            return result


        except Exception as e:
            print(f"[SQL_GEN] ❌ Failed: {e}")
            raise RuntimeError(f"Gemini SQL generation failed: {e}")