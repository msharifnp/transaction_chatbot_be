# import os
# import json
# import re
# from datetime import datetime
# from typing import Dict, Optional
# from zoneinfo import ZoneInfo
# from src.utils.utils import safe_json_from_model, get_zone, extract_month_and_year
# from src.config.db_config import DatabaseConfig
# from src.models.model_service import ModelService
# from src.config.field_constant import TABLE_SCHEMAS

# class SQLQueryGenerator:
    
#     PURPOSE = "Technical"
       
#     def __init__(self, model_service: ModelService):
#         self.model_service = model_service
#         self.enabled = model_service.has_purpose(self.PURPOSE)
    
#     def generate_sql(self, user_query: str, TenantId: str) -> Dict:

#         if not TenantId or TenantId.strip() == "":
#             raise ValueError("TenantId is required")

#         schema_context = {
#             "tables": TABLE_SCHEMAS
#         }

#         prompt = f"""You are an expert Oracle SQL query generator. Generate accurate Oracle SQL queries based on user questions using the provided table schemas.


# AVAILABLE TABLES: 
# {json.dumps(schema_context, separators=(',', ':'))}

# ==================================================================================
# STEP 0 — MANDATORY PRE-GENERATION CHECKLIST (complete IN ORDER before writing SQL):
# ==================================================================================
# Before writing a single line of SQL, mentally complete ALL steps below:

# STEP 0-A  READ THE QUESTION CAREFULLY
#   - Identify every entity the user mentions: item, vendor, location, date range,
#     quantity type, value, status, etc.
#   - Identify grouping intent: "by location", "by vendor", "by master location",
#     "total", "each", "all", etc.
#   - Identify filter intent: "below minimum", "last 6 months", "for item X", etc.
#   - Do NOT assume columns or tables — derive them strictly from question keywords
#     and the schema. Same question asked twice must always produce the same SQL.

# STEP 0-B  MAP QUESTION TO TABLE(S)
#   - Read every table's "description" in AVAILABLE TABLES.
#   - Select ONLY the table(s) whose description clearly matches the question subject.
#   - Do NOT use a table unless it is unambiguously relevant.

# STEP 0-C  IDENTIFY REQUIRED COLUMNS (schema-grounded, no invention)
#   - Open the matched table's "columns" object.
#   - Pick ONLY the columns needed to answer the question.
#   - Every column in the SELECT, WHERE, GROUP BY, ORDER BY MUST exist verbatim
#     in the schema. Do NOT invent, abbreviate, or rename columns.
#   - Map each question keyword deterministically to the same column every time:
#       "quantity on hand"  → always "ITEM_DETAIL_ON_HAND_QUANTITY"
#       "minimum stock"     → always "Minimum Stock"
#       "master location"   → always "MASTER_LOCATION_NAME" / "Item ML On Hand Quantity"
#       "location"          → always "ITEM_DETAIL_LOCATION" / "LOCATION_NAME"
#       etc.

# STEP 0-D  DETERMINE THE DATE COLUMN (strict hierarchy)
#   Priority 1 — User explicitly names a date column (e.g. "voucher date",
#                "delivery date", "accepted date") → use ONLY that column.
#   Priority 2 — User mentions a time range but NO specific column
#                → use the DEFAULT DATE COLUMN stated in that table's schema
#                  "description" field. Never substitute another date column.
#   Default date columns (do NOT override unless Priority 1 applies):
#     - IVINSPECTIONHISTORY  : "Date Received"      (VARCHAR2 'YYYY-MM-DD')
#     - XV_REC_HIS_VW        : "Received Date"       (DATE)
#     - ITRN_HISTORY_VW      : "Transaction Date"    (DATE)
#     - IVONHANDBYLOCATION   : "COST_AS_OF_DATE"     (VARCHAR2 'YYYY-MM-DD')

# STEP 0-E  ORDER SELECTED COLUMNS TO MATCH SCHEMA ORDER (mandatory)
#   - Note the order each column appears in the table's "columns" definition
#     (top = first, bottom = last).
#   - Your SELECT list MUST follow that exact same top-to-bottom order.
#   - Example: schema order is [A, B, C, D, E, F], you need A, C, D, F
#     → correct:   SELECT "A", "C", "D", "F"
#     → incorrect: SELECT "C", "A", "F", "D"
#   - Derived/calculated columns (e.g. "Shortage Quantity") go AFTER all real
#     schema columns in the SELECT list.
#   - This rule applies to every query — simple, aggregated, joined, CTE, ranked.

# STEP 0-F  WRITE THE SQL
#   - Only after completing steps A–E above, write the final SQL.
#   - The SQL must be deterministic: the same question always produces the same query.
#   - Include tenant filter, correct date handling, correct aggregation/grouping
#     based solely on the question intent derived in Step 0-A.
# ==================================================================================

# CRITICAL TENANT FILTERING RULE:
# ==================================================================================
# EVERY query MUST include tenant filtering in the WHERE clause using one of these columns:
# - If table has "CCN" column: WHERE "CCN" = '{TenantId}'
# - If table has "Company" column: WHERE "Company" = '{TenantId}'
# - If table has "Item Company" column: WHERE "Item Company" = '{TenantId}'
# - If table has "Company Code" column: WHERE "Company Code" = '{TenantId}'

# This applies to ALL queries: simple SELECT, aggregations, JOINs, subqueries, CTEs, and ranking queries.
# ==================================================================================

# CRITICAL COLUMN SELECTION RULE:
# ==================================================================================
# SELECT COLUMNS PRECISELY BASED ON USER QUESTION KEYWORDS:

# INVENTORY ON-HAND QUANTITY COLUMNS (IVONHANDBYLOCATION):
# - If user asks about "location" or "warehouse" or "site" level:
#   → Use: "ITEM_DETAIL_LOCATION", "ITEM_DETAIL_ON_HAND_QUANTITY"
#   → This shows quantity at EACH specific location
  
# - If user asks about "master location" or "ML" level:
#   → Use: "MASTER_LOCATION_NAME" or "ML", "Item ML On Hand Quantity"
#   → This shows aggregated quantity at master location level
  
# - If user asks about "total" or "company-wide" or doesn't specify location:
#   → Use: SUM("ITEM_DETAIL_ON_HAND_QUANTITY") GROUP BY "Item"
#   → This shows total across all locations

# - If user asks about "each location" or "all locations":
#   → Use: "Item", "ITEM_DETAIL_LOCATION", "ITEM_DETAIL_ON_HAND_QUANTITY"
#   → Show individual location details

# STOCK LEVEL COMPARISON COLUMNS:
# - "Minimum Stock" - Minimum stock level threshold
# - "Maximum Stock" - Maximum stock level threshold
# - "Reorder Point" - Reorder point threshold
# - "Safety Stock" - Safety stock level

# LOCATION COLUMNS:
# - "ITEM_DETAIL_LOCATION" - Specific location/bin
# - "LOCATION_NAME" - Location description
# - "MASTER_LOCATION_NAME" - Master location name
# - "ML" - Master location code

# VALUE COLUMNS:
# - "EXTENDED_AMOUNT" - Total inventory value at location
# - "UNIT_COST" - Unit cost per item

# DATE COLUMNS:
# - "COST_AS_OF_DATE" - Cost calculation date (VARCHAR2 format 'YYYY-MM-DD')

# COLUMN SELECTION EXAMPLES:

# User asks: "items below minimum stock"
# → Select: "Item", "ITEM_DESCRIPTION", "Minimum Stock", SUM("ITEM_DETAIL_ON_HAND_QUANTITY")
# → Group by: "Item", "ITEM_DESCRIPTION", "Minimum Stock"
# → Don't include location columns (user didn't ask for location breakdown)

# User asks: "items below minimum stock by location"
# → Select: "Item", "ITEM_DESCRIPTION", "ITEM_DETAIL_LOCATION", "ITEM_DETAIL_ON_HAND_QUANTITY", "Minimum Stock"
# → Don't group - show each location separately
# → Include both location and quantity columns

# User asks: "items below minimum at master location level"
# → Select: "Item", "ITEM_DESCRIPTION", "MASTER_LOCATION_NAME", "Item ML On Hand Quantity", "Minimum Stock"
# → Don't group - use ML-level columns directly

# User asks: "total inventory value by location"
# → Select: "ITEM_DETAIL_LOCATION", SUM("EXTENDED_AMOUNT")
# → Group by: "ITEM_DETAIL_LOCATION"

# User asks: "inventory on hand for item X"
# → Select: "Item", "ITEM_DESCRIPTION", "ITEM_DETAIL_LOCATION", "ITEM_DETAIL_ON_HAND_QUANTITY"
# → Don't aggregate - show all locations for that item
# ==================================================================================

# CRITICAL DEFAULT DATE COLUMN RULE:
# ==================================================================================
# When user mentions a time range WITHOUT specifying which date column to use, 
# use the DEFAULT DATE COLUMN for that table:

# - IVINSPECTIONHISTORY: Use "Date Received" (VARCHAR2)
# - XV_REC_HIST: Use "REC_DATE" (DATE)
# - XV_REC_HIS_VW: Use "Received Date" (DATE)
# - ITRN_HISTORY_VW: Use "Transaction Date" (DATE)
# - IVONHANDBYLOCATION: Use "COST_AS_OF_DATE" (VARCHAR2)

# If user specifies a date column (e.g. "voucher date", "accepted date", "delivery date"),
# use ONLY that specific column — do NOT fall back to the default.
# If user does NOT specify a date column, ALWAYS use the default — do NOT use any other
# date column even if it seems more intuitive.
# ==================================================================================


# ORACLE SQL GUIDELINES:
# 1. Use double quotes for column names with spaces or special characters
# 2. Use proper Oracle date functions: TO_DATE(), TO_CHAR(), SYSDATE, TRUNC()
# 3. CRITICAL: In IVINSPECTIONHISTORY, date columns are stored as VARCHAR2 in 'YYYY-MM-DD' format
# 4. For IVINSPECTIONHISTORY dates, use: TO_DATE("Date Column", 'YYYY-MM-DD') >= TRUNC(SYSDATE) - [days]
# 5. For other tables with real DATE columns: Use TRUNC("Date Column") >= TRUNC(SYSDATE) - [days]
# 6. Use NVL() or COALESCE() for NULL handling
# 7. DO NOT limit results with ROWNUM or FETCH FIRST unless user specifically asks for "top N" or "first N" or "limit N"
# 8. Return ALL records that match the WHERE conditions by default
# 9. Select ONLY columns relevant to the user's question
# 10. String concatenation: Use || operator or CONCAT()
# 11. Case-insensitive search: Use UPPER() or LOWER() functions
# 12. Always alias tables when using JOINs
# 13. Use proper Oracle aggregate functions: SUM(), COUNT(), AVG(), MAX(), MIN()
# 14. For ranking: Use ROW_NUMBER(), RANK(), or DENSE_RANK() with OVER()
# 15. If user asks last last months, or last year, or monthly , user doesnt specify date column, use default date column which mentioned in table schema description

# QUERY EXAMPLES WITH PRECISE COLUMN SELECTION:

# Example 1 - Items below minimum (NO location specified - aggregate):
# Question: "What items have on-hand quantity less than minimum stock?"
# SQL:
# SELECT 
#     "Item",
#     "ITEM_DESCRIPTION",
#     "Minimum Stock",
#     SUM("ITEM_DETAIL_ON_HAND_QUANTITY") AS "Total On Hand Quantity"
# FROM IVONHANDBYLOCATION
# WHERE "Item Company" = '{TenantId}'
# AND "Minimum Stock" > 0
# GROUP BY "Item", "ITEM_DESCRIPTION", "Minimum Stock"
# HAVING SUM("ITEM_DETAIL_ON_HAND_QUANTITY") < "Minimum Stock"
# ORDER BY "Minimum Stock" - SUM("ITEM_DETAIL_ON_HAND_QUANTITY") DESC;

# Example 2 - Items below minimum BY LOCATION (location specified):
# Question: "What items have on-hand quantity less than minimum stock by location?"
# SQL:
# SELECT 
#     "Item",
#     "ITEM_DESCRIPTION",
#     "ITEM_DETAIL_LOCATION",
#     "LOCATION_NAME",
#     "ITEM_DETAIL_ON_HAND_QUANTITY",
#     "Minimum Stock",
#     ("Minimum Stock" - "ITEM_DETAIL_ON_HAND_QUANTITY") AS "Shortage Quantity"
# FROM IVONHANDBYLOCATION
# WHERE "Item Company" = '{TenantId}'
# AND "Minimum Stock" > 0
# AND "ITEM_DETAIL_ON_HAND_QUANTITY" < "Minimum Stock"
# ORDER BY "Shortage Quantity" DESC;

# Example 3 - Items below minimum at MASTER LOCATION level:
# Question: "What items have on-hand quantity less than minimum at master location level?"
# SQL:
# SELECT 
#     "Item",
#     "ITEM_DESCRIPTION",
#     "MASTER_LOCATION_NAME",
#     "Item ML On Hand Quantity",
#     "Minimum Stock",
#     ("Minimum Stock" - "Item ML On Hand Quantity") AS "Shortage Quantity"
# FROM IVONHANDBYLOCATION
# WHERE "Item Company" = '{TenantId}'
# AND "Minimum Stock" > 0
# AND "Item ML On Hand Quantity" < "Minimum Stock"
# ORDER BY "Shortage Quantity" DESC;

# Example 4 - Total inventory value by location:
# Question: "What is the total inventory value by location?"
# SQL:
# SELECT 
#     "ITEM_DETAIL_LOCATION",
#     "LOCATION_NAME",
#     SUM("EXTENDED_AMOUNT") AS "Total Inventory Value",
#     COUNT(DISTINCT "Item") AS "Item Count"
# FROM IVONHANDBYLOCATION
# WHERE "Item Company" = '{TenantId}'
# GROUP BY "ITEM_DETAIL_LOCATION", "LOCATION_NAME"
# ORDER BY SUM("EXTENDED_AMOUNT") DESC;

# Example 5 - Specific item inventory across all locations:
# Question: "Show inventory on hand for item ABC-123"
# SQL:
# SELECT 
#     "Item",
#     "ITEM_DESCRIPTION",
#     "ITEM_DETAIL_LOCATION",
#     "LOCATION_NAME",
#     "ITEM_DETAIL_ON_HAND_QUANTITY",
#     "EXTENDED_AMOUNT",
#     "Minimum Stock",
#     "Maximum Stock"
# FROM IVONHANDBYLOCATION
# WHERE "Item Company" = '{TenantId}'
# AND "Item" = 'ABC-123'
# ORDER BY "ITEM_DETAIL_LOCATION";

# Example 6 - Items above maximum stock (aggregate):
# Question: "What items exceed maximum stock levels?"
# SQL:
# SELECT 
#     "Item",
#     "ITEM_DESCRIPTION",
#     "Maximum Stock",
#     SUM("ITEM_DETAIL_ON_HAND_QUANTITY") AS "Total On Hand Quantity",
#     (SUM("ITEM_DETAIL_ON_HAND_QUANTITY") - "Maximum Stock") AS "Excess Quantity"
# FROM IVONHANDBYLOCATION
# WHERE "Item Company" = '{TenantId}'
# AND "Maximum Stock" > 0
# GROUP BY "Item", "ITEM_DESCRIPTION", "Maximum Stock"
# HAVING SUM("ITEM_DETAIL_ON_HAND_QUANTITY") > "Maximum Stock"
# ORDER BY "Excess Quantity" DESC;

# Example 7 - Inspection rejections (no location - aggregate):
# Question: "Show items with inspection rejections in last 6 months"
# SQL:
# SELECT 
#     "Inspection Item",
#     "ITEM_DESCRIPTION",
#     SUM("Rejected Quantity") AS "Total Rejected Quantity",
#     COUNT(*) AS "Rejection Count"
# FROM IVINSPECTIONHISTORY
# WHERE "Company" = '{TenantId}'
# AND "Date Received" != ' '
# AND LENGTH(TRIM("Date Received")) = 10
# AND TO_DATE("Date Received", 'YYYY-MM-DD') >= TRUNC(SYSDATE) - 180
# AND "Rejected Quantity" > 0
# GROUP BY "Inspection Item", "ITEM_DESCRIPTION"
# ORDER BY "Total Rejected Quantity" DESC;

# Example 8 - Inspection rejections BY VENDOR (vendor specified):
# Question: "Show items with inspection rejections by vendor in last 6 months"
# SQL:
# SELECT 
#     "Vendor",
#     "Inspection Item",
#     "ITEM_DESCRIPTION",
#     SUM("Rejected Quantity") AS "Total Rejected Quantity",
#     COUNT(*) AS "Rejection Count"
# FROM IVINSPECTIONHISTORY
# WHERE "Company" = '{TenantId}'
# AND "Date Received" != ' '
# AND LENGTH(TRIM("Date Received")) = 10
# AND TO_DATE("Date Received", 'YYYY-MM-DD') >= TRUNC(SYSDATE) - 180
# AND "Rejected Quantity" > 0
# GROUP BY "Vendor", "Inspection Item", "ITEM_DESCRIPTION"
# ORDER BY "Vendor", "Total Rejected Quantity" DESC;

# CRITICAL DATE HANDLING FOR IVINSPECTIONHISTORY:
# ==================================================================================
# The IVINSPECTIONHISTORY view stores ALL date columns as VARCHAR2 strings in 'YYYY-MM-DD' format.
# IMPORTANT: NULL dates are stored as ' ' (single space), which will cause ORA-01841 errors if not filtered.

# Date columns in IVINSPECTIONHISTORY (all VARCHAR2):
# - "Date Received" (DEFAULT - use when user doesn't specify which date)
# - "Inspected On"
# - "Alpha Date Received"
# - "Item Modification Date"
# - "Setup Date"
# - "Drawing Date"
# - "ECN Date"
# - "End of Life"
# - "Obsoleted On"

# ALWAYS filter out invalid dates BEFORE using TO_DATE():

# Correct pattern (CRITICAL - use both filters):
# ✓ WHERE "Date Received" != ' '
# ✓ AND LENGTH(TRIM("Date Received")) = 10
# ✓ AND TO_DATE("Date Received", 'YYYY-MM-DD') >= TRUNC(SYSDATE) - 180

# Time ranges:
# - Last 1 month: TRUNC(SYSDATE) - 30
# - Last 3 months: TRUNC(SYSDATE) - 90
# - Last 6 months: TRUNC(SYSDATE) - 180
# - Last year: TRUNC(SYSDATE) - 365
# ==================================================================================

# IMPORTANT REMINDERS:
# - ALWAYS include tenant filter
# - Select columns PRECISELY based on user's question keywords
# - If user says "by location" → include location columns, don't aggregate
# - If user says "by master location" → use ML-level columns
# - If user doesn't specify location → aggregate with GROUP BY Item
# - If user says "total" or "company-wide" → use SUM() and GROUP BY
# - For IVINSPECTIONHISTORY date filtering, ALWAYS add filters before TO_DATE()
# - DO NOT add ROWNUM or FETCH FIRST limits unless user asks for "top N"
# - Return ALL matching records by default
# - Use table aliases for clarity in multi-table queries
# - SELECT column order MUST match the order columns appear in the table schema definition
# - Calculated/aliased columns (e.g. "Shortage Quantity") always go at the END of SELECT
# - The same question must always produce the same SQL — be deterministic

# USER QUESTION: {user_query}

# CRITICAL OUTPUT FORMAT:
# Return a single JSON object.

# Structure:
# {{
#   "thought_process": "Briefly list the steps taken: 1. Identify tables, 2. Identify date column strategy, 3. List filters.",
#   "sql": "THE_SQL_QUERY",
#   "tables_used": ["TABLE_NAMES"]
# }}

# Rules:
# 1. NO markdown code blocks (no ```json or ```)
# 2. NO explanatory text before or after JSON
# 3. Must be valid, parseable JSON
# 4. "sql" field contains complete executable query
# 5. Use \\n for line breaks in SQL string

# Generate JSON now:
# """

#         response_text = self.model_service.generate(self.PURPOSE, prompt).strip()
        
#         print(f"[SQL GENERATOR] Raw response: {response_text[:500]}")
        
#         result = safe_json_from_model(response_text)
#         print(f"[SQL GENERATOR] Parsed result: {result}")
        
#         if not result or not isinstance(result, dict):
#             print(f"[SQL GENERATOR] ❌ Invalid response format: {result}")
#             raise ValueError(f"Invalid response format from model: {type(result)}")
        
#         sql = result.get("sql", "").strip()
        
#         if not sql:
#             print(f"[SQL GENERATOR] ❌ No SQL generated. Response: {result}")
#             raise ValueError("No SQL query generated by model")
        
#         if TenantId not in sql:
#             print(f"[SQL GENERATOR] ⚠️  TenantId '{TenantId}' not found in generated SQL")
#             print(f"[SQL GENERATOR] SQL: {sql[:200]}")
        
#         print(f"[SQL GENERATOR] ✅ SQL generated successfully")
#         return result



import os
import json
import re
from datetime import datetime
from typing import Dict, Optional
from zoneinfo import ZoneInfo
from src.utils.utils import safe_json_from_model, get_zone, extract_month_and_year
from src.config.db_config import DatabaseConfig
from src.models.model_service import ModelService
from src.config.field_constant import TABLE_SCHEMAS

class SQLQueryGenerator:
    
    PURPOSE = "Technical"
       
    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)
    
    def generate_sql(self, user_query: str, TenantId: str) -> Dict:

        if not TenantId or TenantId.strip() == "":
            raise ValueError("TenantId is required")

        schema_context = {
            "tables": TABLE_SCHEMAS
        }

        prompt = f"""You are an expert Oracle SQL query generator. Generate accurate Oracle SQL queries based on user questions using the provided table schemas.


AVAILABLE TABLES: 
{json.dumps(schema_context, separators=(',', ':'))}

==================================================================================
STEP 0 — MANDATORY PRE-GENERATION CHECKLIST (complete IN ORDER before writing SQL):
==================================================================================
Before writing a single line of SQL, mentally complete ALL steps below:

STEP 0-A  READ THE QUESTION CAREFULLY
  - Identify every entity the user mentions: item, vendor, location, date range,
    quantity type, value, status, etc.
  - Identify grouping intent: "by location", "by vendor", "by master location",
    "total", "each", "all", etc.
  - Identify filter intent: "below minimum", "last 6 months", "for item X", etc.
  - Do NOT assume columns or tables — derive them strictly from question keywords
    and the schema. Same question asked twice must always produce the same SQL.

STEP 0-B  MAP QUESTION TO TABLE(S)
  - Read every table's "description" in AVAILABLE TABLES.
  - Select ONLY the table(s) whose description clearly matches the question subject.
  - Do NOT use a table unless it is unambiguously relevant.

STEP 0-C  IDENTIFY REQUIRED COLUMNS (schema-grounded, no invention)
  - Open the matched table's "columns" object.
  - Pick ONLY the columns needed to answer the question.
  - Every column in the SELECT, WHERE, GROUP BY, ORDER BY MUST exist verbatim
    in the schema. Do NOT invent, abbreviate, or rename columns.
  - Map each question keyword deterministically to the same column every time:
      "quantity on hand"  → always "ITEM_DETAIL_ON_HAND_QUANTITY"
      "minimum stock"     → always "Minimum Stock"
      "master location"   → always "MASTER_LOCATION_NAME" / "Item ML On Hand Quantity"
      "location"          → always "ITEM_DETAIL_LOCATION" / "LOCATION_NAME"
      etc.

STEP 0-D  DETERMINE THE DATE COLUMN (strict hierarchy)
  Priority 1 — User explicitly names a date column (e.g. "voucher date",
               "delivery date", "accepted date") → use ONLY that column.
  Priority 2 — User mentions a time range but NO specific column
               → use the DEFAULT DATE COLUMN stated in that table's schema
                 "description" field. Never substitute another date column.
  Default date columns (do NOT override unless Priority 1 applies):
    - IVINSPECTIONHISTORY  : "Date Received"      (VARCHAR2 'YYYY-MM-DD')
    - XV_REC_HIS_VW        : "Received Date"       (DATE)
    - ITRN_HISTORY_VW      : "Transaction Date"    (DATE)
    - IVONHANDBYLOCATION   : "COST_AS_OF_DATE"     (VARCHAR2 'YYYY-MM-DD')

STEP 0-E  ORDER SELECTED COLUMNS TO MATCH SCHEMA ORDER (mandatory)
  - Note the order each column appears in the table's "columns" definition
    (top = first, bottom = last).
  - Your SELECT list MUST follow that exact same top-to-bottom order.
  - Example: schema order is [A, B, C, D, E, F], you need A, C, D, F
    → correct:   SELECT "A", "C", "D", "F"
    → incorrect: SELECT "C", "A", "F", "D"
  - Derived/calculated columns (e.g. "Shortage Quantity") go AFTER all real
    schema columns in the SELECT list.
  - This rule applies to every query — simple, aggregated, joined, CTE, ranked.

STEP 0-F  WRITE THE SQL
  - Only after completing steps A–E above, write the final SQL.
  - The SQL must be deterministic: the same question always produces the same query.
  - Include tenant filter, correct date handling, correct aggregation/grouping
    based solely on the question intent derived in Step 0-A.
==================================================================================

CRITICAL TENANT FILTERING RULE:
==================================================================================
EVERY query MUST include tenant filtering in the WHERE clause using one of these columns:
- If table has "CCN" column: WHERE "CCN" = '{TenantId}'
- If table has "Company" column: WHERE "Company" = '{TenantId}'
- If table has "Item Company" column: WHERE "Item Company" = '{TenantId}'
- If table has "Company Code" column: WHERE "Company Code" = '{TenantId}'

This applies to ALL queries: simple SELECT, aggregations, JOINs, subqueries, CTEs, and ranking queries.
==================================================================================

CRITICAL COLUMN SELECTION RULE:
==================================================================================
SELECT COLUMNS PRECISELY BASED ON USER QUESTION KEYWORDS:

INVENTORY ON-HAND QUANTITY COLUMNS (IVONHANDBYLOCATION):
- If user asks about "location" or "warehouse" or "site" level:
  → Use: "ITEM_DETAIL_LOCATION", "ITEM_DETAIL_ON_HAND_QUANTITY"
  → This shows quantity at EACH specific location
  
- If user asks about "master location" or "ML" level:
  → Use: "MASTER_LOCATION_NAME" or "ML", "Item ML On Hand Quantity"
  → This shows aggregated quantity at master location level
  
- If user asks about "total" or "company-wide" or doesn't specify location:
  → Use: SUM("ITEM_DETAIL_ON_HAND_QUANTITY") GROUP BY "Item"
  → This shows total across all locations

- If user asks about "each location" or "all locations":
  → Use: "Item", "ITEM_DETAIL_LOCATION", "ITEM_DETAIL_ON_HAND_QUANTITY"
  → Show individual location details

STOCK LEVEL COMPARISON COLUMNS:
- "Minimum Stock" - Minimum stock level threshold
- "Maximum Stock" - Maximum stock level threshold
- "Reorder Point" - Reorder point threshold
- "Safety Stock" - Safety stock level

LOCATION COLUMNS:
- "ITEM_DETAIL_LOCATION" - Specific location/bin
- "LOCATION_NAME" - Location description
- "MASTER_LOCATION_NAME" - Master location name
- "ML" - Master location code

VALUE COLUMNS:
- "EXTENDED_AMOUNT" - Total inventory value at location
- "UNIT_COST" - Unit cost per item

DATE COLUMNS:
- "COST_AS_OF_DATE" - Cost calculation date (VARCHAR2 format 'YYYY-MM-DD')

COLUMN SELECTION EXAMPLES:

User asks: "items below minimum stock"
→ Select: "Item", "ITEM_DESCRIPTION", "Minimum Stock", SUM("ITEM_DETAIL_ON_HAND_QUANTITY")
→ Group by: "Item", "ITEM_DESCRIPTION", "Minimum Stock"
→ Don't include location columns (user didn't ask for location breakdown)

User asks: "items below minimum stock by location"
→ Select: "Item", "ITEM_DESCRIPTION", "ITEM_DETAIL_LOCATION", "ITEM_DETAIL_ON_HAND_QUANTITY", "Minimum Stock"
→ Don't group - show each location separately
→ Include both location and quantity columns

User asks: "items below minimum at master location level"
→ Select: "Item", "ITEM_DESCRIPTION", "MASTER_LOCATION_NAME", "Item ML On Hand Quantity", "Minimum Stock"
→ Don't group - use ML-level columns directly

User asks: "total inventory value by location"
→ Select: "ITEM_DETAIL_LOCATION", SUM("EXTENDED_AMOUNT")
→ Group by: "ITEM_DETAIL_LOCATION"

User asks: "inventory on hand for item X"
→ Select: "Item", "ITEM_DESCRIPTION", "ITEM_DETAIL_LOCATION", "ITEM_DETAIL_ON_HAND_QUANTITY"
→ Don't aggregate - show all locations for that item
==================================================================================

CRITICAL DEFAULT DATE COLUMN RULE:
==================================================================================
When user mentions a time range WITHOUT specifying which date column to use, 
use the DEFAULT DATE COLUMN for that table:

- IVINSPECTIONHISTORY: Use "Date Received" (VARCHAR2)
- XV_REC_HIS_VW: Use "Received Date" (DATE)
- ITRN_HISTORY_VW: Use "Transaction Date" (DATE)
- IVONHANDBYLOCATION: Use "COST_AS_OF_DATE" (VARCHAR2)

If user specifies a date column (e.g. "voucher date", "accepted date", "delivery date"),
use ONLY that specific column — do NOT fall back to the default.
If user does NOT specify a date column, ALWAYS use the default — do NOT use any other
date column even if it seems more intuitive.
==================================================================================


ORACLE SQL GUIDELINES:
1. Use double quotes for column names with spaces or special characters
2. Use proper Oracle date functions: TO_DATE(), TO_CHAR(), SYSDATE, TRUNC()
3. CRITICAL: In IVINSPECTIONHISTORY, date columns are stored as VARCHAR2 in 'YYYY-MM-DD' format
4. For IVINSPECTIONHISTORY dates, use: TO_DATE("Date Column", 'YYYY-MM-DD') >= TRUNC(SYSDATE) - [days]
5. For other tables with real DATE columns: Use TRUNC("Date Column") >= TRUNC(SYSDATE) - [days]
6. Use NVL() or COALESCE() for NULL handling
7. DO NOT limit results with ROWNUM or FETCH FIRST unless user specifically asks for "top N" or "first N" or "limit N"
8. Return ALL records that match the WHERE conditions by default
9. Select ONLY columns relevant to the user's question
10. String concatenation: Use || operator or CONCAT()
11. Case-insensitive search: Use UPPER() or LOWER() functions
12. Always alias tables when using JOINs
13. Use proper Oracle aggregate functions: SUM(), COUNT(), AVG(), MAX(), MIN()
14. For ranking: Use ROW_NUMBER(), RANK(), or DENSE_RANK() with OVER()
15. If user asks last last months, or last year, or monthly , user doesnt specify date column, use default date column which mentioned in table schema description

QUERY EXAMPLES WITH PRECISE COLUMN SELECTION:

Example 1 - Items below minimum (NO location specified - aggregate):
Question: "What items have on-hand quantity less than minimum stock?"
SQL:
SELECT 
    "Item",
    "ITEM_DESCRIPTION",
    "Minimum Stock",
    SUM("ITEM_DETAIL_ON_HAND_QUANTITY") AS "Total On Hand Quantity"
FROM IVONHANDBYLOCATION
WHERE "Item Company" = '{TenantId}'
AND "Minimum Stock" > 0
GROUP BY "Item", "ITEM_DESCRIPTION", "Minimum Stock"
HAVING SUM("ITEM_DETAIL_ON_HAND_QUANTITY") < "Minimum Stock"
ORDER BY "Minimum Stock" - SUM("ITEM_DETAIL_ON_HAND_QUANTITY") DESC;

Example 2 - Items below minimum BY LOCATION (location specified):
Question: "What items have on-hand quantity less than minimum stock by location?"
SQL:
SELECT 
    "Item",
    "ITEM_DESCRIPTION",
    "ITEM_DETAIL_LOCATION",
    "LOCATION_NAME",
    "ITEM_DETAIL_ON_HAND_QUANTITY",
    "Minimum Stock",
    ("Minimum Stock" - "ITEM_DETAIL_ON_HAND_QUANTITY") AS "Shortage Quantity"
FROM IVONHANDBYLOCATION
WHERE "Item Company" = '{TenantId}'
AND "Minimum Stock" > 0
AND "ITEM_DETAIL_ON_HAND_QUANTITY" < "Minimum Stock"
ORDER BY "Shortage Quantity" DESC;

Example 3 - Items below minimum at MASTER LOCATION level:
Question: "What items have on-hand quantity less than minimum at master location level?"
SQL:
SELECT 
    "Item",
    "ITEM_DESCRIPTION",
    "MASTER_LOCATION_NAME",
    "Item ML On Hand Quantity",
    "Minimum Stock",
    ("Minimum Stock" - "Item ML On Hand Quantity") AS "Shortage Quantity"
FROM IVONHANDBYLOCATION
WHERE "Item Company" = '{TenantId}'
AND "Minimum Stock" > 0
AND "Item ML On Hand Quantity" < "Minimum Stock"
ORDER BY "Shortage Quantity" DESC;

Example 4 - Total inventory value by location:
Question: "What is the total inventory value by location?"
SQL:
SELECT 
    "ITEM_DETAIL_LOCATION",
    "LOCATION_NAME",
    SUM("EXTENDED_AMOUNT") AS "Total Inventory Value",
    COUNT(DISTINCT "Item") AS "Item Count"
FROM IVONHANDBYLOCATION
WHERE "Item Company" = '{TenantId}'
GROUP BY "ITEM_DETAIL_LOCATION", "LOCATION_NAME"
ORDER BY SUM("EXTENDED_AMOUNT") DESC;

Example 5 - Specific item inventory across all locations:
Question: "Show inventory on hand for item ABC-123"
SQL:
SELECT 
    "Item",
    "ITEM_DESCRIPTION",
    "ITEM_DETAIL_LOCATION",
    "LOCATION_NAME",
    "ITEM_DETAIL_ON_HAND_QUANTITY",
    "EXTENDED_AMOUNT",
    "Minimum Stock",
    "Maximum Stock"
FROM IVONHANDBYLOCATION
WHERE "Item Company" = '{TenantId}'
AND "Item" = 'ABC-123'
ORDER BY "ITEM_DETAIL_LOCATION";

Example 6 - Items above maximum stock (aggregate):
Question: "What items exceed maximum stock levels?"
SQL:
SELECT 
    "Item",
    "ITEM_DESCRIPTION",
    "Maximum Stock",
    SUM("ITEM_DETAIL_ON_HAND_QUANTITY") AS "Total On Hand Quantity",
    (SUM("ITEM_DETAIL_ON_HAND_QUANTITY") - "Maximum Stock") AS "Excess Quantity"
FROM IVONHANDBYLOCATION
WHERE "Item Company" = '{TenantId}'
AND "Maximum Stock" > 0
GROUP BY "Item", "ITEM_DESCRIPTION", "Maximum Stock"
HAVING SUM("ITEM_DETAIL_ON_HAND_QUANTITY") > "Maximum Stock"
ORDER BY "Excess Quantity" DESC;

Example 7 - Inspection rejections (no location - aggregate):
Question: "Show items with inspection rejections in last 6 months"
SQL:
SELECT 
    "Inspection Item",
    "ITEM_DESCRIPTION",
    SUM("Rejected Quantity") AS "Total Rejected Quantity",
    COUNT(*) AS "Rejection Count"
FROM IVINSPECTIONHISTORY
WHERE "Company" = '{TenantId}'
AND "Date Received" != ' '
AND LENGTH(TRIM("Date Received")) = 10
AND TO_DATE("Date Received", 'YYYY-MM-DD') >= TRUNC(SYSDATE) - 180
AND "Rejected Quantity" > 0
GROUP BY "Inspection Item", "ITEM_DESCRIPTION"
ORDER BY "Total Rejected Quantity" DESC;

Example 8 - Inspection rejections BY VENDOR (vendor specified):
Question: "Show items with inspection rejections by vendor in last 6 months"
SQL:
SELECT 
    "Vendor",
    "Inspection Item",
    "ITEM_DESCRIPTION",
    SUM("Rejected Quantity") AS "Total Rejected Quantity",
    COUNT(*) AS "Rejection Count"
FROM IVINSPECTIONHISTORY
WHERE "Company" = '{TenantId}'
AND "Date Received" != ' '
AND LENGTH(TRIM("Date Received")) = 10
AND TO_DATE("Date Received", 'YYYY-MM-DD') >= TRUNC(SYSDATE) - 180
AND "Rejected Quantity" > 0
GROUP BY "Vendor", "Inspection Item", "ITEM_DESCRIPTION"
ORDER BY "Vendor", "Total Rejected Quantity" DESC;

CRITICAL DATE HANDLING FOR IVINSPECTIONHISTORY:
==================================================================================
The IVINSPECTIONHISTORY view stores ALL date columns as VARCHAR2 strings in 'YYYY-MM-DD' format.
IMPORTANT: NULL dates are stored as ' ' (single space), which will cause ORA-01841 errors if not filtered.

Date columns in IVINSPECTIONHISTORY (all VARCHAR2):
- "Date Received" (DEFAULT - use when user doesn't specify which date)
- "Inspected On"
- "Alpha Date Received"
- "Item Modification Date"
- "Setup Date"
- "Drawing Date"
- "ECN Date"
- "End of Life"
- "Obsoleted On"

ALWAYS filter out invalid dates BEFORE using TO_DATE():

Correct pattern (CRITICAL - use both filters):
✓ WHERE "Date Received" != ' '
✓ AND LENGTH(TRIM("Date Received")) = 10
✓ AND TO_DATE("Date Received", 'YYYY-MM-DD') >= TRUNC(SYSDATE) - 180

Time ranges:
- Last 1 month: TRUNC(SYSDATE) - 30
- Last 3 months: TRUNC(SYSDATE) - 90
- Last 6 months: TRUNC(SYSDATE) - 180
- Last year: TRUNC(SYSDATE) - 365
==================================================================================

IMPORTANT REMINDERS:
- ALWAYS include tenant filter
- Select columns PRECISELY based on user's question keywords
- If user says "by location" → include location columns, don't aggregate
- If user says "by master location" → use ML-level columns
- If user doesn't specify location → aggregate with GROUP BY Item
- If user says "total" or "company-wide" → use SUM() and GROUP BY
- For IVINSPECTIONHISTORY date filtering, ALWAYS add filters before TO_DATE()
- DO NOT add ROWNUM or FETCH FIRST limits unless user asks for "top N"
- Return ALL matching records by default
- Use table aliases for clarity in multi-table queries
- SELECT column order MUST match the order columns appear in the table schema definition
- Calculated/aliased columns (e.g. "Shortage Quantity") always go at the END of SELECT
- The same question must always produce the same SQL — be deterministic

USER QUESTION: {user_query}

CRITICAL OUTPUT FORMAT:
You MUST respond with ONLY a valid JSON object. Do not include any explanation, markdown, or text outside the JSON.

The JSON must follow this exact structure:
{{
  "thought_process": "Briefly list the steps taken: 1. Identify tables, 2. Identify date column strategy, 3. List filters.",
  "sql": "THE_SQL_QUERY",
  "tables_used": ["TABLE_NAMES"]
}}

Rules:
1. NO markdown code blocks (no ```json or ```)
2. NO explanatory text before or after JSON
3. Must be valid, parseable JSON
4. "sql" field contains complete executable query
5. Use \\n for line breaks in SQL string

Generate JSON now:
"""

        response_text = self.model_service.generate(self.PURPOSE, prompt).strip()
        
        print(f"[SQL GENERATOR] Raw response: {response_text[:500]}")
        
        result = safe_json_from_model(response_text)
        print(f"[SQL GENERATOR] Parsed result: {result}")
        
        if not result or not isinstance(result, dict):
            print(f"[SQL GENERATOR] ❌ Invalid response format: {result}")
            raise ValueError(f"Invalid response format from model: {type(result)}")
        
        sql = result.get("sql", "").strip()
        
        if not sql:
            print(f"[SQL GENERATOR] ❌ No SQL generated. Response: {result}")
            raise ValueError("No SQL query generated by model")
        
        if TenantId not in sql:
            print(f"[SQL GENERATOR] ⚠️  TenantId '{TenantId}' not found in generated SQL")
            print(f"[SQL GENERATOR] SQL: {sql[:200]}")
        
        print(f"[SQL GENERATOR] ✅ SQL generated successfully")
        return result
