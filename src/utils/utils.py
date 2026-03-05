import re
import time
import csv
import json
import calendar
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional,Tuple
from zoneinfo import ZoneInfo
from src.config.field_constant import  MONTH_NAMES
from io import StringIO
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger(__name__)  
load_dotenv()

zone : str = os.getenv("zone")
def get_zone():
    name = os.getenv("zone", "Asia/Kolkata")
    try:
        return ZoneInfo(name)
    except Exception:
        return timezone(timedelta(hours=5, minutes=30))

MAX_ROWS: int = int(os.getenv("MAX_ROWS"))

def safe_serialize(value: Any) -> Any:

    if value is None:
        return None
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, (int, float)):
        return float(value) if isinstance(value, float) else int(value)
    elif isinstance(value, bool):
        return value
    elif isinstance(value, str):
        return str(value)
    elif isinstance(value, (list, tuple)):
        return [safe_serialize(v) for v in value]
    elif isinstance(value, dict):
        return {k: safe_serialize(v) for k, v in value.items()}
    else:
        return str(value)

def safe_json_from_model(txt: str):
    
    if not txt:
        raise ValueError("empty model text")
    s = txt.strip()

    if s.startswith("```"):
        s = s.strip("`").strip()
        if s[:4].lower() == "json":
            s = s[4:].lstrip()

    def _looks_like_json(t: str) -> bool:
        t = t.lstrip()
        return t.startswith("{") or t.startswith("[")

    def _rm_trailing_commas(t: str) -> str:
        return re.sub(r",\s*(\]|\})", r"\1", t)

    try:
        parsed = json.loads(s)
        if isinstance(parsed, (dict, list)):
            return parsed
        unwrap_guard = 0
        while isinstance(parsed, str) and _looks_like_json(parsed) and unwrap_guard < 5:
            parsed = json.loads(parsed)
            unwrap_guard += 1
        if isinstance(parsed, (dict, list)):
            return parsed
        s = str(parsed)
    except Exception:
        pass

    if len(s) >= 2 and s[0] == s[-1] == '"':
        try:
            u = json.loads(s) 
            if isinstance(u, str):
                s = u
        except Exception:
            pass

    s_tc = _rm_trailing_commas(s)
    try:
        parsed = json.loads(s_tc)
        if isinstance(parsed, (dict, list)):
            return parsed
        unwrap_guard = 0
        while isinstance(parsed, str) and _looks_like_json(parsed) and unwrap_guard < 5:
            parsed = json.loads(parsed)
            unwrap_guard += 1
        if isinstance(parsed, (dict, list)):
            return parsed
    except Exception:
        pass
    
    l_brace = s.find("{")
    l_brack = s.find("[")
    l = min([x for x in (l_brace, l_brack) if x != -1], default=-1)
    if l != -1:
        r_curly = s.rfind("}")
        r_brack = s.rfind("]")
        r = max(r_curly, r_brack)
        if r > l:
            candidate = _rm_trailing_commas(s[l:r+1])
            try:
                parsed = json.loads(candidate)
                return parsed
            except Exception:
                try:
                    inner = json.loads(candidate)
                    if isinstance(inner, str):
                        inner = _rm_trailing_commas(inner)
                        return json.loads(inner)
                except Exception:
                    pass

    raise ValueError("Could not extract JSON from model text:\n" + s[:500])
     
def extract_month_and_year(query: str) -> tuple[Optional[int], Optional[int]]:
    
        q = query.lower()
        detected_month = None
        detected_year = None
        
        year_match = re.search(r'\b(20\d{2})\b', query)
        if year_match:
            detected_year = int(year_match.group(1))
        
        for month_name, month_num in MONTH_NAMES.items():
            if month_name in q:
                detected_month = month_num
                break
        
        if detected_month and not detected_year:
            now = datetime.now(get_zone())
            if detected_month > now.month:
                detected_year = now.year - 1
            else:
                detected_year = now.year
        
        return detected_month, detected_year

def choose_optimal_format(rows: List[Dict], context: str = "general") -> Tuple[str, str]:

    if not rows:
        return "csv", ""
    try:
        buf = StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        return "csv", buf.getvalue()
    except Exception as e:
        logger.info(f"[FORMAT] CSV error, falling back to JSON: {e}")
        return "json", json.dumps(rows, indent=2, default=str)

def retry_with_backoff(func, max_retries=3, initial_delay=2):
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                error_msg = str(e).lower()
                if "timeout" in error_msg or "504" in error_msg or "deadline" in error_msg:
                    if attempt < max_retries - 1:
                        delay = initial_delay * (2 ** attempt)
                        logger.info(f"[RETRY] Timeout on attempt {attempt + 1}/{max_retries}. Retrying in {delay}s...")
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"[RETRY]  All {max_retries} attempts failed due to timeout")
                        raise Exception(f"The model is taking longer than expected. This can happen with complex data or charts. Please try:\n• Reducing the date range or number of records\n• Simplifying your request\n• Trying again in a moment\n\nTechnical: {e}")
                else:
                    raise
        return None
    

REFRESH_WORDS = {"yes", "ok", "okay", "yes please", "refresh", "sure", "yep", "yeah"}

def is_refresh_request(text: str) -> bool:
    
    text_lower = text.strip().lower()
    
    for refresh_word in REFRESH_WORDS:
        if text_lower == refresh_word or text_lower.startswith(refresh_word + " "):
            return True
    
    return False

def extract_additional_context(query: str) -> str:
  
    query_lower = query.lower().strip()
    
   
    for keyword in REFRESH_WORDS:
        if query_lower == keyword:
    
            return ""
        elif query_lower.startswith(keyword + " "):
           
            additional = query[len(keyword):].strip()
            return additional
    
    return ""

def get_last_real_user_query(all_messages: list) -> str | None:

    for msg in reversed(all_messages):
        if msg.get("role") != "user":
            continue
        content = (msg.get("content") or "").strip()
        if not content:
            continue
        if is_refresh_request(content):
            continue
        return content
    return None

def parse_markdown_table(
    lines: List[str],
    start_index: int
) -> Tuple[Optional[List[List[str]]], int]:
    table_lines = []
    i = start_index

    while i < len(lines) and lines[i].strip().startswith("|"):
        table_lines.append(lines[i].strip())
        i += 1

    if not table_lines:
        return None, start_index

    rows = []
    for idx, row_line in enumerate(table_lines):
        if idx == 1 and re.search(r"-{3,}", row_line):
            continue

        parts = [cell.strip() for cell in row_line.split("|")]
        parts = [p for p in parts if p]
        if parts:
            rows.append(parts)

    return rows, i

def restore_original_columns(
    forecast_rows: list[dict],
    spec: dict
) -> list[dict]:
   
    date_col = spec.get("date_column", "InvoiceDate")
    grouping_cols = spec.get("grouping_columns", [])

    restored = []
    for r in forecast_rows:
        new_r = dict(r)

        new_r[date_col] = new_r.pop("date")

        if "group" in new_r and grouping_cols:
            new_r[grouping_cols[0]] = new_r.pop("group")

        restored.append(new_r)

    return restored

def get_summary_spec() -> Dict:
        
        return {
            "aggregations": [
                {
                    "category": "status",
                    "group_by": ["InvoiceStatusType", "InvoiceApprovalStatus", "VerificationResult", "PaymentStatus"],
                    "metrics": [
                        {"field": "GrandTotal", "function": "sum"},
                        {"field": "*", "function": "count"}
                    ]
                },
                {
                    "category": "provider",
                    "group_by": ["ProviderName"],
                    "metrics": [
                        {"field": "GrandTotal", "function": "sum"},
                        {"field": "*", "function": "count"}
                    ]
                },
                {
                    "category": "cost_center",
                    "group_by": ["CostName", "SiteName"],
                    "metrics": [
                        {"field": "GrandTotal", "function": "sum"},
                        {"field": "*", "function": "count"}
                    ]
                },
                {
                    "category": "service",
                    "group_by": ["ServiceName"],
                    "metrics": [
                        {"field": "GrandTotal", "function": "sum"},
                        {"field": "*", "function": "count"}
                    ]
                },
                {
                    "category": "financial",
                    "metrics": [
                        {"field": "GrandTotal", "function": "sum"},
                        {"field": "GrandTotal", "function": "avg"},
                        {"field": "GrandTotal", "function": "min"},
                        {"field": "GrandTotal", "function": "max"},
                        {"field": "NetTotal", "function": "sum"},
                        {"field": "TotalTax", "function": "sum"},
                        {"field": "UsageCharge", "function": "sum"},
                        {"field": "RentalCharge", "function": "sum"},
                        {"field": "*", "function": "count"}
                    ]
                },
                {
                    "category": "time",
                    "group_by": ["InvoiceDate"],
                    "time_bucket": "month",
                    "metrics": [
                        {"field": "GrandTotal", "function": "sum"},
                        {"field": "*", "function": "count"}
                    ]
                },
                {
                    "category": "risk",
                    "sub_aggregations": [
                        {
                            "name": "disputed_breakdown",
                            "description": "Detailed breakdown of disputed invoices",
                            "filter_type": "disputed",
                            "filter_field": "InvoiceStatusType",
                            "filter_values": ["Disputed", "System Disputed"],
                            "group_by": ["InvoiceStatusType", "InvoiceApprovalStatus", "PaymentStatus", "VerificationResult"],
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
                            "group_by": ["InvoiceStatusType", "InvoiceApprovalStatus", "PaymentStatus", "VerificationResult"],
                            "metrics": [
                                {"field": "GrandTotal", "function": "sum"},
                                {"field": "*", "function": "count"}
                            ]
                        },
                        {
                            "name": "not_verified",
                            "description": "Invoices with verification issues",
                            "filter_type": "not_verified",
                            "filter_field": "VerificationResult",
                            "filter_values": ["Not Verified", "Unknown"]
                        },
                        {
                            "name": "pending_approval",
                            "description": "Invoices pending approval",
                            "filter_type": "pending_approval",
                            "filter_field": "InvoiceApprovalStatus",
                            "filter_values": ["Initiated", "Pending"]
                        }
                    ]
                }
            ],
            "include_categories": ["status", "provider", "cost_center", "service", "financial", "time", "risk"]
        }