import re
import time
import csv
import json
import calendar
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional,Tuple
from difflib import get_close_matches
from zoneinfo import ZoneInfo
from src.config.field_constant import TABLE_SCHEMAS
from io import StringIO
from dotenv import load_dotenv
import os  
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
    """
    Safely serialize values to JSON-compatible types.
    
    Args:
        value: Value to serialize
        
    Returns:
        JSON-compatible value
    """
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



def escape_odata_literal(s: str) -> str:
    """
    Escape single quotes in OData string literals.
    
    Args:
        s: String to escape
        
    Returns:
        Escaped string
    """
    return s.replace("'", "''")


def safe_json_from_model(txt: str):
    """
    Parse JSON from model output, handling:
    - ```json fences
    - raw JSON objects/arrays
    - quoted JSON blobs (e.g. "\"{\\\"a\\\":1}\"")
    - repeated nested quoting
    - trailing commas
    - noisy text around the JSON (extracts largest JSON slice)
    """
    if not txt:
        raise ValueError("empty model text")
    s = txt.strip()

    # 1) Strip code fences if present
    if s.startswith("```"):
        s = s.strip("`").strip()
        if s[:4].lower() == "json":
            s = s[4:].lstrip()

    def _looks_like_json(t: str) -> bool:
        t = t.lstrip()
        return t.startswith("{") or t.startswith("[")

    def _rm_trailing_commas(t: str) -> str:
        # remove trailing commas before ] or }
        return re.sub(r",\s*(\]|\})", r"\1", t)

    # 2) Try direct parse (covers raw JSON)
    try:
        parsed = json.loads(s)
        # If we get a dict/list, done
        if isinstance(parsed, (dict, list)):
            return parsed
        # If we get a string that itself looks like JSON, unwrap repeatedly
        unwrap_guard = 0
        while isinstance(parsed, str) and _looks_like_json(parsed) and unwrap_guard < 5:
            parsed = json.loads(parsed)
            unwrap_guard += 1
        if isinstance(parsed, (dict, list)):
            return parsed
        # Fall through to slicing
        s = str(parsed)
    except Exception:
        pass

    # 3) If the whole thing is a quoted string (starts/ends with quotes), try unquoting once
    if len(s) >= 2 and s[0] == s[-1] == '"':
        try:
            u = json.loads(s)  # unquote once
            if isinstance(u, str):
                s = u
        except Exception:
            pass

    # 4) Remove trailing commas and try again
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

    # 5) As a last resort, extract largest JSON slice from the string and parse
    #    Find the first opening { or [ and the last closing } or ] and slice
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
                # sometimes inner is still quoted
                try:
                    inner = json.loads(candidate)
                    if isinstance(inner, str):
                        inner = _rm_trailing_commas(inner)
                        return json.loads(inner)
                except Exception:
                    pass

    # 6) Give up with a helpful error
    raise ValueError("Could not extract JSON from model text:\n" + s[:500])


def rows_to_markdown(rows: List[Dict], cols: List[str], max_rows=MAX_ROWS) -> str:
    """
    Convert rows to markdown table format.
    
    Args:
        rows: List of row dictionaries
        cols: Column names to include
        max_rows: Maximum number of rows to include
        
    Returns:
        Markdown formatted table string
    """
    if not rows:
        return "No rows."
    cols = [c for c in cols if c in rows[0]]
    header = "| " + " | ".join(cols) + " |"
    sep = "|" + "|".join(["---"] * len(cols)) + "|"
    lines = [header, sep]
    for r in rows[:max_rows]:
        line = "| " + " | ".join(str(r.get(c, "")).replace("\n", " ")[:120] for c in cols) + " |"
        lines.append(line)
    if len(rows) > max_rows:
        lines.append(f"... (truncated, total rows: {len(rows)})")
    return "\n".join(lines)



def gemini_text_extract(resp) -> str:
    """
    Robustly extract text from Gemini response (handles .text and multi-Part).
    Returns '' if no textual content is present.
    """
    try:
        # Try convenience accessor
        try:
            t = getattr(resp, "text", None)
            if callable(t):
                t = t()
        except Exception as e:
            print(f"[GEMINI_EXTRACT] .text access raised: {e}")
            t = None

        if isinstance(t, str) and t.strip():
            s = t.strip()
            print(f"[GEMINI_EXTRACT] ✅ Extracted via .text ({len(s)} chars)")
            return s

        # Fallback: candidates -> content.parts -> text
        candidates = getattr(resp, "candidates", None)
        if not candidates:
            print("[GEMINI_EXTRACT] ⚠️ No candidates in response")
            return ""

        print(f"[GEMINI_EXTRACT] Found {len(candidates)} candidate(s)")
        for i, c in enumerate(candidates):
            content = getattr(c, "content", None)
            if not content:
                print(f"[GEMINI_EXTRACT] Candidate {i}: No content")
                continue
            parts = getattr(content, "parts", None)
            if not parts:
                print(f"[GEMINI_EXTRACT] Candidate {i}: No parts")
                continue

            chunks = []
            for j, p in enumerate(parts):
                txt = getattr(p, "text", None)
                if not txt and isinstance(p, dict):
                    txt = p.get("text")
                if isinstance(txt, str) and txt:
                    print(f"[GEMINI_EXTRACT] Part {j}: {len(txt)} chars")
                    chunks.append(txt)

            if chunks:
                out = "\n".join(chunks).strip()
                print(f"[GEMINI_EXTRACT] ✅ Extracted via candidates ({len(out)} chars)")
                return out

        try:
            fr = getattr(candidates[0], "finish_reason", None)
            if fr is not None:
                print(f"[GEMINI_EXTRACT] ⚠️ Finish reason: {fr}")
        except Exception:
            pass

        print("[GEMINI_EXTRACT] ❌ Could not extract text from response")
        return ""
    except Exception as e:
        print(f"[GEMINI_EXTRACT] ❌ Exception: {e}")
        import traceback; traceback.print_exc()
        return ""



def extract_svg_from_gemini(resp) -> str:
    """
    Returns a single SVG string from a Gemini response.
    Concatenate all text parts, then slice from first '<svg' to last '</svg>'.
    """
    try:
        buf = []
        # Prefer multi-part assembly; falls back to .text if needed
        candidates = getattr(resp, "candidates", None) or []
        for c in candidates:
            content = getattr(c, "content", None)
            parts = getattr(content, "parts", None) if content else None
            if parts:
                for p in parts:
                    txt = getattr(p, "text", None)
                    if not txt and isinstance(p, dict):
                        txt = p.get("text")
                    if txt:
                        buf.append(txt)

        if not buf:
            # fall back to simple text
            s = gemini_text_extract(resp)
            if s:
                buf.append(s)

        text = "\n".join(buf)
        l = text.lower().find("<svg")
        r = text.lower().rfind("</svg>")
        if l == -1 or r == -1:
            return ""
        return text[l:r+6]
    except Exception as e:
        print(f"[SVG_EXTRACT] ❌ Failed: {e}")
        return ""



        
def extract_month_and_year(query: str) -> tuple[Optional[int], Optional[int]]:
        """Extract month and year from query."""
        q = query.lower()
        detected_month = None
        detected_year = None
        
        # Extract year if present
        year_match = re.search(r'\b(20\d{2})\b', query)
        if year_match:
            detected_year = int(year_match.group(1))
        
        # Extract month
        for month_name, month_num in MONTH_NAMES.items():
            if month_name in q:
                detected_month = month_num
                break
        
        # Smart year selection
        if detected_month and not detected_year:
            now = datetime.now(get_zone())
            if detected_month > now.month:
                detected_year = now.year - 1
            else:
                detected_year = now.year
        
        return detected_month, detected_year


# def choose_optimal_format(rows: List[Dict], context: str = "general") -> Tuple[str, str]:
#     """Pick CSV for flat tables; JSON for nested data."""
#     if not rows:
#         return "json", "[]"
    
#     # Check for nested structures (dict/list values)
#     has_nested = any(
#         isinstance(v, (dict, list)) 
#         for row in rows[:min(5, len(rows))] 
#         for v in row.values()
#     )
    
#     # ✅ Simple rule: CSV if flat, JSON if nested
#     if not has_nested and context in ["chart", "analysis", "summary", "chat","general_qa"]:
#         try:
#             buf = StringIO()
#             writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
#             writer.writeheader()
#             writer.writerows(rows)
#             return "csv", buf.getvalue()
#         except Exception as e:
#             print(f"[FORMAT] CSV error: {e}")
#             return "json", json.dumps(rows, indent=2, default=str)
    
#     return "json", json.dumps(rows, indent=2, default=str)

def choose_optimal_format(rows: List[Dict], context: str = "general") -> Tuple[str, str]:
    """
    Default: always return CSV for any rows.
    Fallback: JSON only if CSV conversion fails.
    """
    if not rows:
        # Empty but still "CSV"
        return "csv", ""

    try:
        buf = StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        return "csv", buf.getvalue()
    except Exception as e:
        print(f"[FORMAT] CSV error, falling back to JSON: {e}")
        return "json", json.dumps(rows, indent=2, default=str)



def retry_with_backoff(func, max_retries=3, initial_delay=2):
        """Retry a function with exponential backoff for timeout errors."""
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                error_msg = str(e).lower()
                if "timeout" in error_msg or "504" in error_msg or "deadline" in error_msg:
                    if attempt < max_retries - 1:
                        delay = initial_delay * (2 ** attempt)
                        print(f"[RETRY] ⏳ Timeout on attempt {attempt + 1}/{max_retries}. Retrying in {delay}s...")
                        time.sleep(delay)
                        continue
                    else:
                        print(f"[RETRY] ❌ All {max_retries} attempts failed due to timeout")
                        raise Exception(f"⏳ The model is taking longer than expected. This can happen with complex data or charts. Please try:\n• Reducing the date range or number of records\n• Simplifying your request\n• Trying again in a moment\n\nTechnical: {e}")
                else:
                    # Non-timeout error, raise immediately
                    raise
        return None
    

REFRESH_WORDS = {"yes", "ok", "okay", "yes please", "refresh", "sure", "yep", "yeah"}

def is_refresh_request(text: str) -> bool:
    """
    Check if the text starts with a refresh keyword.
    
    Examples:
    - "yes" → True
    - "yes for March to June" → True
    - "ok show me data" → True
    - "pending invoices" → False
    """
    text_lower = text.strip().lower()
    
    # Check if text starts with any refresh word
    for refresh_word in REFRESH_WORDS:
        if text_lower == refresh_word or text_lower.startswith(refresh_word + " "):
            return True
    
    return False


def extract_additional_context(query: str) -> str:
    """
    Extract additional context after refresh keywords.
    
    Examples:
    - "yes" → ""
    - "yes for March to June" → "for March to June"
    - "ok show me last month" → "show me last month"
    """
    query_lower = query.lower().strip()
    
    # Check each refresh keyword
    for keyword in REFRESH_WORDS:
        if query_lower == keyword:
            # Exact match, no additional context
            return ""
        elif query_lower.startswith(keyword + " "):
            # Found keyword with space, extract the rest
            additional = query[len(keyword):].strip()
            return additional
    
    return ""


def get_last_real_user_query(all_messages: list) -> str | None:
    """
    Return the last user message that is NOT just a refresh request.
    This will be things like 'pending invoices', 'approved invoices', etc.
    """
    for msg in reversed(all_messages):
        if msg.get("role") != "user":
            continue
        content = (msg.get("content") or "").strip()
        if not content:
            continue
        # ✅ Use is_refresh_request to check if it's a refresh
        if is_refresh_request(content):
            # Skip YES/OK messages (including "yes for March")
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
    """
    Convert:
    date  -> original date column (InvoiceDate / BillReceiveDate)
    group -> original grouping column (AccountNumber / ProviderName)
    """

    date_col = spec.get("date_column", "InvoiceDate")
    grouping_cols = spec.get("grouping_columns", [])

    restored = []
    for r in forecast_rows:
        new_r = dict(r)

        # restore date
        new_r[date_col] = new_r.pop("date")

        # restore group
        if "group" in new_r and grouping_cols:
            new_r[grouping_cols[0]] = new_r.pop("group")

        restored.append(new_r)

    return restored


def validate_aggregation_spec(spec: Dict[str, Any]) -> bool:
    """Validate that aggregation spec has correct structure."""
    if not isinstance(spec, dict):
        return False
    
    if "aggregations" not in spec:
        return False
    
    if not isinstance(spec["aggregations"], list):
        return False
    
    for agg in spec["aggregations"]:
        if "category" not in agg:
            return False
    
    return True


def get_aggregation_summary_size(summary: Dict[str, Any]) -> int:
    """Calculate approximate size of aggregated summary in characters."""
    import json
    return len(json.dumps(summary))


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