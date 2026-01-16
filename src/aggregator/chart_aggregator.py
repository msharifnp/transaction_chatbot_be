from __future__ import annotations
from difflib import get_close_matches
from typing import Any, Dict, List, Optional
import pandas as pd

ALLOWED_AGGS = {"sum", "avg", "min", "max", "count"}
ALLOWED_TIME_BUCKETS = {None, "day", "week", "month"}

def _fix_col(col: str, available: List[str]) -> Optional[str]:

    if not col:
        return None
    if col in available:
        return col
    m = get_close_matches(col, available, n=1, cutoff=0.6)
    return m[0] if m else None


def validate_and_fix_spec(
    spec: Dict[str, Any],
    available_cols: List[str],
    field_types: Dict[str, str],
) -> Dict[str, Any]:
 
    fixed = dict(spec or {})

    fixed["task"] = str(fixed.get("task") or "chart").lower()
    fixed["chart_type"] = fixed.get("chart_type")
    time_bucket = fixed.get("time_bucket")
    fixed["time_bucket"] = time_bucket if time_bucket in ALLOWED_TIME_BUCKETS else None

    # group_by
    gb = fixed.get("group_by") or []
    gb_fixed: List[str] = []
    for c in gb:
        fc = _fix_col(str(c), available_cols)
        if fc:
            gb_fixed.append(fc)
    fixed["group_by"] = gb_fixed

    # metrics
    metrics = fixed.get("metrics") or []
    m_fixed: List[Dict[str, str]] = []
    for m in metrics:
        if not isinstance(m, dict):
            continue
        col_raw = str(m.get("col") or "")
        agg_raw = str(m.get("agg") or "sum").lower()

        if agg_raw == "average":
            agg_raw = "avg"
        if agg_raw not in ALLOWED_AGGS:
            agg_raw = "sum"

        if agg_raw == "count":
            # allow row-count even if no column
            m_fixed.append({"col": "__rows__", "agg": "count"})
            continue

        col = _fix_col(col_raw, available_cols)
        if not col:
            continue

        # enforce numeric metrics unless it's count
        if field_types.get(col) != "number":
            continue

        m_fixed.append({"col": col, "agg": agg_raw})

    fixed["metrics"] = m_fixed

    # sort_by
    sb = fixed.get("sort_by")
    if isinstance(sb, dict):
        sb_col = _fix_col(str(sb.get("col") or ""), available_cols)
        fixed["sort_by"] = {"col": sb_col, "desc": bool(sb.get("desc", True))} if sb_col else None
    else:
        fixed["sort_by"] = None

    # limit
    limit = fixed.get("limit")
    try:
        fixed["limit"] = int(limit) if limit is not None else None
    except Exception:
        fixed["limit"] = None

    # Defaults: metrics
    if not fixed["metrics"]:
        if "grand_total" in available_cols and field_types.get("grand_total") == "number":
            fixed["metrics"] = [{"col": "grand_total", "agg": "sum"}]
        else:
            fixed["metrics"] = [{"col": "__rows__", "agg": "count"}]

    # Defaults: group_by
    if not fixed["group_by"]:
        if fixed["task"] == "forecast" and "invoice_date" in available_cols:
            fixed["group_by"] = ["invoice_date"]
        else:
            for c in ["site_name", "providers_name", "cost_name", "line_name"]:
                if c in available_cols:
                    fixed["group_by"] = [c]
                    break

    return fixed


def aggregate_rows(
    rows: List[Dict[str, Any]],
    spec: Dict[str, Any],
    field_types: Dict[str, str],
    max_groups: int = 500,
) -> List[Dict[str, Any]]:
   
    if not rows:
        return []

    df = pd.DataFrame(rows)
    available_cols = list(df.columns)

    spec = validate_and_fix_spec(spec, available_cols, field_types)

    group_cols = spec["group_by"]
    metrics = spec["metrics"]
    time_bucket = spec.get("time_bucket")

    # If invoice_date is present and time_bucket requested, bucket it
    if time_bucket and "invoice_date" in group_cols and "invoice_date" in df.columns:
        df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

        if time_bucket == "day":
            df["_bucket"] = df["invoice_date"].dt.to_period("D").dt.to_timestamp()
        elif time_bucket == "week":
            df["_bucket"] = df["invoice_date"].dt.to_period("W").dt.start_time
        elif time_bucket == "month":
            df["_bucket"] = df["invoice_date"].dt.to_period("M").dt.to_timestamp()
        else:
            df["_bucket"] = df["invoice_date"]

        group_cols = ["_bucket" if c == "invoice_date" else c for c in group_cols]

    # Keep only needed columns
    needed = set(group_cols)
    for m in metrics:
        col = m["col"]
        if col != "__rows__" and col in df.columns:
            needed.add(col)

    df = df[[c for c in needed if c in df.columns]].copy()

    # Convert metric columns to numeric
    for m in metrics:
        col = m["col"]
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Build aggregation map
    agg_map: Dict[str, str] = {}
    for m in metrics:
        col = m["col"]
        agg = m["agg"]
        if agg == "count" or col == "__rows__":
            continue
        if col in df.columns:
            agg_map[col] = {"sum": "sum", "avg": "mean", "min": "min", "max": "max"}[agg]

    # Aggregate
    if agg_map:
        out = df.groupby(group_cols, dropna=False).agg(agg_map).reset_index()
    else:
        out = df.groupby(group_cols, dropna=False).size().reset_index(name="count")

    # Add row count metric if requested
    if any(m["agg"] == "count" for m in metrics):
        counts = df.groupby(group_cols, dropna=False).size().reset_index(name="count")
        if "count" in out.columns:
            # already present
            pass
        else:
            out = out.merge(counts, on=group_cols, how="left")

    # Rename bucket column
    if "_bucket" in out.columns:
        out = out.rename(columns={"_bucket": "invoice_date_bucket"})

    # Sort
    sb = spec.get("sort_by")
    if sb and sb.get("col") in out.columns:
        out = out.sort_values(by=sb["col"], ascending=not sb.get("desc", True))

    # Guard: too many groups -> trim
    if len(out) > max_groups:
        out = out.head(max_groups)

    # Limit
    if spec.get("limit"):
        out = out.head(spec["limit"])

    return out.to_dict(orient="records")
