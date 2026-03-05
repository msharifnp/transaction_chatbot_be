from __future__ import annotations
from difflib import get_close_matches
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import logging
logger = logging.getLogger(__name__)    

ALLOWED_TIME_BUCKETS = {None, "day", "week", "month"}


def _fix_col(col: str, available: List[str]) -> Optional[str]:

    if not col:
        return None
    if col in available:
        return col
    m = get_close_matches(col, available, n=1, cutoff=0.6)
    return m[0] if m else None


def validate_and_fix_forecast_spec(
    spec: Dict[str, Any],
    available_cols: List[str],
    field_types: Dict[str, str],
) -> Dict[str, Any]:
   
    fixed = dict(spec or {})

    date_col_raw = fixed.get("date_column")
    date_col = _fix_col(str(date_col_raw or ""), available_cols)
    
    if not date_col or field_types.get(date_col) != "date":
        for col in ["InvoiceDate", "BillReceiveDate", "invoice_date", "bill_receive_date"]:
            if col in available_cols:
                date_col = col
                break
    
    fixed["date_column"] = date_col

    value_cols = fixed.get("value_columns") or []
    value_cols_fixed: List[str] = []
    for c in value_cols:
        fc = _fix_col(str(c), available_cols)
        if fc and field_types.get(fc) == "number":
            value_cols_fixed.append(fc)
    
    if not value_cols_fixed:
        for preferred in ["GrandTotal", "NetTotal", "UsageCharge", "RentalCharge", 
                         "grand_total", "net_total", "usage_charge", "rental_charge"]:
            if preferred in available_cols and field_types.get(preferred) == "number":
                value_cols_fixed.append(preferred)
                break
    
    fixed["value_columns"] = value_cols_fixed

    grouping_cols = fixed.get("grouping_columns") or []
    grouping_cols_fixed: List[str] = []
    for c in grouping_cols:
        fc = _fix_col(str(c), available_cols)
        if fc:
            grouping_cols_fixed.append(fc)
    
    fixed["grouping_columns"] = grouping_cols_fixed

    time_bucket = fixed.get("time_bucket")
    fixed["time_bucket"] = time_bucket if time_bucket in ALLOWED_TIME_BUCKETS else "month"

    try:
        periods = int(fixed.get("forecast_periods", 12))
        fixed["forecast_periods"] = max(1, min(periods, 36))  
    except Exception:
        fixed["forecast_periods"] = 12

    fixed["filters"] = fixed.get("filters") or []

    return fixed


def prepare_forecast_data(
    rows: List[Dict[str, Any]],
    spec: Dict[str, Any],
    field_types: Dict[str, str],
    max_groups: int = 50,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    
    if not rows:
        return [], spec

    df = pd.DataFrame(rows)
    available_cols = list(df.columns)

    spec = validate_and_fix_forecast_spec(spec, available_cols, field_types)

    date_col = spec["date_column"]
    value_cols = spec["value_columns"]
    grouping_cols = spec["grouping_columns"]
    time_bucket = spec.get("time_bucket", "month")

    if not date_col or not value_cols:
        logger.info(f"[FORECAST_AGG]  Missing required columns. date_column={date_col}, value_columns={value_cols}")
        return [], spec

    if date_col not in df.columns:
        logger.info(f"[FORECAST_AGG]  Date column '{date_col}' not found in data")
        return [], spec

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])  

    if df.empty:
        logger.info(f"[FORECAST_AGG]  No valid dates found")
        return [], spec

    if time_bucket == "day":
        df["_date_bucket"] = df[date_col].dt.to_period("D").dt.to_timestamp()
    elif time_bucket == "week":
        df["_date_bucket"] = df[date_col].dt.to_period("W").dt.start_time
    elif time_bucket == "month":
        df["_date_bucket"] = df[date_col].dt.to_period("M").dt.to_timestamp()
    else:
        df["_date_bucket"] = df[date_col].dt.normalize()  

    for col in value_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    agg_cols = ["_date_bucket"]
    agg_map = {col: "sum" for col in value_cols if col in df.columns}

    if grouping_cols:
        valid_grouping = [g for g in grouping_cols if g in df.columns]
        if valid_grouping:
            agg_cols.extend(valid_grouping)
            
            unique_groups = df[valid_grouping].drop_duplicates()
            if len(unique_groups) > max_groups:
                logger.info(f"[FORECAST_AGG] Too many groups ({len(unique_groups)}), limiting to top {max_groups}")
                if value_cols and value_cols[0] in df.columns:
                    top_groups = (
                        df.groupby(valid_grouping)[value_cols[0]]
                        .sum()
                        .nlargest(max_groups)
                        .index
                    )

                    if len(valid_grouping) == 1:
                        df = df[df[valid_grouping[0]].isin(top_groups)]
                    else:
                        df = df.set_index(valid_grouping).loc[top_groups].reset_index()


    if agg_map:
        result = df.groupby(agg_cols, dropna=False).agg(agg_map).reset_index()
    else:
        logger.info(f"[FORECAST_AGG]  No valid value columns to aggregate")
        return [], spec


    result = result.rename(columns={"_date_bucket": "date"})

    if grouping_cols and len([g for g in grouping_cols if g in result.columns]) > 1:
        valid_grouping = [g for g in grouping_cols if g in result.columns]
        result["group"] = result[valid_grouping].astype(str).agg(" - ".join, axis=1)
        result = result.drop(columns=valid_grouping)
    elif grouping_cols and len([g for g in grouping_cols if g in result.columns]) == 1:
        group_col = [g for g in grouping_cols if g in result.columns][0]
        result = result.rename(columns={group_col: "group"})

    sort_cols = ["group", "date"] if "group" in result.columns else ["date"]
    result = result.sort_values(sort_cols)
    result["date"] = result["date"].dt.strftime("%Y-%m-%d")

    output = result.to_dict(orient="records")

    logger.info(f"[FORECAST_AGG] Prepared {len(output)} records for forecasting")
    if "group" in result.columns:
        logger.info(f"[FORECAST_AGG] Groups: {result['group'].nunique()} unique groups")
    logger.info(f"[FORECAST_AGG] Date range: {result['date'].min()} to {result['date'].max()}")
    logger.info(f"[FORECAST_AGG] Value columns: {value_cols}")
    logger.info(f"[FORECAST_AGG] Sample output: {output}")

    return output, spec