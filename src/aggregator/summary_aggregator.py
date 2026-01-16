"""
summary_aggregator.py
Aggregates large invoice datasets into compact summaries using pandas.
FIXED: Now handles PascalCase, camelCase, and snake_case column names.
"""

import pandas as pd
from typing import List, Dict, Any


def aggregate_for_summary(
    rows: List[Dict],
    spec: Dict[str, Any],
    field_types: Dict[str, str]
) -> Dict[str, Any]:
    """
    Aggregate invoice data for summary analysis using pandas.
    
    Reduces millions of rows to a compact summary dict containing:
    - Status breakdowns
    - Top providers
    - Top cost centers
    - Financial metrics
    - Time series
    - Risk indicators
    
    Args:
        rows: List of invoice records as dicts
        spec: Aggregation specification from SummarySpecGenerator
        field_types: Mapping of column names to types (date, number, currency, text)
        
    Returns:
        Compact summary dict with all aggregations
    """
    if not rows:
        return {"error": "No data to aggregate", "total_records": 0}
    
    # Convert to DataFrame
    df = pd.DataFrame(rows)
    
    print(f"[AGGREGATOR] Processing {len(df)} rows, {len(df.columns)} columns")
    print(f"[AGGREGATOR] Available columns: {list(df.columns)[:10]}...")
    
    # Create case-insensitive column mapping
    column_mapping = _create_column_mapping(df.columns)
    
    # Convert date columns to datetime
    for col in df.columns:
        col_lower = col.lower()
        field_type = field_types.get(col) or field_types.get(col_lower)
        if field_type == "date":
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Convert numeric/currency columns to numeric
    for col in df.columns:
        col_lower = col.lower()
        field_type = field_types.get(col) or field_types.get(col_lower)
        if field_type in ["number", "currency"]:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Initialize summary structure
    summary = {
        "total_records": len(df),
        "aggregations": {}
    }
    
    # Process each aggregation category
    for agg_spec in spec.get("aggregations", []):
        category = agg_spec.get("category")
        
        try:
            if category == "status":
                summary["aggregations"]["status"] = _aggregate_status(df, agg_spec, column_mapping)
            
            elif category == "provider":
                summary["aggregations"]["provider"] = _aggregate_provider(df, agg_spec, column_mapping)
            
            elif category == "cost_center":
                summary["aggregations"]["cost_center"] = _aggregate_cost_center(df, agg_spec, column_mapping)
            
            elif category == "service":
                summary["aggregations"]["service"] = _aggregate_service(df, agg_spec, column_mapping)
            
            elif category == "financial":
                summary["aggregations"]["financial"] = _aggregate_financial(df, agg_spec, column_mapping)
            
            elif category == "time":
                summary["aggregations"]["time"] = _aggregate_time(df, agg_spec, column_mapping)
            
            elif category == "risk":
                summary["aggregations"]["risk"] = _aggregate_risk(df, agg_spec, column_mapping)
            
            print(f"[AGGREGATOR] ✅ Completed {category} aggregation")
                
        except Exception as e:
            print(f"[AGGREGATOR] ⚠️ Error in {category}: {e}")
            import traceback
            traceback.print_exc()
            summary["aggregations"][category] = {"error": str(e)}
    
    print(f"[AGGREGATOR] ✅ All aggregations complete")
    
    return summary


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _create_column_mapping(columns: pd.Index) -> Dict[str, str]:
    """
    Create a case-insensitive column mapping.
    Maps lowercase column names to actual column names.
    
    Examples:
        grandtotal -> GrandTotal
        grand_total -> GrandTotal
        nettotal -> NetTotal
    """
    mapping = {}
    for col in columns:
        mapping[col.lower()] = col
    return mapping


def _get_column_name(spec_column: str, column_mapping: Dict[str, str]) -> str:
    """
    Get the actual column name from spec column (case-insensitive).
    
    Args:
        spec_column: Column name from spec (any case)
        column_mapping: Lowercase to actual column mapping
        
    Returns:
        Actual column name or None if not found
    """
    return column_mapping.get(spec_column.lower())


# ============================================================================
# AGGREGATION FUNCTIONS
# ============================================================================

def _aggregate_status(df: pd.DataFrame, spec: Dict, column_mapping: Dict) -> List[Dict]:
    """
    Aggregate by invoice status fields.
    
    Returns breakdown of invoices by status with counts and totals.
    """
    group_cols = spec.get("group_by", [])
    
    # Map to actual column names
    actual_cols = []
    for col in group_cols:
        actual_col = _get_column_name(col, column_mapping)
        if actual_col and actual_col in df.columns:
            actual_cols.append(actual_col)
    
    if not actual_cols:
        return {"error": "No valid grouping columns for status"}
    
    # Get the actual GrandTotal column name
    grand_total_col = _get_column_name("grandtotal", column_mapping)
    if not grand_total_col or grand_total_col not in df.columns:
        return {"error": "GrandTotal column not found"}
    
    # Group and aggregate
    result = df.groupby(actual_cols, dropna=False).agg(
        total_amount=(grand_total_col, 'sum'),
        invoice_count=(grand_total_col, 'count')
    ).reset_index()
    
    # Convert to serializable format
    result = result.fillna("Unknown")
    
    return result.to_dict('records')


def _aggregate_provider(df: pd.DataFrame, spec: Dict, column_mapping: Dict) -> List[Dict]:
    """
    Aggregate by provider with top N filtering.
    
    Returns top providers by spend.
    """
    group_cols = spec.get("group_by", [])
    
    # Map to actual column names
    actual_cols = []
    for col in group_cols:
        actual_col = _get_column_name(col, column_mapping)
        if actual_col and actual_col in df.columns:
            actual_cols.append(actual_col)
    
    if not actual_cols:
        return {"error": "No valid grouping columns for provider"}
    
    # Get the actual GrandTotal column name
    grand_total_col = _get_column_name("grandtotal", column_mapping)
    if not grand_total_col or grand_total_col not in df.columns:
        return {"error": "GrandTotal column not found"}
    
    # Group and aggregate
    result = df.groupby(actual_cols, dropna=False).agg(
        total_amount=(grand_total_col, 'sum'),
        invoice_count=(grand_total_col, 'count')
    ).reset_index()
    
    # Sort by total amount descending
    result = result.sort_values('total_amount', ascending=False)
    
    # Apply top_n filter
    top_n = spec.get("top_n", 10)
    result = result.head(top_n)
    
    # Convert to serializable format
    result = result.fillna("Unknown")
    
    return result.to_dict('records')


def _aggregate_cost_center(df: pd.DataFrame, spec: Dict, column_mapping: Dict) -> List[Dict]:
    """
    Aggregate by cost center/location with top N filtering.
    
    Returns top cost centers by spend.
    """
    group_cols = spec.get("group_by", [])
    
    # Map to actual column names
    actual_cols = []
    for col in group_cols:
        actual_col = _get_column_name(col, column_mapping)
        if actual_col and actual_col in df.columns:
            actual_cols.append(actual_col)
    
    if not actual_cols:
        return {"error": "No valid grouping columns for cost_center"}
    
    # Get the actual GrandTotal column name
    grand_total_col = _get_column_name("grandtotal", column_mapping)
    if not grand_total_col or grand_total_col not in df.columns:
        return {"error": "GrandTotal column not found"}
    
    # Group and aggregate
    result = df.groupby(actual_cols, dropna=False).agg(
        total_amount=(grand_total_col, 'sum'),
        invoice_count=(grand_total_col, 'count')
    ).reset_index()
    
    # Sort by total amount descending
    result = result.sort_values('total_amount', ascending=False)
    
    # Apply top_n filter
    top_n = spec.get("top_n", 10)
    result = result.head(top_n)
    
    # Convert to serializable format
    result = result.fillna("Unknown")
    
    return result.to_dict('records')


def _aggregate_service(df: pd.DataFrame, spec: Dict, column_mapping: Dict) -> List[Dict]:
    """
    Aggregate by service type with top N filtering.
    
    Returns top services by spend.
    """
    group_cols = spec.get("group_by", [])
    
    # Map to actual column names
    actual_cols = []
    for col in group_cols:
        actual_col = _get_column_name(col, column_mapping)
        if actual_col and actual_col in df.columns:
            actual_cols.append(actual_col)
    
    if not actual_cols:
        return {"error": "No valid grouping columns for service"}
    
    # Get the actual GrandTotal column name
    grand_total_col = _get_column_name("grandtotal", column_mapping)
    if not grand_total_col or grand_total_col not in df.columns:
        return {"error": "GrandTotal column not found"}
    
    # Group and aggregate
    result = df.groupby(actual_cols, dropna=False).agg(
        total_amount=(grand_total_col, 'sum'),
        invoice_count=(grand_total_col, 'count')
    ).reset_index()
    
    # Sort by total amount descending
    result = result.sort_values('total_amount', ascending=False)
    
    # Apply top_n filter
    top_n = spec.get("top_n", 10)
    result = result.head(top_n)
    
    # Convert to serializable format
    result = result.fillna("Unknown")
    
    return result.to_dict('records')


def _aggregate_financial(df: pd.DataFrame, spec: Dict, column_mapping: Dict) -> Dict:
    """
    Calculate overall financial metrics.
    
    Returns sum, average, min, max for key financial fields.
    """
    metrics = {}
    
    # Get actual column names (case-insensitive)
    grand_total_col = _get_column_name("grandtotal", column_mapping)
    net_total_col = _get_column_name("nettotal", column_mapping)
    total_tax_col = _get_column_name("totaltax", column_mapping)
    usage_charge_col = _get_column_name("usagecharge", column_mapping)
    rental_charge_col = _get_column_name("rentalcharge", column_mapping)
    
    # Grand total metrics
    if grand_total_col and grand_total_col in df.columns:
        metrics['total_billed'] = float(df[grand_total_col].sum())
        metrics['average_invoice'] = float(df[grand_total_col].mean())
        metrics['min_invoice'] = float(df[grand_total_col].min())
        metrics['max_invoice'] = float(df[grand_total_col].max())
        metrics['median_invoice'] = float(df[grand_total_col].median())
    
    # Net total
    if net_total_col and net_total_col in df.columns:
        metrics['total_net'] = float(df[net_total_col].sum())
    
    # Tax
    if total_tax_col and total_tax_col in df.columns:
        metrics['total_tax'] = float(df[total_tax_col].sum())
    
    # Usage charges
    if usage_charge_col and usage_charge_col in df.columns:
        metrics['total_usage'] = float(df[usage_charge_col].sum())
    
    # Rental charges
    if rental_charge_col and rental_charge_col in df.columns:
        metrics['total_rental'] = float(df[rental_charge_col].sum())
    
    # Invoice count
    metrics['total_invoices'] = len(df)
    
    return metrics


def _aggregate_time(df: pd.DataFrame, spec: Dict, column_mapping: Dict) -> Dict:
    """
    Aggregate by time periods (month/quarter).
    
    Returns time series data and date range.
    """
    group_cols = spec.get("group_by", [])
    
    # Map to actual column name
    date_col = None
    for col in group_cols:
        actual_col = _get_column_name(col, column_mapping)
        if actual_col and actual_col in df.columns:
            date_col = actual_col
            break
    
    if not date_col:
        return {"error": "No valid date column for time aggregation"}
    
    # Get the actual GrandTotal column name
    grand_total_col = _get_column_name("grandtotal", column_mapping)
    if not grand_total_col or grand_total_col not in df.columns:
        return {"error": "GrandTotal column not found"}
    
    # Get time bucket (month, quarter, year)
    time_bucket = spec.get("time_bucket", "month")
    
    # Create copy to avoid modifying original
    df_copy = df.copy()
    
    # Check if column is already datetime
    if not pd.api.types.is_datetime64_any_dtype(df_copy[date_col]):
        df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors='coerce')
    
    # Create period column based on bucket
    try:
        if time_bucket == "month":
            df_copy['period'] = df_copy[date_col].dt.to_period('M').astype(str)
        elif time_bucket == "quarter":
            df_copy['period'] = df_copy[date_col].dt.to_period('Q').astype(str)
        elif time_bucket == "year":
            df_copy['period'] = df_copy[date_col].dt.to_period('Y').astype(str)
        else:
            df_copy['period'] = df_copy[date_col].dt.date.astype(str)
    except Exception as e:
        return {"error": f"Error creating time periods: {str(e)}"}
    
    # Aggregate by period
    result = df_copy.groupby('period', dropna=False).agg(
        total_amount=(grand_total_col, 'sum'),
        invoice_count=(grand_total_col, 'count')
    ).reset_index()
    
    # Sort by period
    result = result.sort_values('period')
    
    # Get date range
    date_range = {
        "min_date": df_copy[date_col].min().strftime('%Y-%m-%d') if not df_copy[date_col].isna().all() else None,
        "max_date": df_copy[date_col].max().strftime('%Y-%m-%d') if not df_copy[date_col].isna().all() else None
    }
    
    return {
        "date_range": date_range,
        "time_bucket": time_bucket,
        "by_period": result.to_dict('records')
    }


def _aggregate_risk(df: pd.DataFrame, spec: Dict, column_mapping: Dict) -> Dict:
    """
    Aggregate risk indicators (disputes, verification issues, pending approvals).
    
    Returns counts, amounts, and percentages for each risk category.
    """
    filters = spec.get("filters", [])
    
    # Get the actual GrandTotal column name
    grand_total_col = _get_column_name("grandtotal", column_mapping)
    if not grand_total_col or grand_total_col not in df.columns:
        return {"error": "GrandTotal column not found"}
    
    risk_summary = {}
    total_records = len(df)
    
    # Process each risk filter
    for filter_spec in filters:
        risk_type = filter_spec.get("type")
        field = filter_spec.get("field")
        values = filter_spec.get("values", [])
        
        # Map to actual column name
        actual_field = _get_column_name(field, column_mapping)
        
        if not actual_field or actual_field not in df.columns:
            continue
        
        # Filter data for this risk type
        risk_df = df[df[actual_field].isin(values)]
        
        count = len(risk_df)
        total_amount = float(risk_df[grand_total_col].sum()) if count > 0 else 0
        percentage = round(count / total_records * 100, 1) if total_records > 0 else 0
        
        risk_summary[risk_type] = {
            "count": count,
            "total_amount": total_amount,
            "percentage": percentage
        }
    
    # Add overall risk score (percentage of invoices with any risk)
    if risk_summary:
        total_risk_count = sum(r["count"] for r in risk_summary.values())
        risk_summary["overall_risk"] = {
            "count": total_risk_count,
            "percentage": round(total_risk_count / total_records * 100, 1) if total_records > 0 else 0
        }
    
    return risk_summary


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def validate_aggregation_spec(spec: Dict[str, Any]) -> bool:
    """
    Validate that aggregation spec has correct structure.
    
    Args:
        spec: Aggregation specification dict
        
    Returns:
        True if valid, False otherwise
    """
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
    """
    Calculate approximate size of aggregated summary in characters.
    
    Args:
        summary: Aggregated summary dict
        
    Returns:
        Approximate size in characters
    """
    import json
    return len(json.dumps(summary))