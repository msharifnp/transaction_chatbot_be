import pandas as pd
from typing import List, Dict, Any


def aggregate_for_summary(
    rows: List[Dict],
    spec: Dict[str, Any],
    field_types: Dict[str, str]
) -> Dict[str, Any]:
   
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
                summary["aggregations"]["risk"] = _aggregate_risk_enhanced(df, agg_spec, column_mapping)
            
            print(f"[AGGREGATOR] ✅ Completed {category} aggregation")
                
        except Exception as e:
            print(f"[AGGREGATOR] ⚠️ Error in {category}: {e}")
            import traceback
            traceback.print_exc()
            summary["aggregations"][category] = {"error": str(e)}
    
    print(f"[AGGREGATOR] ✅ All aggregations complete")
    
    return summary


def _create_column_mapping(columns: pd.Index) -> Dict[str, str]:
    """
    Create a case-insensitive column mapping.
    Maps lowercase column names to actual column names.
    """
    mapping = {}
    for col in columns:
        mapping[col.lower()] = col
    return mapping


def _get_column_name(spec_column: str, column_mapping: Dict[str, str]) -> str:
    """Get the actual column name from spec column (case-insensitive)."""
    return column_mapping.get(spec_column.lower())


# ============================================================================
# AGGREGATION FUNCTIONS
# ============================================================================

def _aggregate_status(df: pd.DataFrame, spec: Dict, column_mapping: Dict) -> List[Dict]:
    """Aggregate by invoice status fields."""
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
    """Aggregate by provider with top N filtering."""
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
    """Aggregate by cost center/location with top N filtering."""
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
    """Aggregate by service type with top N filtering."""
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
    """Calculate overall financial metrics."""
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
    """Aggregate by time periods (month/quarter)."""
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


def _aggregate_risk_enhanced(df: pd.DataFrame, spec: Dict, column_mapping: Dict) -> Dict:
    """
    Enhanced risk aggregation with separate disputed/accepted breakdowns.
    
    Returns detailed breakdowns for:
    - Disputed invoices (by status, approval, payment, verification)
    - Accepted invoices (by status, approval, payment, verification)
    - Not verified issues
    - Pending approvals
    """
    sub_aggs = spec.get("sub_aggregations", [])
    
    # Get the actual GrandTotal column name
    grand_total_col = _get_column_name("grandtotal", column_mapping)
    if not grand_total_col or grand_total_col not in df.columns:
        return {"error": "GrandTotal column not found"}
    
    risk_summary = {}
    total_records = len(df)
    
    # Process each sub-aggregation
    for sub_agg in sub_aggs:
        agg_name = sub_agg.get("name")
        filter_field = sub_agg.get("filter_field")
        filter_values = sub_agg.get("filter_values", [])
        group_by = sub_agg.get("group_by", [])
        
        # Map filter field to actual column name
        actual_filter_field = _get_column_name(filter_field, column_mapping)
        
        if not actual_filter_field or actual_filter_field not in df.columns:
            continue
        
        # Filter data
        filtered_df = df[df[actual_filter_field].isin(filter_values)]
        
        # If group_by exists, do detailed breakdown
        if group_by:
            # Map group_by columns to actual names
            actual_group_cols = []
            for col in group_by:
                actual_col = _get_column_name(col, column_mapping)
                if actual_col and actual_col in df.columns:
                    actual_group_cols.append(actual_col)
            
            if actual_group_cols and len(filtered_df) > 0:
                # Detailed breakdown
                breakdown = filtered_df.groupby(actual_group_cols, dropna=False).agg(
                    total_amount=(grand_total_col, 'sum'),
                    invoice_count=(grand_total_col, 'count')
                ).reset_index()
                
                breakdown = breakdown.fillna("Unknown")
                
                risk_summary[agg_name] = {
                    "summary": {
                        "total_count": len(filtered_df),
                        "total_amount": float(filtered_df[grand_total_col].sum()),
                        "percentage": round(len(filtered_df) / total_records * 100, 1) if total_records > 0 else 0
                    },
                    "breakdown": breakdown.to_dict('records')
                }
            else:
                # No detailed breakdown possible
                count = len(filtered_df)
                total_amount = float(filtered_df[grand_total_col].sum()) if count > 0 else 0
                
                risk_summary[agg_name] = {
                    "summary": {
                        "total_count": count,
                        "total_amount": total_amount,
                        "percentage": round(count / total_records * 100, 1) if total_records > 0 else 0
                    }
                }
        else:
            # Simple count and amount (for not_verified, pending_approval)
            count = len(filtered_df)
            total_amount = float(filtered_df[grand_total_col].sum()) if count > 0 else 0
            percentage = round(count / total_records * 100, 1) if total_records > 0 else 0
            
            risk_summary[agg_name] = {
                "count": count,
                "total_amount": total_amount,
                "percentage": percentage
            }
    
    return risk_summary
