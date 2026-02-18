"""
Transaction Type Lookups for ITRN_HISTORY_VW
Maps numeric codes to human-readable descriptions
"""

# Source Type Mapping (Transaction Types)
SOURCE_TYPE_LOOKUP = {
    1: "Inventory Adjustments",
    2: "Commitments",
    3: "Dependent Demands",
    4: "Independent Demands",
    5: "Independent Demand Issues",
    6: "Dependent Demand Issues",
    7: "Manufacturing Receipts",
    8: "Manufacturing Supplies",
    9: "Purchased Receipts",
    10: "Purchased Supplies",
    11: "Scrap",
    12: "Transfer within a ML",
    13: "Transfer across ML",
    14: "Dependent Demands Issues Adjustments",
    15: "Independent Demands Issues Adjustments",
    16: "Transfer Order Receipt",
    17: "Pre-Assigned Serial Numbers",
    18: "Assembly Instruction Receipts",
    19: "Shipping Instruction Drop Ship",
    20: "Relay Ship Receipts",
    21: "Voucher Receipt",
    37: "Return Material from Site"
}

# Source Qualifier Mapping (Transaction Qualifiers)
SOURCE_QUALIFIER_LOOKUP = {
    1: "By Physical Inventory",
    2: "By Cycle Count",
    3: "By Inventory",
    4: "For Released Work Order",
    5: "For Sales Orders",
    6: "For Repetitive Schedule",
    7: "For Backflush",
    8: "For Service Work Order",
    9: "For Inter Plant Orders",
    10: "For Purchase Order (Inv)",
    11: "For Purchase Orders",
    12: "For Purchase Orders (WO)",
    13: "To Location (MRB Results)",
    14: "To Location (Inspection Results)",
    15: "To Location (Loc to Loc)",
    16: "To Location (Assembly Storage Location)",
    17: "From Location (MRB Results)",
    18: "From Location",
    19: "From Location",
    20: "From Location (Ass. Storage Location)",
    21: "To Location",
    22: "From Location",
    23: "By Cost",
    24: "For Purchase Orders (SO)",
    26: "Assembly",
    27: "Operation",
    28: "Component",
    29: "Return to Supplier for Credit",
    30: "Return to Supplier for Replacement",
    31: "Return to Supplier for Rework",
    32: "For Contract Orders",
    33: "For Return Goods Authorization",
    34: "For Configurator Sales Order",
    35: "For Transfer Order",
    36: "For Installation Order",
    38: "For Contract Management",
    39: "For Electronic Kanban Ordering",
    40: "For Customer Release",
    41: "For Supplier Releasing",
    42: "For Customer Releasing Return",
    43: "To Location (Tool Calibration)",
    44: "From Location (Tool Calibration)",
    45: "For Field Service Order",
    47: "For Production Plan",
    49: "For RSI",
    50: "For Consignment PO",
    51: "For Consignment Contract Order"
}


def get_source_type_description(source_type_code):
    """
    Get human-readable description for Source Type code.
    
    Args:
        source_type_code: Numeric source type code (1-37)
    
    Returns:
        Description string or "Unknown Source Type (N)" if not found
    """
    if source_type_code is None:
        return "Unknown"
    
    code = int(source_type_code) if isinstance(source_type_code, (int, float, str)) else None
    return SOURCE_TYPE_LOOKUP.get(code, f"Unknown Source Type ({code})")


def get_source_qualifier_description(qualifier_code):
    """
    Get human-readable description for Source Qualifier code.
    
    Args:
        qualifier_code: Numeric qualifier code (1-51)
    
    Returns:
        Description string or "Unknown Qualifier (N)" if not found
    """
    if qualifier_code is None:
        return "Unknown"
    
    code = int(qualifier_code) if isinstance(qualifier_code, (int, float, str)) else None
    return SOURCE_QUALIFIER_LOOKUP.get(code, f"Unknown Qualifier ({code})")


def enrich_transaction_data(transactions):
    """
    Enrich transaction records by replacing numeric codes with descriptions.
    
    Args:
        transactions: List of transaction dictionaries with "Source Type" and "Source Qualifier" keys
    
    Returns:
        Enriched list with codes replaced by descriptions
    """
    enriched = []
    
    for txn in transactions:
        enriched_txn = txn.copy()
        
        # Replace Source Type code with description
        if "Source Type" in enriched_txn:
            code = enriched_txn["Source Type"]
            enriched_txn["Source Type"] = get_source_type_description(code)
        
        # Replace Source Qualifier code with description
        if "Source Qualifier" in enriched_txn:
            code = enriched_txn["Source Qualifier"]
            enriched_txn["Source Qualifier"] = get_source_qualifier_description(code)
        
        enriched.append(enriched_txn)
    
    return enriched
