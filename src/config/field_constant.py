from typing import Dict, List, Set

# Field type definitions
FIELD_TYPES: Dict[str, str] = {
    "TenantId": "string",
    "InvoiceStatusType": "string",
    "InvoiceApprovalStatus": "string",
    "VerificationResult": "string",
    "InvoiceNumber": "string",
    "AccountNumber": "string",
    "BandWidth": "string",
    "ServiceName": "string",
    "SiteName": "string",
    "SiteAddress": "string",
    "SiteLocationCode": "string",
    "CostCode": "string",
    "CostName": "string",
    "LineName": "string",
    "ConnectionName": "string",
    "ProviderName": "string",
     # Numeric
    "NetTotal": "number",
    "TotalTax": "number",
    "GrandTotal": "number",
    "ExpectedAmount": "number",
    "RentalCharge": "number",
    "ChargePerMinute": "number",
    # Date
    "InvoiceDate": "datetime",
}

# Field aliases for natural language queries
FIELD_ALIASES: Dict[str, str] = {
    "invoice status": "InvoiceStatusType",
    "status": "InvoiceStatusType",
    "approval status": "InvoiceApprovalStatus",
    "approval": "InvoiceApprovalStatus",
    "provider": "ProviderName",
    "vendor": "ProviderName",
    "bandwidth": "BandWidth",
    "account": "AccountNumber",
    "invoice": "InvoiceNumber",
    "amount": "GrandTotal",
    "total": "GrandTotal",
    "date": "InvoiceDate",
    "connection name": "ConnectionName",
    "service name": "ServiceName",
    "site name":"SiteName",
    "site address": "SiteAddress",
    "address": "SiteAddress",
    "site location code": "SiteLocationCode",
    "location code": "SiteLocationCode",
    "site code": "SiteLocationCode",
    "site location code": "SiteLocationCode",
    "cost code": "CostCode",
    "cost name": "CostName",
    "line name":"LineName",
    "line":"LineName",
    "connection name":"ConnectionName",
    "net total":"NetTotal",
    "total tax":"TotalTax",
    "tax":"TotalTax",
    "expected amount":"ExpectedAmount",
    "rental charge": "RentalCharge",
    "rental": "RentalCharge",
    "charge per minute": "ChargePerMinute"
        
}

PASS_THROUGH_FIELDS = {
    # field: regex pattern (anchor to avoid partials)
    "AccountNumber": r"^[A-Za-z0-9\-_/]{3,64}$"
}


# Canonical values for string fields
CANON_VALUES: Dict[str, List[str]] = {
    "InvoiceStatusType": [
        "Accepted",
        "System Accepted",
        "System Disputed",
        "Disputed"
    ],
    "InvoiceApprovalStatus": [
        "Pending",
        "Approval Completed",
        "Initiated",
        "Approval InProgress"
    ],
    "VerificationResult": [
        "Verified",
        "Not Verified",
        "Unknown"
    ],
    "ProvidersName": [
        "Etisalat",
        "Du",
        "Mobily"
    ],
    "ConnectionName":[
        "Data",
        "Fixed voice data",
        "Voice"
    ],
    "LineName":[
        "ADSL",
        "Digital Internet",
        "Internet leased line",
        "ISDN BRI",
        "MPLS Backup",
        "MPLS Primary",
        "PABX Lines"
    ],
    "CostName":[
        "Auto-Mobile",
        "Real-Estate",
        "Retail"
    ],
    "CostCode":[
        "AM101",
        "R101",
        "RE101"
    ],
    "SiteName":[
        "Dubai",
        "Abu Dhabi",
        "Sharjah",
        "Ajman",
        "Ras Al Khaimah",
        "Umm Al Quwain",
        "Fujairah"
    ],
    "SiteLocationCode":[
        "DB101",
        "AD101",
        "SH101",
        "AJ101,",
        "RAK101",
        "UAQ101",
        "FU101"
    ],
    "ServiceName":[
        "Broadband 100gb 2gb/s",
        "Broadband 150 GB 500mbps",
        "Broadband 200gb 1gb/s"
    ],
    "PaymentStatus":[
        "Ready For Payment",
        "Partially Settled",
        "Settled"
    ]
        
}

# Default fields to select in search queries
DEFAULT_SELECT_FIELDS: List[str] = [
    '"Id"',
    '"InvoiceDate"',
    '"BillReceiveDate"',
    '"AccountNumber"',
    '"InvoiceNumber"' ,
    '"InvoiceStatusType"',
    '"InvoiceApprovalStatus"',
    '"PaymentStatus"',
    '"NetTotal"',
    '"TotalTax"',
    '"GrandTotal"',
    '"UsageCharge"',
    '"ExpectedAmount"',
    '"VerificationResult"',
    '"RentalCharge"',
    '"BandWidth"',
    '"ChargePerMinute"',
    '"ServiceName"',
    '"SiteName"',
    '"SiteLocationCode"',
    '"SiteAddress"',
    '"CostName"',
    '"CostCode"',
    '"LineName"',
    '"ConnectionName"',
    '"ProviderName"',
    '"DepartmentName"',
    
]

# UI display columns
UI_COLUMNS: List[str] = [
    "InvoiceDate",
    "BillReceiveDate",
    "AccountNumber",
    "InvoiceNumber" ,
    "InvoiceStatusType",
    "InvoiceApprovalStatus",
    "PaymentStatus",
    "NetTotal",
    "TotalTax",
    "GrandTotal",
    "UsageCharge",
    "ExpectedAmount",
    "VerificationResult",
    "RentalCharge",
    "BandWidth",
    "ChargePerMinute",
    "ServiceName",
    "SiteName",
    "SiteLocationCode",
    "SiteAddress",
    "CostName",
    "CostCode",
    "LineName",
    "ConnectionName",
    "ProviderName",
    "DepartmentName",
]

# Field-to-field comparison operators
FIELD_TO_FIELD_OPS: Set[str] = {
    "!=", "<>", "not equal", "not equals", "ne",
    ">", "<", ">=", "<=",
    "ge", "gt", "le", "lt",
    "eq", "=", "equal", "equals", "eql"
}


MONTH_NAMES = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6,
    "july": 7, "jul": 7, "august": 8, "aug": 8, "agust": 8, "agustt": 8,
    "september": 9, "sep": 9, "sept": 9, "october": 10, "oct": 10,
    "november": 11, "nov": 11, "december": 12, "dec": 12,
}



SYSTEM_INSTRUCTION_SVG = """
You are a chart renderer that outputs pure SVG.

HARD OUTPUT RULES
- Output ONE valid <svg>…</svg> element, and NOTHING else.
- The FIRST characters of your response MUST be "<svg".
- NO code fences. NO markdown. NO prose. NO HTML shell. NO <script>.
- Use vector primitives (rect, line, text, circle, path, g). No external assets.
- Include width/height (e.g., width="960" height="560") and a sensible viewBox.

CHART RULES
- If the request says "by/against/per X", treat X as the category axis.
- You MAY compute aggregate(s) (sum/avg/count) inside your rendering logic from the raw rows the user gives you.
- Parse numbers from strings if needed. Treat blank/None as 0 or skip.
- Draw axis ticks/labels, a legend if multiple measures, and readable text (12–14px).
- Dark background OK; ensure sufficient contrast for text and bars/lines.
"""

