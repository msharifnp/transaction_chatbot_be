from typing import Dict, List
from datetime import date
from src.db.db_service import DatabaseService
from src.db.redis_service import RedisService
from src.config.db_config import Config as DatabaseConfig
from src.config.redis_config import Config as RedisConfig
from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict
from datetime import datetime, date
import uuid
import logging
logger = logging.getLogger(__name__)


class InvoiceFetchRequest(BaseModel):
    FromDate: date = Field(..., description="Start date for invoice range")
    ToDate: date = Field(..., description="End date for invoice range")


class InvoiceFetchResponse(BaseModel):
    success: bool
    code: int
    message: str
    errors: List[str] = []
    data: Optional[Dict[str, Any]] = None


class InvoiceFetchService:
   
    def __init__(self):
        self.db_config = DatabaseConfig.get_database_config()
        self.redis_config = RedisConfig.get_redis_config()
        self.db_service = DatabaseService(self.db_config)
        self.redis_service = RedisService(self.redis_config)
    
    
    def fetch_invoices(
        self, 
        req: InvoiceFetchRequest, 
        tenant_id: str,
        session_id: Optional[str] = None
    ) -> InvoiceFetchResponse:
        
        try:
           
            if not session_id:
                session_id = str(uuid.uuid4())
            
            logger.info(f"[FETCH]  Fetching invoices from {req.FromDate} to {req.ToDate}")
            logger.info(f"[FETCH] TenantId: {tenant_id}, SessionId: {session_id}")
            
            invoices = self.fetch_invoice_data(
                tenant_id=tenant_id,
                from_date=req.FromDate,
                to_date=req.ToDate
            )
            
            if not invoices:
                return InvoiceFetchResponse(
                    success=False,
                    code=404,
                    message="No invoices found for the selected date range",
                    errors=["NO_INVOICES_FOUND"]
                )
            
            logger.info(f"[FETCH]  Found {len(invoices)} invoices")
            
            columns = list(invoices[0].keys()) if invoices else []
            rows = invoices
            
            return InvoiceFetchResponse(
                success=True,
                code=200,
                message=f"Successfully fetched {len(invoices)} invoices",
                data={
                    "response_type": "database",
                    "columns": columns,
                    "rows": rows,
                    "count": len(rows),
                    "session_id": session_id,
                    "from_date": str(req.FromDate),
                    "to_date": str(req.ToDate)
                }
            )
            
        except Exception as e:
            logger.error(f"[FETCH]  Error: {e}")
            import traceback
            traceback.print_exc()
            
            return InvoiceFetchResponse(
                success=False,
                code=500,
                message=f"Failed to fetch invoices: {str(e)}",
                errors=["INTERNAL_ERROR"]
            )
    
    
    def fetch_invoice_data(
        self, 
        tenant_id: str, 
        from_date: date, 
        to_date: date
    ) -> List[Dict]:
      
        sql = """
        SELECT 
           
            "InvoiceDate", 
            "BillReceiveDate", 
            "AccountNumber", 
            "InvoiceNumber", 
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
            "DepartmentName"
        FROM "data"."ChatInvoices"
        WHERE "InvoiceDate" BETWEEN %s AND %s
          AND "TenantId" = %s
        ORDER BY "InvoiceDate" DESC, "AccountNumber"
        """
        
        rows = self.db_service.execute_query(sql, (from_date, to_date, tenant_id))
        logger.info(f"[FETCH] Fetched {len(rows)} rows from database")
        return rows