from typing import Dict, List, Optional
from src.schemas.schemas import ComparisonRequest, ComparisonResponse
from src.db.db_service import DatabaseService
from src.db.redis_service import RedisService
from src.ai.gemini_service import GeminiService
from src.function.export_service import ExportService
from src.config.db_config import Config as DatabaseConfig
from src.config.redis_config import Config as RedisConfig
from src.db.model_service import ModelService
from src.config.model_config import Config as GeminiConfig
from src.schemas.schemas import ExportPdfRequest
from dateutil.relativedelta import relativedelta
from datetime import date


class ComparisonService:
    """Service for invoice comparison."""
    
    def __init__(self):
        # Initialize configs
        self.db_config = DatabaseConfig.get_database_config()
        self.redis_config = RedisConfig.get_redis_config()
        self.model_config = GeminiConfig.get_gemini_config()
        
        # Initialize services
        self.db_service = DatabaseService(self.db_config)
        self.redis_service = RedisService(self.redis_config)
        self.model_service = ModelService(config=self.model_config)
        self.gemini_service = GeminiService(self.model_service)
        self.export_service = ExportService()
    
    
    def compare_invoices(self, req: ComparisonRequest) -> ComparisonResponse:
       
        try:
            print(f"[COMPARISON] 🔍 Starting for {req.AccountNumber}")
            print(f"[COMPARISON] Using frontend CurrentDate: {req.CurrentDate}")
            
            # Step 1: Fetch data from database
            all_data = self.fetch_invoice_data(req.TenantId, req.AccountNumber, req.CurrentDate)
            print(f"All invoice data : {all_data}")
            
            if not all_data:
                return ComparisonResponse(
                    success=False,
                    code=404,
                    message="No invoices found for this account",
                    errors=["NO_INVOICES_FOUND"]
                )
            
            print(f"[COMPARISON] ✅ Fetched {len(all_data)} invoices")
            
            # Step 2: Split data
            latest, previous, last_6_months = self.split_data(all_data)
            print(f" spliited latest : {latest}")
            print(f" spliited previous : {previous}")
            print(f" spliited last_6_months : {last_6_months}")
            
            if not latest:
                return ComparisonResponse(
                    success=False,
                    code=404,
                    message="No latest invoice found",
                    errors=["NO_LATEST_INVOICE"]
                )
            
            if not previous:
                return ComparisonResponse(
                    success=False,
                    code=404,
                    message="No previous month invoice found",
                    errors=["NO_PREVIOUS_INVOICE"]
                )
            
            print(f"[COMPARISON] Latest: {latest.get('InvoiceDate')}")
            print(f"[COMPARISON] Previous: {previous.get('InvoiceDate')}")
            print(f"[COMPARISON] Historical: {len(last_6_months)} invoices")
            
            # Step 3: Generate AI comparison
            comparison_text = self.gemini_service.generate_comparison(
                latest_invoice = latest,
                previous_month_invoice = previous,
                last_6_months = last_6_months
            )
            
            if not comparison_text or comparison_text.startswith("⚠️"):
                return ComparisonResponse(
                    success=False,
                    code=500,
                    message="Failed to generate comparison",
                    errors=["COMPARISON_GENERATION_FAILED"]
                )
            
            print(f"[COMPARISON] ✅ Generated report ({len(comparison_text)} chars)")
            
            # Step 4: Store in Redis
            comparison_index = self.redis_service.store_message(
                TenantId=req.TenantId,
                SessionId=req.SessionId,
                role="assistant",
                content=comparison_text,
                metadata={
                    "intent": "comparison",
                    "type": "comparison_report",
                    "account_number": req.AccountNumber,
                    "latest_date": latest.get('InvoiceDate'),
                    "previous_date": previous.get('InvoiceDate')
                }
            )
            
            print(f"[COMPARISON] ✅ Stored in Redis at index {comparison_index}")
            
            
            
            
            pdf_req = ExportPdfRequest(
                TenantId=req.TenantId,
                SessionId=req.SessionId,
                index=comparison_index,
                title=f"Invoice Comparison - {req.AccountNumber}",
                output_dir=r"D:\Python_Project\exports\comparisons"
            )
            
            pdf_response = self.export_service.generate_pdf_to_disk(pdf_req)

            if not pdf_response or not pdf_response.get("success"):
                return ComparisonResponse(
                    success=False,
                    code=500,
                    message="Comparison generated but PDF export failed",
                    errors=["PDF_EXPORT_FAILED"]
                )

            pdf_path = pdf_response["file_path"]
            
            return ComparisonResponse(
                success=True,
                code=200,
                message="Comparison completed successfully",
                data={
                    "response_type": "comparison",
                    "tenant_id": req.TenantId,
                    "session_id": req.SessionId,
                    "account_number": req.AccountNumber,
                    "file_path": pdf_path
                }
            )

            
        except Exception as e:
            print(f"[COMPARISON] ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            
            return ComparisonResponse(
                success=False,
                code=500,
                message=f"Comparison failed: {str(e)}",
                errors=["INTERNAL_ERROR"]
            )
    
    
    def fetch_invoice_data(self, tenant_id: str, account_number: str, current_date: date) -> List[Dict]:
        
        start_date = current_date - relativedelta(months=6)
      
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
          AND "AccountNumber" = %s
        ORDER BY "InvoiceDate" DESC
        """
        
        
        
        rows = self.db_service.execute_query(sql, (start_date,current_date,tenant_id, account_number))
        
        print(f"[COMPARISON] Fetched {len(rows)} rows from database")
        
        return rows
    
    
    def split_data(self, all_data: List[Dict]) -> tuple:
                
        if len(all_data) == 0:
            return None, None, []
        
        # Data is already sorted DESC by InvoiceDate
        latest = all_data[0] if len(all_data) > 0 else None
        previous = all_data[1] if len(all_data) > 1 else None
        last_6_months = all_data[1:8] if len(all_data) > 2 else []  # Next 6 invoices
        
        return latest, previous, last_6_months
    
    
  