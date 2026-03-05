from typing import Dict, List, Optional
from src.schemas.schemas import ComparisonRequest, ComparisonResponse
from src.db.db_service import DatabaseService
from src.db.redis_service import RedisService
from src.ai.comparison import ComparisonServices
from src.function.export_service import ExportService
from src.config.db_config import Config as DatabaseConfig
from src.config.redis_config import Config as RedisConfig
from dateutil.relativedelta import relativedelta
from datetime import date, datetime
from src.config.startup import model_startup
import json
from pathlib import Path
from datetime import timedelta,datetime
import traceback
from fastapi import HTTPException
import logging
logger = logging.getLogger(__name__)

BASE_DIR = Path(r"D:\Python_Project\exports\comparisons")

class ComparisonService:

    def __init__(self, TenantId: str = None):
        
        self.TenantId = TenantId
        self.db_config = DatabaseConfig.get_database_config()
        self.redis_config = RedisConfig.get_redis_config()
        self.db_service = DatabaseService(self.db_config)
        self.redis_service = RedisService(self.redis_config)        
        if self.TenantId:
            self.model_service = model_startup.get_or_create_service(self.TenantId)        
        self.comparison_service = ComparisonServices(self.model_service)
        self.export_service = ExportService()
    
    def compare_invoices(self, req: ComparisonRequest) -> ComparisonResponse:
    
        try:
            logger.info(f"[COMPARISON] Starting comparison")
            logger.info(f"[COMPARISON] AccountNumber: {req.AccountNumber}")
            logger.info(f"[COMPARISON] CurrentDate: {req.CurrentDate}")
            logger.info(f"[COMPARISON] CurrentDate type: {type(req.CurrentDate)}")
            logger.info(f"[COMPARISON] TenantId: {self.TenantId}")
            logger.info(f"{'='*80}")
            
            all_data = self.fetch_invoice_data(self.TenantId, req.AccountNumber, req.CurrentDate)
            logger.info(f"[COMPARISON] Fetched data count: {len(all_data) if all_data else 0}")
            
            if not all_data:
                error_response = ComparisonResponse(
                    success=False,
                    code=404,
                    message="No invoices found for this account",
                    errors=["NO_INVOICES_FOUND"]
                )
                logger.info(f"[COMPARISON] No invoices found")
                logger.info(f"[COMPARISON] Response: {json.dumps(error_response.dict(), indent=2)}")
                return error_response
            
            logger.info(f"[COMPARISON]  Fetched {len(all_data)} invoices")
            
            latest, previous, last_6_months = self.split_data(all_data)
            
            if not latest:
                error_response = ComparisonResponse(
                    success=False,
                    code=404,
                    message="No latest invoice found",
                    errors=["NO_LATEST_INVOICE"]
                )
                logger.info(f"[COMPARISON]  No latest invoice")
                logger.info(f"[COMPARISON] Response: {json.dumps(error_response.dict(), indent=2)}")
                return error_response
            
            if not previous:
                error_response = ComparisonResponse(
                    success=False,
                    code=404,
                    message="No previous month invoice found",
                    errors=["NO_PREVIOUS_INVOICE"]
                )
                logger.info(f"[COMPARISON]  No previous invoice")
                logger.info(f"[COMPARISON] Response: {json.dumps(error_response.dict(), indent=2)}")
                return error_response
            
            logger.info(f"[COMPARISON] Latest invoice date: {latest.get('InvoiceDate')}")
            logger.info(f"[COMPARISON] Previous invoice date: {previous.get('InvoiceDate')}")
            logger.info(f"[COMPARISON] Historical invoices: {len(last_6_months)}")
            
            logger.info(f"[COMPARISON] Generating AI comparison...")
            comparison_text = self.comparison_service.generate_comparison(
                latest_invoice = latest,
                previous_month_invoice = previous,
                last_6_months = last_6_months,
                TenantId = self.TenantId,
                SessionId = f"{req.AccountNumber}_{datetime.now().isoformat()}"
            )
            
            logger.info(f"[COMPARISON] Generated AI comparison: {len(comparison_text)} characters")
            if not comparison_text or comparison_text.startswith("⚠️"):
                error_response = ComparisonResponse(
                    success=False,
                    code=500,
                    message="Failed to generate comparison",
                    errors=["COMPARISON_GENERATION_FAILED"]
                )
                logger.info(f"[COMPARISON]  Failed to generate comparison")
                logger.info(f"[COMPARISON] Response: {json.dumps(error_response.dict(), indent=2)}")
                return error_response
            
            logger.info(f"[COMPARISON]  Generated report ({len(comparison_text)} chars)")
                       
            buffer = self.export_service.export_pdf(
                content=comparison_text,
                title=f"Invoice Comparison - {req.AccountNumber}"
            )
            
            current_date_str = self._format_date(req.CurrentDate)
            file_name = f"{req.AccountNumber}-{current_date_str}.pdf"
            
            file_info = self._save_pdf_to_disk(
                buffer=buffer,
                tenant_id=self.TenantId,
                file_name=file_name,
                current_date=req.CurrentDate
            )
            
            file_record = self._save_file_metadata(
                tenant_id=self.TenantId,
                account_number=req.AccountNumber,
                file_name=file_name,
                file_path=file_info["relative_path"],
                file_size=file_info["file_size"]
            )
                       
            return ComparisonResponse(
                success=True,
                code=200,
                message="Comparison completed successfully",
                data={
                    "AccountNumber": req.AccountNumber,
                    "CurrentDate": req.CurrentDate,
                    "file_id": file_record["file_id"],
                    "file_name": file_name,
                    "file_size": file_info["file_size"],
                    "created_at": file_record["created_at"],
                    
                }
            )   
        except Exception as e:
            print(f"[COMPARSION] Error : {e}")
            traceback.print_exc()

            return ComparisonResponse(
                success=False,
                code=500,
                message=f"Comparison failed: {str(e)}",
                errors=["INTERNAL_ERROR"]
            )

    def fetch_invoice_data(self, TenantId: str, account_number: str, current_date) -> List[Dict]:

        if isinstance(current_date, str):
            current_date = datetime.strptime(current_date, "%Y-%m-%d").date()
        
        start_date = current_date - relativedelta(months=6)
        
        logger.info(f"[COMPARISON] Database query parameters:")
        logger.info(f"  - TenantId: {TenantId}")
        logger.info(f"  - AccountNumber: {account_number}")
        logger.info(f"  - Date range: {start_date} to {current_date}")
      
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
        
        rows = self.db_service.execute_query(sql, (start_date, current_date, TenantId, account_number))
        
        logger.info(f"[COMPARISON] Fetched {len(rows)} rows from database")
        
        return rows   
    
    def split_data(self, all_data: List[Dict]) -> tuple:
                
        if len(all_data) == 0:
            return None, None, []

        latest = all_data[0] if len(all_data) > 0 else None
        previous = all_data[1] if len(all_data) > 1 else None
        last_6_months = all_data[1:8] if len(all_data) > 2 else []  
        
        return latest, previous, last_6_months

    def _save_pdf_to_disk(self, buffer, tenant_id, file_name, current_date):
        
        year = str(current_date.year)
        month = str(current_date.month).zfill(2)
        
        dir_path = BASE_DIR / tenant_id / year / month
        dir_path.mkdir(parents=True, exist_ok=True)  
        
        full_path = dir_path / file_name
        
        with open(full_path, 'wb') as f:
            f.write(buffer.getvalue())
        
        file_size = full_path.stat().st_size
        
        relative_path = f"{tenant_id}/{year}/{month}/{file_name}"
        
        return {
            "relative_path": relative_path,
            "file_size": file_size
        }
    
    
    def _save_file_metadata(
        self,
        tenant_id: str,
        account_number: str,
        file_name: str,
        file_path: str, 
        file_size: int
    ) -> dict:
     
    
        expires_at = datetime.now() + timedelta(days=90)
        
        sql = """
        INSERT INTO "data"."GeneratedFiles" (
            "TenantId",
            "AccountNumber",
            "FileName",
            "FilePath",
            "FileSize",
            "FileType",
            "ExpiresAt"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING "FileId", "CreatedAt"
        """
        
        result = self.db_service.execute_query(
            sql,
            (
                tenant_id,
                account_number,
                file_name,
                file_path,  
                file_size,
                "comparison_report",
                expires_at
            )
        )
        
        if not result or len(result) == 0:
            raise Exception("Failed to save file metadata to database")
        
        file_id = result[0]["FileId"]
        created_at = result[0]["CreatedAt"]
        
        logger.info(f"[COMPARISON] File metadata saved with FileId: {file_id}")
        
        return {
            "file_id": file_id,
            "created_at": created_at
        }
    
    def download_file(self, file_id: int, tenant_id: str) -> dict:
       
        sql = """
        SELECT 
            "FileId",
            "TenantId",
            "FileName",
            "FilePath",
            "FileSize",
            "ExpiresAt",
            "IsDeleted"
        FROM "data"."GeneratedFiles"
        WHERE "FileId" = %s
        """
 
        result = self.db_service.execute_query(sql, (file_id,))
        
        if not result or len(result) == 0:
            logger.warning(f"[DOWNLOAD]  File not found in database: FileId={file_id}")
            raise HTTPException(
                status_code=404,
                detail="File not found"
            )
        
        file_record = result[0]
        logger.info(f"[DOWNLOAD]  Retrieved file record: FileId={file_id}, TenantId={file_record['TenantId']}, FileName={file_record['FileName']}")
        if file_record["TenantId"] != tenant_id:
            logger.warning(f"[DOWNLOAD]  TenantId mismatch: FileId={file_id}, Expected={tenant_id}, Got={file_record['TenantId']}")
            raise HTTPException(
                status_code=403,
                detail="Access denied"
            )

        if file_record.get("IsDeleted"):
            logger.warning(f"[DOWNLOAD]  File is deleted: FileId={file_id}")
            raise HTTPException(
                status_code=410,
                detail="File has been deleted"
            )
        
        expires_at = file_record.get("ExpiresAt")

        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)

        if expires_at and datetime.now() > expires_at:
            logger.warning(f"[DOWNLOAD]  File expired: FileId={file_id}, ExpiresAt={expires_at}")
            raise HTTPException(
                status_code=410,
                detail="File has expired"
            )
        
        relative_path = file_record["FilePath"]
        
        try:
            full_path = (BASE_DIR / relative_path).resolve()
            base_dir_resolved = BASE_DIR.resolve()

            if not str(full_path).startswith(str(base_dir_resolved)):
                raise ValueError("Path traversal detected")

        except Exception:
            logger.warning(f"[DOWNLOAD]  Path traversal attempt: {relative_path}")
            raise HTTPException(
                status_code=403,
                detail="Access denied"
            )
        
        if not full_path.exists():
            logger.warning(f"[DOWNLOAD]  File not found on disk: {full_path}")
            self.db_service.execute_update(
                'UPDATE "data"."GeneratedFiles" SET "IsDeleted" = TRUE WHERE "FileId" = %s',
                (file_id,)
            )
            raise HTTPException(
                status_code=404,
                detail="File not found on server"
            )
        
        if not full_path.is_file():
            logger.warning(f"[DOWNLOAD]  Path is not a file: {full_path}")
            raise HTTPException(
                status_code=400,
                detail="Invalid file"
            )
        
        if full_path.suffix.lower() != ".pdf":
            logger.warning(f"[DOWNLOAD]  Invalid file type: {full_path.suffix}")
            raise HTTPException(
                status_code=400,
                detail="Only PDF files can be downloaded"
            )
        
        affected_rows = self.db_service.execute_update(
            """
            UPDATE "data"."GeneratedFiles" 
            SET "DownloadCount" = "DownloadCount" + 1,
                "LastDownloadedAt" = NOW()
            WHERE "FileId" = %s
            """,
            (file_id,)
        )
        
        logger.info(f"[DOWNLOAD]  File validated: FileId={file_id}, Path={full_path}")
        logger.info(f"[DOWNLOAD] Updated download stats (affected {affected_rows} rows)")
        
        return {
            "file_path": str(full_path),
            "file_name": file_record["FileName"],
            "file_size": file_record["FileSize"]
        }
    
    def _format_date(self, date_input) -> str:
        
        if isinstance(date_input, date):
            return date_input.isoformat()
        elif isinstance(date_input, str):
            return date_input
        else:
            return str(date_input)
        
        
        
        
        
