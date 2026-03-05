from fastapi import APIRouter, HTTPException, Query, Request
from typing import Optional
from datetime import date
from src.function.invoice_fetch_service import (
    InvoiceFetchRequest, 
    InvoiceFetchResponse,
    InvoiceFetchService
)

router = APIRouter(prefix="/api/invoices", tags=["invoices"])

fetch_service = InvoiceFetchService()


@router.get("/fetch", response_model=InvoiceFetchResponse)
async def fetch_invoices_get(
    request: Request,
    from_date: date = Query(..., alias="FromDate", description="Start date (YYYY-MM-DD)"),
    to_date: date = Query(..., alias="ToDate", description="End date (YYYY-MM-DD)")
):
    try:
        tenant_id = request.state.TenantId
       
        
        req = InvoiceFetchRequest(
            FromDate=from_date,
            ToDate=to_date
        )
        
        response = fetch_service.fetch_invoices(
            req=req,
            tenant_id=tenant_id,
            
        )
        
        if not response.success:
            raise HTTPException(
                status_code=response.code,
                detail={
                    "success": False,
                    "message": response.message,
                    "errors": response.errors
                }
            )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": f"Internal server error: {str(e)}",
                "errors": ["INTERNAL_ERROR"]
            }
        )