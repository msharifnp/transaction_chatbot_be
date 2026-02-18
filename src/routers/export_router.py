from fastapi import APIRouter, Request, Query
from src.function.export_service import ExportService
from src.schemas.schemas import ExportPdfRequest,ExportWordRequest,ExportExcelRequest,ExportPngRequest

router = APIRouter(prefix="/api/export", tags=["Export"])

export_service = ExportService()

@router.get("/pdf")
def export_pdf(request: Request, 
               index: int = Query(...), 
               title: str | None = Query(None)):
     
    req = ExportPdfRequest(index=index, title=title)
    return export_service.export_pdf_handler(TenantId=request.state.TenantId, SessionId=request.state.SessionId, req=req)


@router.get("/word")
def export_word(request: Request, 
                index: int = Query(...), 
                title: str | None = Query(None)):
   
    req = ExportWordRequest(index=index, title=title)
    return export_service.export_word_handler(TenantId=request.state.TenantId, SessionId=request.state.SessionId, req=req)


@router.get("/excel")
def export_excel(request: Request, 
                 index: int = Query(...), 
                 sheet_name: str | None = Query(None)):
       
    req = ExportExcelRequest(index=index, sheet_name=sheet_name)
    return export_service.export_excel_handler(TenantId=request.state.TenantId, SessionId=request.state.SessionId , req=req)


@router.get("/png")
def export_png(request: Request, 
               index: int = Query(...), 
               width: int = Query(default=1920), 
               height: int = Query(default=1120)):
    
    req = ExportPngRequest(index=index, width=width, height=height)
    return export_service.export_png_handler(TenantId=request.state.TenantId, SessionId=request.state.SessionId, req=req)