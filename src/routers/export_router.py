from fastapi import APIRouter, status
from src.schemas.schemas import (ExportExcelRequest,
    ExportPdfRequest, 
    ExportWordRequest, 
    ExportPngRequest, 
    BaseResponse,)
from src.function.export_service import ExportService

router = APIRouter(prefix="/api/export", tags=["Export"])

export_service = ExportService()

@router.post("/pdf")
def export_pdf(req: ExportPdfRequest):
    return export_service.export_pdf_handler(req)

@router.post("/word")
def export_word(req: ExportWordRequest):
    return export_service.export_word_handler(req)


@router.post("/excel")
def export_excel(req: ExportExcelRequest):
    return export_service.export_excel_handler(req)


@router.post("/png")
def export_png(req: ExportPngRequest):
    return export_service.export_png_handler(req)

