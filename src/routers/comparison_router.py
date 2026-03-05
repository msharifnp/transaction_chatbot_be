from fastapi import APIRouter, status, Request
from fastapi.responses import FileResponse
from src.schemas.schemas import ComparisonRequest, ComparisonResponse
from src.function.comparison_service import ComparisonService

router = APIRouter(prefix="/api/comparison", tags=["Comparison"])

@router.post(
    "/comparison",
    response_model=ComparisonResponse,
    status_code=status.HTTP_200_OK,
)
def generate_comparison(req: ComparisonRequest, request: Request):

    TenantId = request.state.TenantId
    comparison_service = ComparisonService(TenantId=TenantId)
    return comparison_service.compare_invoices(req=req)

@router.get(
    "/download/{file_id}",
    status_code=status.HTTP_200_OK,
)
def download_comparison(file_id: int, request: Request):

    TenantId = request.state.TenantId
    comparison_service = ComparisonService(TenantId=TenantId)
    
    file_info = comparison_service.download_file(
        file_id=file_id,
        tenant_id=TenantId
    )
    
    return FileResponse(
        path=file_info["file_path"],
        filename=file_info["file_name"],
        media_type="application/pdf"
    )
    
    