"""
src/routers/comparison_router.py
Comparison API endpoint
"""

from fastapi import APIRouter, status
from src.schemas.schemas import ComparisonRequest, ComparisonResponse
from src.function.comparison_service import ComparisonService


router = APIRouter(prefix="/api/comparison", tags=["Comparison"])

# Create service instance
comparison_service = ComparisonService()


@router.post(
    "/generate",
    response_model=ComparisonResponse,
    status_code=status.HTTP_200_OK,
   )
def generate_comparison(req: ComparisonRequest) -> ComparisonResponse:

    return comparison_service.compare_invoices(req)