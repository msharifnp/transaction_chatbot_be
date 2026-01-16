from fastapi import APIRouter, status
from typing import Union

from src.schemas.schemas import (
    DatabaseResponseWrapper,
    MessageResponseWrapper,
    ChatResponseWrapper,
    HybridResponseWrapper,
    UnifiedSearchRequest,
    ErrorResponse,
)

from src.function.serach_service import SearchService

router = APIRouter(prefix="/api", tags=["Search"])

# ✅ Create ONE service instance (singleton-style)
search_service = SearchService()


@router.post(
    "/search",
    response_model=Union[DatabaseResponseWrapper, MessageResponseWrapper, ChatResponseWrapper, HybridResponseWrapper,],
    responses={400: {"model": ErrorResponse},500: {"model": ErrorResponse},},
    status_code=status.HTTP_200_OK,
)
def unified_search_endpoint(req: UnifiedSearchRequest):
   
    return search_service.unified_search(req)
