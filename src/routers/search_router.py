from fastapi import APIRouter, status, Request
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
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["Search"])

@router.post(
    "/search",
    response_model=Union[
        DatabaseResponseWrapper,
        MessageResponseWrapper,
        ChatResponseWrapper,
        HybridResponseWrapper,
    ],
    responses={
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    status_code=status.HTTP_200_OK,
)
def unified_search_endpoint(
    req: UnifiedSearchRequest,
    request: Request,
):
    tenant_id = request.state.TenantId
    session_id = request.state.SessionId
    session_created = request.state.SessionCreated

    try:
        search_service = SearchService(tenant_id=tenant_id)
        response = search_service.unified_search(req, session_id)

        if session_created:
            response.metadata = {"new_session_id": session_id}
            logger.info("[SEARCH] Returned new session in response: %s", session_id)

        return response

    except Exception as e:
        logger.error("[SEARCH] Router crash (BUG): %s", e, exc_info=True)
        raise