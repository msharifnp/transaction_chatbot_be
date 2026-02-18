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
from src.routers.session_router import bootstrap_session
from src.db.redis_service import RedisService
from src.config.redis_config import Config as RedisConfig
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["Search"])


@router.post(
    "/search",
    response_model=Union[
        DatabaseResponseWrapper, 
        MessageResponseWrapper, 
        ChatResponseWrapper, 
        HybridResponseWrapper
    ],
    responses={
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    },
    status_code=status.HTTP_200_OK,
)

def unified_search_endpoint(
    req: UnifiedSearchRequest,
    request: Request,
):
   
    
    tenant_id = request.state.TenantId
    session_id = request.state.SessionId
    
    redis_service = RedisService(RedisConfig.get_redis_config())
    session_created = False
    
    # ================================================
    # Session Validation & Auto-Creation
    # ================================================
    
    if session_id:
        # ✅ Frontend sent a SessionId - check if it exists in memory
        messages_key = redis_service.get_messages_key(tenant_id, session_id)
        
        if redis_service.redis_client.exists(messages_key):
            # ✅ Session exists in memory - use it
            logger.info(f"[SEARCH] ✅ Session validated in memory: {session_id}")
            session_created = False
        else:
            # ❌ Session expired in memory - create new one
            logger.warning(f"[SEARCH] Session {session_id} expired in memory - creating new")
            session_id = bootstrap_session(request)
            request.state.SessionId = session_id
            session_created = True
            logger.info(f"[SEARCH] ✅ Created new SessionId: {session_id}")
    else:
        # ❌ No SessionId sent - create new one
        logger.info(f"[SEARCH] No SessionId provided - creating new for tenant {tenant_id}")
        session_id = bootstrap_session(request)
        request.state.SessionId = session_id
        session_created = True
        logger.info(f"[SEARCH] ✅ Created new SessionId: {session_id}")
    
    # ================================================
    # Execute Search
    # ================================================
    
    try:
        search_service = SearchService(tenant_id=tenant_id)
        response = search_service.unified_search(req, session_id)
      
        if session_created:
            response.metadata = {
                "new_session_id": session_id
            }
            logger.info(f"[SEARCH] ✅ Session expired - created & returned new session: {session_id}")
        
        return response
        
    except Exception as e:
        logger.error(f"[SEARCH] Router crash (BUG): {e}", exc_info=True)
        raise
 
