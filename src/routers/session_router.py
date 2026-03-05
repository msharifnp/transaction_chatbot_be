from fastapi import APIRouter, Request, HTTPException
from src.function.session_service import start_session, end_session
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/session", tags=["Session"])


@router.post("/start")
async def start_session_endpoint(request: Request):
    try:
        return start_session(tenant_id=request.state.TenantId)
    except Exception as e:
        logger.error("[SESSION START] Failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/end")
async def end_session_endpoint(request: Request):
    try:
        session_id = getattr(request.state, "SessionId", None)
        return end_session(
            tenant_id=request.state.TenantId,
            session_id=session_id
        )
    except Exception as e:
        logger.error("[SESSION END] Failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))