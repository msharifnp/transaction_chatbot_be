from fastapi import APIRouter, Request, HTTPException, Header
from src.config.startup import model_startup
from src.models.registry import ModelRegistry
from src.db.redis_service import RedisService
from src.config.redis_config import Config as RedisConfig
from datetime import datetime
import secrets
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/session", tags=["Session"])


def generate_session_id() -> str:
    """Generate cryptographically secure session ID"""
    random_part = secrets.token_urlsafe(12)
    timestamp = int(datetime.now().timestamp() * 1000)
    return f"ivp-{random_part}-{timestamp}"


def bootstrap_session(request: Request) -> str:
    tenant_id = request.state.TenantId

    # redis_service = RedisService(RedisConfig.get_redis_config())
    session_id = generate_session_id()

    # redis_service.incr_active_sessions(tenant_id)
    model_startup.get_or_create_service(tenant_id)

    return session_id



@router.post("/start")
async def start_session(request: Request):
   
    try:
        tenant_id = request.state.TenantId
        session_id = generate_session_id()
        
        logger.info(f"[SESSION START] Tenant: {tenant_id}, SessionId: {session_id}")
        
        # ✅ 1. Increment active session count
        redis_service = RedisService(RedisConfig.get_redis_config())
        before_count = redis_service.get_active_sessions(tenant_id)
        logger.info(f"[DEBUG START] BEFORE increment: {before_count}")
        active_count = redis_service.incr_active_sessions(tenant_id)
        logger.info(f"[SESSION] Active sessions for tenant {tenant_id}: {active_count}")
        
        # ✅ 2. Load/get models (cached if already loaded)
        model_service = model_startup.get_or_create_service(tenant_id)
        
        # ✅ 3. Get model info
        registry = ModelRegistry()
        tenant_models = registry.get_all_for_tenant(tenant_id)
        
        models_info = {
            purpose: {
                "provider": provider.config.provider,
                "model": provider.config.model_name,
                "available": provider.is_available()
            }
            for purpose, provider in tenant_models.items()
        }
        
        logger.info(
            f"[SESSION] ✅ Started - SessionId: {session_id}, "
            f"Active sessions: {active_count}, Models: {len(models_info)}"
        )
        
        return {
            "success": True,
            "TenantId": tenant_id,
            "SessionId": session_id,
            "active_sessions": active_count,
            "models": models_info,
            "message": f"Session started successfully ({active_count} active sessions)"
        }
        
    except Exception as e:
        logger.error(f"[SESSION START] Failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



@router.post("/end")
async def end_session(request: Request):
    """
    End a user's chat session.
    Deletes that session's in-memory data.
    Model lifecycle is handled separately by tenant idle cleanup.
    """
    tenant_id = request.state.TenantId
    session_id = request.state.SessionId if hasattr(request.state, 'SessionId') else None
    logger.info(f"[SESSION END] Tenant: {tenant_id}, SessionId: {session_id}")

    try:
        redis_service = RedisService(RedisConfig.get_redis_config())

        deleted = redis_service.delete_session(tenant_id, session_id)

        logger.info(
            f"[SESSION] ✅ Deleted {deleted} in-memory keys for session {session_id}"
        )

        return {
            "success": True,
            "TenantId": tenant_id,
            "SessionId": session_id,
            "message": "Session ended and data removed from memory"
        }

    except Exception as e:
        logger.error(f"[SESSION END] Failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

