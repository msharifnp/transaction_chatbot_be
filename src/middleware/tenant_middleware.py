from fastapi import Request
from fastapi.responses import JSONResponse
from src.db.redis_service import RedisService
from src.config.redis_config import Config as RedisConfig
from src.function.session_service import bootstrap_session
import logging

logger = logging.getLogger(__name__)


async def handle_tenant_middleware(request: Request, call_next):
    path = request.url.path

    if path.startswith("/docs") or \
       path.startswith("/openapi") or \
       path.startswith("/favicon") or \
       path == "/" or \
       path.startswith("/health") or \
       request.method == "OPTIONS":
        return await call_next(request)

    tenant_id = request.headers.get("TenantId")

    if not tenant_id:
        return JSONResponse(
            status_code=400,
            content={
                "error": "MISSING_TENANT_ID",
                "message": "TenantId header is required"
            },
        )

    request.state.TenantId = tenant_id

    redis_service = RedisService(RedisConfig.get_redis_config())
    redis_service.update_tenant_activity(tenant_id)

    session_id = request.headers.get("SessionId")
    session_created = False

    if session_id:
        messages_key = redis_service.get_messages_key(tenant_id, session_id)
        if redis_service.redis_client.exists(messages_key):
            logger.info("[MIDDLEWARE]  Session validated in Redis: %s", session_id)
        else:
            logger.warning("[MIDDLEWARE] Session %s expired in Redis - creating new", session_id)
            session_id = bootstrap_session(tenant_id)
            session_created = True
            logger.info("[MIDDLEWARE]  Created new SessionId: %s", session_id)
    else:
        logger.info("[MIDDLEWARE] No SessionId provided - creating new for tenant %s", tenant_id)
        session_id = bootstrap_session(tenant_id)
        session_created = True
        logger.info("[MIDDLEWARE]  Created new SessionId: %s", session_id)

    request.state.SessionId = session_id
    request.state.SessionCreated = session_created

    logger.debug(
        "API call: %s | Tenant: %s | Session: %s",
        path,
        tenant_id,
        session_id,
    )

    return await call_next(request)