import logging

from fastapi import Request
from fastapi.responses import JSONResponse

from src.config.redis_config import Config as RedisConfig
from src.db.redis_service import RedisService
from src.function.session_service import bootstrap_session
from src.utils.jwt_utils import JWTDecodeError, decode_jwt_payload, extract_bearer_token

logger = logging.getLogger(__name__)


async def handle_tenant_middleware(request: Request, call_next):
    path = request.url.path

    if (
        path.startswith("/docs")
        or path.startswith("/openapi")
        or path.startswith("/favicon")
        or path == "/"
        or path.startswith("/health")
        or request.method == "OPTIONS"
    ):
        return await call_next(request)

    try:
        token = extract_bearer_token(request.headers.get("Authorization"))
        claims = decode_jwt_payload(token)
    except JWTDecodeError as exc:
        return JSONResponse(
            status_code=401,
            content={
                "error": "INVALID_AUTHORIZATION",
                "message": str(exc),
            },
        )

    tenant_id = claims.get("TenantId")
    user_id = claims.get("UserId")

    if not tenant_id or not user_id:
        return JSONResponse(
            status_code=400,
            content={
                "error": "MISSING_JWT_CLAIMS",
                "message": "TenantId and UserId are required in the JWT payload",
            },
        )

    request.state.TenantId = tenant_id
    request.state.UserId = user_id

    redis_service = RedisService(RedisConfig.get_redis_config())
    redis_service.update_tenant_activity(tenant_id)

    if path == "/api/session/start":
        request.state.SessionId = None
        request.state.SessionCreated = False
        return await call_next(request)

    session_id = request.headers.get("SessionId")
    session_created = False

    if session_id:
        messages_key = redis_service.get_messages_key(tenant_id, user_id, session_id)
        if redis_service.redis_client.exists(messages_key):
            logger.info(
                "[MIDDLEWARE] Session validated in Redis: tenant=%s user=%s session=%s",
                tenant_id,
                user_id,
                session_id,
            )
        else:
            logger.warning(
                "[MIDDLEWARE] Session expired in Redis: tenant=%s user=%s session=%s",
                tenant_id,
                user_id,
                session_id,
            )
            session_id = bootstrap_session(tenant_id, user_id)
            session_created = True
            logger.info("[MIDDLEWARE] Created new SessionId: %s", session_id)
    else:
        logger.info(
            "[MIDDLEWARE] No SessionId provided - creating new for tenant=%s user=%s",
            tenant_id,
            user_id,
        )
        session_id = bootstrap_session(tenant_id, user_id)
        session_created = True
        logger.info("[MIDDLEWARE] Created new SessionId: %s", session_id)

    request.state.SessionId = session_id
    request.state.SessionCreated = session_created

    logger.debug(
        "API call: %s | Tenant: %s | User: %s | Session: %s",
        path,
        tenant_id,
        user_id,
        session_id,
    )

    return await call_next(request)
