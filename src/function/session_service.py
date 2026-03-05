from src.config.startup import model_startup
from src.models.registry import ModelRegistry
from src.db.redis_service import RedisService
from src.config.redis_config import Config as RedisConfig
from datetime import datetime
import secrets
import logging

logger = logging.getLogger(__name__)


def generate_session_id() -> str:
    random_part = secrets.token_urlsafe(12)
    timestamp = int(datetime.now().timestamp() * 1000)
    return f"ivp-{random_part}-{timestamp}"


def bootstrap_session(tenant_id: str) -> str:
    session_id = generate_session_id()
    model_startup.get_or_create_service(tenant_id)
    return session_id


def start_session(tenant_id: str) -> dict:

    session_id = generate_session_id()
    redis_service = RedisService(RedisConfig.get_redis_config())
    active_count = redis_service.incr_active_sessions(tenant_id)
    model_startup.get_or_create_service(tenant_id)
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
        "[SESSION]  Started - SessionId: %s, Active sessions: %s, Models: %s",
        session_id, active_count, len(models_info)
    )

    return {
        "success": True,
        "TenantId": tenant_id,
        "SessionId": session_id,
        "active_sessions": active_count,
        "models": models_info,
        "message": f"Session started successfully ({active_count} active sessions)"
    }


def end_session(tenant_id: str, session_id: str) -> dict:
   
    redis_service = RedisService(RedisConfig.get_redis_config())
    deleted = redis_service.delete_session(tenant_id, session_id)

    logger.info(
        "[SESSION]  Deleted %s Redis keys for session %s",
        deleted, session_id
    )

    return {
        "success": True,
        "TenantId": tenant_id,
        "SessionId": session_id,
        "message": "Session ended and data removed from Redis"
    }