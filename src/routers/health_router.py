from fastapi import APIRouter, Request
from datetime import datetime
import time

from src.config.db_config import Config as DatabaseConfig
from src.db.db_service import DatabaseService
from src.config.redis_config import Config as RedisConfig
from src.db.redis_service import RedisService
from src.models.registry import ModelRegistry
from src.config.app_config import Config

router = APIRouter(prefix="/health", tags=["Health"])


@router.get("")
async def health_check():
    return {
        "status": "healthy",
        "service": "Invoice Search API",
        "version": "5.5.0"
    }


@router.get("/database")
async def database_health_check():
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "checks": {}
    }

    # Database Check
    try:
        db_config = DatabaseConfig.get_database_config()
        db_service = DatabaseService(db_config)
        db_service.execute_query("SELECT 1")

        health_status["checks"]["database"] = {
            "status": "up",
            "host": db_config.host,
            "database": db_config.database
        }
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["checks"]["database"] = {
            "status": "down",
            "error": str(e)
        }

    # In-memory chat store check
    try:
        redis_config = RedisConfig.get_redis_config()
        RedisService(redis_config)

        health_status["checks"]["memory"] = {
            "status": "up",
            "mode": "in-memory"
        }

    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["checks"]["memory"] = {
            "status": "down",
            "error": str(e)
        }

    return health_status


@router.get("/models")
async def check_models(request: Request):
    tenant_id = getattr(request.state, 'TenantId', None)

    if not tenant_id:
        return {"error": "No TenantId found in request"}

    registry = ModelRegistry()
    tenant_models = registry.get_all_for_tenant(tenant_id)

    redis = RedisService(RedisConfig.get_redis_config())
    last_activity = redis.get_tenant_last_activity(tenant_id)

    idle_time = None
    if last_activity:
        idle_time = int(time.time() - last_activity)

    return {
        "TenantId": tenant_id,
        "models_loaded": len(tenant_models),
        "last_activity": datetime.fromtimestamp(last_activity).isoformat() if last_activity else None,
        "idle_seconds": idle_time,
        "models": list(tenant_models.keys())
    }
