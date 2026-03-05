from src.config.db_config import Config as DatabaseConfig
from src.db.db_service import DatabaseService
from src.config.redis_config import Config as RedisConfig
from src.db.redis_service import RedisService
from src.models.registry import ModelRegistry
from datetime import datetime
import time
import logging

logger = logging.getLogger(__name__)


def get_basic_health() -> dict:
    return {
        "status": "healthy",
        "service": "Invoice Search API",
        "version": "5.5.0"
    }


def get_database_health() -> dict:
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "checks": {}
    }

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
        logger.error("[HEALTH] Database check failed: %s", e)
        health_status["status"] = "unhealthy"
        health_status["checks"]["database"] = {
            "status": "down",
            "error": str(e)
        }

    try:
        redis_config = RedisConfig.get_redis_config()
        RedisService(redis_config)
        health_status["checks"]["redis"] = {
            "status": "up",
            "host": redis_config.host,
            "db": redis_config.db
        }
    except Exception as e:
        logger.error("[HEALTH] Redis check failed: %s", e)
        health_status["status"] = "unhealthy"
        health_status["checks"]["redis"] = {
            "status": "down",
            "error": str(e)
        }

    return health_status


def get_model_health(tenant_id: str) -> dict:
    registry = ModelRegistry()
    tenant_models = registry.get_all_for_tenant(tenant_id)

    redis = RedisService(RedisConfig.get_redis_config())
    last_activity = redis.get_tenant_last_activity(tenant_id)

    idle_time = int(time.time() - last_activity) if last_activity else None

    return {
        "TenantId": tenant_id,
        "models_loaded": len(tenant_models),
        "last_activity": datetime.fromtimestamp(last_activity).isoformat() if last_activity else None,
        "idle_seconds": idle_time,
        "models": list(tenant_models.keys())
    }