import asyncio
import time
import logging
from src.db.redis_service import RedisService
from src.config.redis_config import Config as RedisConfig
from src.models.registry import ModelRegistry
from src.config.startup import model_startup

logger = logging.getLogger(__name__)

TENANT_IDLE_TIMEOUT = 600
CLEANUP_CHECK_INTERVAL = 600
SESSION_IDLE_TIMEOUT = 3600


async def smart_cleanup_daemon():
    await asyncio.sleep(30)

    logger.info("Smart cleanup daemon started")

    redis = RedisService(RedisConfig.get_redis_config())
    registry = ModelRegistry()

    while True:
        try:
            current_time = time.time()
            tenants = registry.get_all_tenants()

            for tenant_id in tenants:
                sessions = redis.get_session_keys(tenant_id)
                for session_id in sessions:
                    last = redis.get_session_last_activity(tenant_id, session_id)
                    if last and (current_time - last > SESSION_IDLE_TIMEOUT):
                        redis.delete_session(tenant_id, session_id)
                        logger.info(
                            "Removed idle session %s (tenant=%s)",
                            session_id,
                            tenant_id,
                        )

                last_activity = redis.get_tenant_last_activity(tenant_id)
                if last_activity and (current_time - last_activity > TENANT_IDLE_TIMEOUT):

                    deleted = redis.delete_all_tenant_data(tenant_id)
                    registry.unload_tenant_models(tenant_id)
                    model_startup.cleanup_tenant_service(tenant_id)

                    logger.info(
                        "Unloaded tenant %s (deleted %s keys)",
                        tenant_id,
                        deleted,
                    )

        except Exception:
            logger.exception("Cleanup daemon error")

        await asyncio.sleep(CLEANUP_CHECK_INTERVAL)