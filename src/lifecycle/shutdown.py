import logging
from src.db.redis_service import RedisService

logger = logging.getLogger(__name__)

async def handle_shutdown():
    logger.info("Shutting down application")

    try:
        RedisService.close_all_pools()
        logger.info("Closed Redis connection pools")
    except Exception as e:
        logger.error("Error closing Redis pools: %s", e)