import logging
from src.db.redis_service import RedisService
from src.db.db_service import DatabaseService

logger = logging.getLogger(__name__)

async def handle_shutdown():
    logger.info("Shutting down application")

    try:
        DatabaseService.close_all_pools()
        logger.info("Closed database connection pools")
    except Exception as e:
        logger.error("Error closing database pools: %s", e)

    try:
        RedisService.close_all_pools()
        logger.info("Closed Redis connection pools")
    except Exception as e:
        logger.error("Error closing Redis pools: %s", e)
