import sys
import logging
from src.db.db_service import DatabaseService
from src.config.db_config import Config as DatabaseConfig
from src.config.redis_config import Config as RedisConfig
from src.db.redis_service import RedisService
from src.config.startup import model_startup

logger = logging.getLogger(__name__)


async def handle_startup():
    logger.info("Starting Invoice Search API")

    try:
        db_config = DatabaseConfig.get_database_config()
        db_service = DatabaseService(db_config)

        result = db_service.execute_query("SELECT version()")
        logger.info("Database connected")
    except Exception as e:
        logger.error("Database connection failed: %s", e)
        sys.exit(1)

    try:
        redis_config = RedisConfig.get_redis_config()
        redis_service = RedisService(redis_config)
        redis_service.redis_client.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.error("Redis connection failed: %s", e)
        sys.exit(1)

    try:
        model_startup.initialize(db_service)
        logger.info("Model loader initialized")
    except Exception as e:
        logger.error("Model loader failed: %s", e)
        sys.exit(1)

    return db_service