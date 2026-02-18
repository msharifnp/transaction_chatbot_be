# from fastapi import FastAPI, Request
# from fastapi.responses import JSONResponse
# from fastapi.middleware.cors import CORSMiddleware
# from src.config.app_config import Config
# from src.routers.search_router import router as search_router
# from src.routers.export_router import router as export_router
# from src.routers.comparison_router import router as comparison_router
# from src.db.db_service import DatabaseService
# from src.config.startup import model_startup
# from src.config.db_config import Config as DatabaseConfig
# from src.config.redis_config import Config as RedisConfig
# from src.routers.session_router import router as session_router
# from src.routers.health_router import router as health_router
# from src.db.redis_service import RedisService
# import logging
# import sys
# from datetime import datetime
# import asyncio
# from src.models.registry import ModelRegistry
# import time
# from src.routers.fetch_router import router as fetch_router

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='[%(asctime)s] %(levelname)s - %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S'
# )
# logger = logging.getLogger(__name__)

# app = FastAPI(
#     title="Invoice Search API",
#     version="5.5.0",
#     description="AI-powered invoice search and analysis system"
# )

# # CORS Middleware
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=Config.get_allowed_origins(),
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # ==================== CLEANUP CONFIGURATION ====================
# TENANT_IDLE_TIMEOUT = 3600      
# CLEANUP_CHECK_INTERVAL = 600    
# SESSION_IDLE_TIMEOUT = 3600
# # ================================================================


# async def smart_cleanup_daemon():
#     """
#     Smart cleanup daemon that:
#     1. Removes idle sessions (per user)
#     2. Unloads models when tenant is idle
#     """
#     await asyncio.sleep(30)
#     logger.info("🧹 Smart cleanup daemon started")
#     logger.info(f"   Tenant idle timeout: {TENANT_IDLE_TIMEOUT}s")
#     logger.info(f"   Session idle timeout: {SESSION_IDLE_TIMEOUT}s")
#     logger.info(f"   Check interval: {CLEANUP_CHECK_INTERVAL}s")

#     while True:
#         try:
#             redis = RedisService(RedisConfig.get_redis_config())
#             registry = ModelRegistry()
#             current_time = time.time()

#             tenants_with_models = registry.get_all_tenants()

#             if not tenants_with_models:
#                 logger.debug("No tenants with loaded models")
#                 await asyncio.sleep(CLEANUP_CHECK_INTERVAL)
#                 continue

#             for tenant_id in tenants_with_models:

#                 # =====================================================
#                 # 🧹 1. SESSION LEVEL CLEANUP (dead users)
#                 # =====================================================
#                 sessions = redis.get_session_keys(tenant_id)

#                 for session_id in sessions:
#                     last_session_activity = redis.get_session_last_activity(
#                         tenant_id, session_id
#                     )

#                     if not last_session_activity:
#                         continue

#                     session_idle = current_time - last_session_activity

#                     if session_idle > SESSION_IDLE_TIMEOUT:
#                         logger.info(
#                             f"🧹 Removing idle session {session_id} "
#                             f"for tenant {tenant_id} "
#                             f"(idle: {int(session_idle)}s)"
#                         )
#                         redis.delete_session(tenant_id, session_id)

#                 # =====================================================
#                 # 🔥 2. TENANT LEVEL CLEANUP (models)
#                 # =====================================================
#                 last_activity = redis.get_tenant_last_activity(tenant_id)

#                 if not last_activity:
#                     continue

#                 idle_time = current_time - last_activity

#                 if idle_time > TENANT_IDLE_TIMEOUT:
#                     logger.info(
#                         f"🧹 Unloading tenant: {tenant_id} "
#                         f"(idle: {int(idle_time)}s / {TENANT_IDLE_TIMEOUT}s)"
#                     )

#                     # Delete all Redis data
#                     deleted_keys = redis.delete_all_tenant_data(tenant_id)
#                     logger.info(f"   ├─ Deleted {deleted_keys} Redis keys")

#                     # Unload models
#                     registry.unload_tenant_models(tenant_id)
#                     logger.info(f"   ├─ Unloaded models from registry")

#                     # Cleanup service
#                     model_startup.cleanup_tenant_service(tenant_id)
#                     logger.info(f"   └─ Cleaned up model service")

#                     logger.info(f"✅ Complete cleanup for tenant: {tenant_id}")

#                 else:
#                     remaining = TENANT_IDLE_TIMEOUT - idle_time
#                     logger.debug(
#                         f"✓ Tenant {tenant_id} active "
#                         f"(idle: {int(idle_time)}s, unload in: {int(remaining)}s)"
#                     )

#         except Exception as e:
#             logger.error(f"[CLEANUP] Error in cleanup daemon: {e}", exc_info=True)

#         await asyncio.sleep(CLEANUP_CHECK_INTERVAL)




# @app.on_event("startup")
# async def startup_event():
#     """Initialize application on startup."""
#     logger.info("=" * 60)
#     logger.info("🚀 Starting Invoice Search API v5.5.0")
#     logger.info("=" * 60)
    
#     # Verify Database Connection
#     try:
#         logger.info("📊 Connecting to Oracle Database...")
#         db_config = DatabaseConfig.get_database_config()
#         db_service = DatabaseService(db_config)
        
#         result = db_service.execute_query("SELECT BANNER FROM v$version WHERE ROWNUM = 1")
#         oracle_version = result[0]['BANNER'] if result else "Unknown"
        
#         logger.info(f"✅ Database connected: {db_config.host}:{db_config.port}/{db_config.database}")
#         logger.info(f"   Oracle version: {oracle_version[:80]}...")
#     except Exception as e:
#         logger.error(f"❌ Database connection failed: {e}")
#         logger.error("   Application will exit - fix database configuration")
#         sys.exit(1)
    
#     # Verify Redis Connection
#     try:
#         logger.info("🔴 Connecting to Redis...")
#         redis_config = RedisConfig.get_redis_config()
#         redis_service = RedisService(redis_config)
        
#         info = redis_service.redis_client.info()
#         redis_version = info.get('redis_version', 'Unknown')

#         logger.info(f"✅ Redis connected: {redis_config.host}:{redis_config.port}/db{redis_config.db}")
#         logger.info(f"   Redis version: {redis_version}")
                    
#     except Exception as e:
#         logger.error(f"❌ Redis connection failed: {e}")
#         logger.error("   Application will exit - fix Redis configuration")
#         sys.exit(1)
    
#     # Initialize Model Loader
#     try:
#         logger.info("🤖 Initializing AI model loader...")
#         model_startup.initialize(db_service)
#         logger.info("✅ Model loader initialized")
#         logger.info("   Models will be loaded on-demand when API is called")
        
#     except Exception as e:
#         logger.error(f"❌ Model loader initialization failed: {e}")
#         logger.error("   Application will exit - fix model configuration")
#         sys.exit(1)
    
#     logger.info("=" * 60)
#     logger.info("✅ Application startup complete")
#     logger.info("=" * 60)
#     logger.info(f"📡 API available at: http://{Config.HOST}:{Config.PORT}")
#     logger.info(f"📚 API docs at: http://{Config.HOST}:{Config.PORT}/docs")
#     logger.info("=" * 60)
    
#     # Start smart cleanup daemon
#     asyncio.create_task(smart_cleanup_daemon())
#     logger.info(f"🧹 Model cleanup daemon started:")
#     logger.info(f"   - Tenant idle timeout: {TENANT_IDLE_TIMEOUT}s ({TENANT_IDLE_TIMEOUT/60:.1f} minutes)")
#     logger.info(f"   - Cleanup check interval: {CLEANUP_CHECK_INTERVAL}s")
#     logger.info("=" * 60)


# @app.middleware("http")
# async def tenant_middleware(request: Request, call_next):
#     """
#     Middleware to extract TenantId and track tenant activity.
#     This tracks ALL API calls (search, export, comparison, sessions).
#     """
#     path = request.url.path

#     # Skip FastAPI internal routes and CORS preflight
#     if path.startswith("/docs") or \
#        path.startswith("/openapi") or \
#        path.startswith("/favicon") or \
#        path == "/" or \
#        path.startswith("/health") or \
#        request.method == "OPTIONS":
#         return await call_next(request)

#     tenant_id = request.headers.get("TenantId")
    
#     if not tenant_id:
#         return JSONResponse(
#             status_code=400,
#             content={
#                 "error": "MISSING_TENANT_ID",
#                 "message": "TenantId header is required"
#             },
#         )

#     request.state.TenantId = tenant_id
    
#     # session_id = request.headers.get("SessionId")
#     # if session_id:
#     #     request.state.SessionId = session_id
    
    
#     session_id = request.headers.get("SessionId")
#     request.state.SessionId = session_id  # always set, even if None

    
#     # ⭐ CRITICAL: Update tenant activity on EVERY API call
#     # This includes: /api/search, /api/export, /api/comparison, /api/session/*
#     redis_service = RedisService(RedisConfig.get_redis_config())
#     redis_service.update_tenant_activity(tenant_id)
    
#     logger.debug(f"API call: {path} | Tenant: {tenant_id} | Session: {session_id or 'N/A'}")
    
#     return await call_next(request)


# @app.on_event("shutdown")
# async def shutdown_event():
#     """Clean up resources on application shutdown."""
#     logger.info("=" * 60)
#     logger.info("🛑 Shutting down Invoice Search API")
#     logger.info("=" * 60)
    
#     try:
#         # Close Redis connection pools
#         RedisService.close_all_pools()
#         logger.info("✅ Closed Redis connection pools")
#     except Exception as e:
#         logger.error(f"⚠️  Error closing Redis pools: {e}")
    
#     logger.info("=" * 60)
#     logger.info("✅ Shutdown complete")
#     logger.info("=" * 60)


# # Include routers
# app.include_router(session_router)
# app.include_router(search_router)
# app.include_router(export_router)
# app.include_router(comparison_router)
# app.include_router(health_router)
# app.include_router(fetch_router)


# if __name__ == "__main__":
#     import uvicorn

#     uvicorn.run(
#         "main:app",
#         host=Config.HOST,
#         port=Config.PORT,
#         reload=Config.RELOAD,
#         log_level="info"
#     )








from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from src.config.app_config import Config
from src.routers.search_router import router as search_router
from src.routers.export_router import router as export_router
from src.db.db_service import DatabaseService
from src.config.startup import model_startup
from src.config.db_config import Config as DatabaseConfig
from src.config.redis_config import Config as RedisConfig
from src.routers.session_router import router as session_router
from src.routers.health_router import router as health_router
from src.db.redis_service import RedisService
import logging
import sys
from datetime import datetime
import asyncio
from src.models.registry import ModelRegistry
import time


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Invoice Search API",
    version="5.5.0",
    description="AI-powered invoice search and analysis system"
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== CLEANUP CONFIGURATION ====================
TENANT_IDLE_TIMEOUT = 84000      
CLEANUP_CHECK_INTERVAL = 84000   
SESSION_IDLE_TIMEOUT = 84000
# ================================================================


async def smart_cleanup_daemon():
    """
    Smart cleanup daemon that:
    1. Removes idle sessions (per user)
    2. Unloads models when tenant is idle
    """
    await asyncio.sleep(30)
    logger.info("🧹 Smart cleanup daemon started")
    logger.info(f"   Tenant idle timeout: {TENANT_IDLE_TIMEOUT}s")
    logger.info(f"   Session idle timeout: {SESSION_IDLE_TIMEOUT}s")
    logger.info(f"   Check interval: {CLEANUP_CHECK_INTERVAL}s")

    while True:
        try:
            redis = RedisService(RedisConfig.get_redis_config())
            registry = ModelRegistry()
            current_time = time.time()

            tenants_with_models = registry.get_all_tenants()

            if not tenants_with_models:
                logger.debug("No tenants with loaded models")
                await asyncio.sleep(CLEANUP_CHECK_INTERVAL)
                continue

            for tenant_id in tenants_with_models:

                # =====================================================
                # 🧹 1. SESSION LEVEL CLEANUP (dead users)
                # =====================================================
                sessions = redis.get_session_keys(tenant_id)

                for session_id in sessions:
                    last_session_activity = redis.get_session_last_activity(
                        tenant_id, session_id
                    )

                    if not last_session_activity:
                        continue

                    session_idle = current_time - last_session_activity

                    if session_idle > SESSION_IDLE_TIMEOUT:
                        logger.info(
                            f"🧹 Removing idle session {session_id} "
                            f"for tenant {tenant_id} "
                            f"(idle: {int(session_idle)}s)"
                        )
                        redis.delete_session(tenant_id, session_id)

                # =====================================================
                # 🔥 2. TENANT LEVEL CLEANUP (models)
                # =====================================================
                last_activity = redis.get_tenant_last_activity(tenant_id)

                if not last_activity:
                    continue

                idle_time = current_time - last_activity

                if idle_time > TENANT_IDLE_TIMEOUT:
                    logger.info(
                        f"🧹 Unloading tenant: {tenant_id} "
                        f"(idle: {int(idle_time)}s / {TENANT_IDLE_TIMEOUT}s)"
                    )

                    # Delete all Redis data
                    deleted_keys = redis.delete_all_tenant_data(tenant_id)
                    logger.info(f"   ├─ Deleted {deleted_keys} Redis keys")

                    # Unload models
                    registry.unload_tenant_models(tenant_id)
                    logger.info(f"   ├─ Unloaded models from registry")

                    # Cleanup service
                    model_startup.cleanup_tenant_service(tenant_id)
                    logger.info(f"   └─ Cleaned up model service")

                    logger.info(f"✅ Complete cleanup for tenant: {tenant_id}")

                else:
                    remaining = TENANT_IDLE_TIMEOUT - idle_time
                    logger.debug(
                        f"✓ Tenant {tenant_id} active "
                        f"(idle: {int(idle_time)}s, unload in: {int(remaining)}s)"
                    )

        except Exception as e:
            logger.error(f"[CLEANUP] Error in cleanup daemon: {e}", exc_info=True)

        await asyncio.sleep(CLEANUP_CHECK_INTERVAL)




@app.on_event("startup")
async def startup_event():
    """Initialize application on startup."""
    logger.info("=" * 60)
    logger.info("🚀 Starting Invoice Search API v5.5.0")
    logger.info("=" * 60)
    
    # Verify Database Connection
    try:
        logger.info("📊 Connecting to Oracle Database...")
        db_config = DatabaseConfig.get_database_config()
        db_service = DatabaseService(db_config)
        
        result = db_service.execute_query("SELECT BANNER FROM v$version WHERE ROWNUM = 1")
        oracle_version = result[0]['BANNER'] if result else "Unknown"
        
        logger.info(f"✅ Database connected: {db_config.host}:{db_config.port}/{db_config.database}")
        logger.info(f"   Oracle version: {oracle_version[:80]}...")
        
        # Log pool statistics
        pool_stats = DatabaseService.get_pool_stats()
        if pool_stats:
            logger.info(f"   Pool: min={pool_stats['min']}, max={pool_stats['max']}, open={pool_stats['opened']}, busy={pool_stats['busy']}")
        
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        logger.error("   Application will exit - fix database configuration")
        sys.exit(1)
    
    # Verify in-memory chat store
    try:
        logger.info("Initializing in-memory chat store...")
        redis_config = RedisConfig.get_redis_config()
        redis_service = RedisService(redis_config)
        
        info = redis_service.redis_client.info()
        store_mode = info.get('mode', info.get('redis_version', 'in-memory'))

        logger.info(f"In-memory store ready: {store_mode}")
                    
    except Exception as e:
        logger.error(f"Failed to initialize in-memory store: {e}")
        sys.exit(1)
    
    # Initialize Model Loader
    try:
        logger.info("🤖 Initializing AI model loader...")
        model_startup.initialize(db_service)
        logger.info("✅ Model loader initialized")
        logger.info("   Models will be loaded on-demand when API is called")
        
    except Exception as e:
        logger.error(f"❌ Model loader initialization failed: {e}")
        logger.error("   Application will exit - fix model configuration")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("✅ Application startup complete")
    logger.info("=" * 60)
    logger.info(f"📡 API available at: http://{Config.HOST}:{Config.PORT}")
    logger.info(f"📚 API docs at: http://{Config.HOST}:{Config.PORT}/docs")
    logger.info("=" * 60)
    
    # Start smart cleanup daemon
    asyncio.create_task(smart_cleanup_daemon())
    logger.info(f"🧹 Model cleanup daemon started:")
    logger.info(f"   - Tenant idle timeout: {TENANT_IDLE_TIMEOUT}s ({TENANT_IDLE_TIMEOUT/60:.1f} minutes)")
    logger.info(f"   - Cleanup check interval: {CLEANUP_CHECK_INTERVAL}s")
    logger.info("=" * 60)


@app.middleware("http")
async def tenant_middleware(request: Request, call_next):
    """
    Middleware to extract TenantId and track tenant activity.
    This tracks ALL API calls (search, export, comparison, sessions).
    """
    path = request.url.path

    # Skip FastAPI internal routes and CORS preflight
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
    
    # session_id = request.headers.get("SessionId")
    # if session_id:
    #     request.state.SessionId = session_id
    
    
    session_id = request.headers.get("SessionId")
    request.state.SessionId = session_id  # always set, even if None

    
    # ⭐ CRITICAL: Update tenant activity on EVERY API call
    # This includes: /api/search, /api/export, /api/comparison, /api/session/*
    redis_service = RedisService(RedisConfig.get_redis_config())
    redis_service.update_tenant_activity(tenant_id)
    
    logger.debug(f"API call: {path} | Tenant: {tenant_id} | Session: {session_id or 'N/A'}")
    
    return await call_next(request)


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on application shutdown."""
    logger.info("=" * 60)
    logger.info("🛑 Shutting down Invoice Search API")
    logger.info("=" * 60)
    
    try:
        # Close database connection pool
        DatabaseService.close_pool()
        logger.info("✅ Closed database connection pool")
    except Exception as e:
        logger.error(f"⚠️  Error closing database pool: {e}")
    
    try:
        # No-op in in-memory mode (kept for compatibility)
        RedisService.close_all_pools()
        logger.info("In-memory store shutdown complete")
    except Exception as e:
        logger.error(f"⚠️  Error shutting down in-memory store: {e}")
    
    logger.info("=" * 60)
    logger.info("✅ Shutdown complete")
    logger.info("=" * 60)


# Include routers
app.include_router(session_router)
app.include_router(search_router)
app.include_router(export_router)
app.include_router(health_router)



if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=Config.HOST,
        port=Config.PORT,
        reload=Config.RELOAD,
        log_level="info"
    )
