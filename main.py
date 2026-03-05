from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from src.config.app_config import Config
from src.routers.search_router import router as search_router
from src.routers.export_router import router as export_router
from src.routers.comparison_router import router as comparison_router
from src.routers.session_router import router as session_router
from src.routers.health_router import router as health_router
import logging
import sys
import asyncio
from src.routers.fetch_router import router as fetch_router
from src.lifecycle.startup import handle_startup
from src.lifecycle.cleanup import smart_cleanup_daemon
from src.lifecycle.shutdown import handle_shutdown
from src.middleware.tenant_middleware import handle_tenant_middleware

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Invoice Search API",
    version="5.5.0",
    description="AI-powered invoice search and analysis system"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    db_service = await handle_startup()
    asyncio.create_task(smart_cleanup_daemon())


@app.on_event("shutdown")
async def shutdown_event():
    await handle_shutdown()

@app.middleware("http")
async def tenant_middleware(request: Request, call_next):
    return await handle_tenant_middleware(request, call_next)


app.include_router(session_router)
app.include_router(search_router)
app.include_router(export_router)
app.include_router(comparison_router)
app.include_router(health_router)
app.include_router(fetch_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=Config.HOST,
        port=Config.PORT,
        reload=Config.RELOAD,
        log_level="info"
    )
    
    