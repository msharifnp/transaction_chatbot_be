from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.config.app_config import Config
from src.routers.search_router import router as search_router
from src.routers.export_router import router as export_router
from src.routers.comparison_router import router as comparison_router

app = FastAPI(
    title="Invoice Search API",
    version="5.5.0",
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(search_router)
app.include_router(export_router)
app.include_router(comparison_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=Config.HOST,
        port=Config.PORT,
        reload=Config.RELOAD,
    )
