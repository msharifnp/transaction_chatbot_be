from fastapi import APIRouter, Request
from src.function.health_service import get_basic_health, get_database_health, get_model_health

router = APIRouter(prefix="/health", tags=["Health"])


@router.get("")
async def health_check():
    return get_basic_health()


@router.get("/database")
async def database_health_check():
    return get_database_health()


@router.get("/models")
async def model_health_check(request: Request):
    tenant_id = getattr(request.state, "TenantId", None)
    if not tenant_id:
        return {"error": "No TenantId found in request"}
    return get_model_health(tenant_id)
