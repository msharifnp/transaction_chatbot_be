from fastapi import APIRouter, HTTPException, Request, status

from src.function.model_config_service import ModelConfigService
from src.schemas.schemas import (
    ModelConfigDeleteResponse,
    ModelConfigListResponse,
    ModelConfigOptionsResponse,
    ModelConfigResponse,
    ModelConfigUpsertRequest,
)

router = APIRouter(prefix="/api/model-config", tags=["Model Config"])

model_config_service = ModelConfigService()


def _raise_if_failed(response):
    if response.success:
        return

    raise HTTPException(
        status_code=response.code,
        detail={
            "success": response.success,
            "message": response.message,
            "errors": response.errors,
        },
    )


@router.get("/tenantmodels", response_model=ModelConfigListResponse)
async def list_model_configs(request: Request):
    response = model_config_service.list_model_configs(request.state.TenantId)
    _raise_if_failed(response)
    return response


@router.get("/dropdown", response_model=ModelConfigOptionsResponse)
async def get_model_config_options():
    return model_config_service.get_model_config_options()


@router.get("/get/{config_id}", response_model=ModelConfigResponse)
async def get_model_config(config_id: int, request: Request):
    response = model_config_service.get_model_config(request.state.TenantId, config_id)
    _raise_if_failed(response)
    return response


@router.post(
    "/create",
    response_model=ModelConfigResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_model_config(payload: ModelConfigUpsertRequest, request: Request):
    response = model_config_service.create_model_config(request.state.TenantId, payload)
    _raise_if_failed(response)
    return response


@router.put("/update/{config_id}", response_model=ModelConfigResponse)
async def update_model_config(
    config_id: int,
    payload: ModelConfigUpsertRequest,
    request: Request,
):
    response = model_config_service.update_model_config(
        request.state.TenantId,
        config_id,
        payload,
    )
    _raise_if_failed(response)
    return response


@router.delete("/delete/{config_id}", response_model=ModelConfigDeleteResponse)
async def delete_model_config(config_id: int, request: Request):
    response = model_config_service.delete_model_config(request.state.TenantId, config_id)
    _raise_if_failed(response)
    return response
