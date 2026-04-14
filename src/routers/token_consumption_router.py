from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from src.function.token_consumption_service import TokenConsumptionService
from src.schemas.schemas import (
    TokenConsumptionListResponse,
    TokenConsumptionOptionsResponse,
)

router = APIRouter(prefix="/api/token-consumption", tags=["Token Consumption"])

token_consumption_service = TokenConsumptionService()


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


@router.get("/options", response_model=TokenConsumptionOptionsResponse)
async def get_token_consumption_options(request: Request):
    response = token_consumption_service.get_filter_options(request.state.TenantId)
    _raise_if_failed(response)
    return response


@router.get("/summary", response_model=TokenConsumptionListResponse)
async def get_token_consumption_summary(
    request: Request,
    from_date: Optional[date] = Query(None, alias="FromDate"),
    to_date: Optional[date] = Query(None, alias="ToDate"),
    user_id: Optional[str] = Query(None, alias="UserId"),
    provider: Optional[str] = Query(None, alias="Provider"),
):
    response = token_consumption_service.get_token_consumption(
        tenant_id=request.state.TenantId,
        from_date=from_date,
        to_date=to_date,
        user_id=user_id,
        provider=provider,
    )
    _raise_if_failed(response)
    return response
