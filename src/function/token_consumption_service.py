from datetime import date
import logging
from typing import Optional

from src.config.db_config import Config as DatabaseConfig
from src.db.db_service import DatabaseService
from src.schemas.schemas import (
    TokenConsumptionListResponse,
    TokenConsumptionOptionsData,
    TokenConsumptionOptionsResponse,
)

logger = logging.getLogger(__name__)


class TokenConsumptionService:

    DEFAULT_PROVIDERS = ("Anthropic", "Gemini", "OpenAI")

    def __init__(self):
        self.db_service = DatabaseService(DatabaseConfig.get_database_config())

    def get_filter_options(self, tenant_id: str) -> TokenConsumptionOptionsResponse:
        try:
            user_query = """
                SELECT DISTINCT "UserId"
                FROM "data"."AiTokenUsage"
                WHERE "TenantId" = %s
                ORDER BY "UserId"
            """
            provider_query = """
                SELECT DISTINCT "Provider"
                FROM "data"."AiTokenUsage"
                WHERE "TenantId" = %s
                ORDER BY "Provider"
            """

            user_rows = self.db_service.execute_query(user_query, (tenant_id,))
            provider_rows = self.db_service.execute_query(provider_query, (tenant_id,))

            user_ids = [row["UserId"] for row in user_rows if row.get("UserId")]
            providers = [row["Provider"] for row in provider_rows if row.get("Provider")]
            providers = sorted(set(list(self.DEFAULT_PROVIDERS) + providers))

            return TokenConsumptionOptionsResponse(
                success=True,
                code=200,
                message="Token consumption filter options fetched successfully",
                data=TokenConsumptionOptionsData(
                    UserIds=user_ids,
                    Providers=providers,
                ),
            )
        except Exception as e:
            logger.error("[TOKEN_CONSUMPTION] Failed to fetch filter options: %s", e, exc_info=True)
            return TokenConsumptionOptionsResponse(
                success=False,
                code=500,
                message=f"Failed to fetch token consumption filter options: {str(e)}",
                errors=["INTERNAL_ERROR"],
                data=TokenConsumptionOptionsData(
                    UserIds=[],
                    Providers=list(self.DEFAULT_PROVIDERS),
                ),
            )

    def get_token_consumption(
        self,
        tenant_id: str,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        user_id: Optional[str] = None,
        provider: Optional[str] = None,
    ) -> TokenConsumptionListResponse:
        try:
            if from_date and to_date and from_date > to_date:
                return TokenConsumptionListResponse(
                    success=False,
                    code=400,
                    message="FromDate cannot be greater than ToDate",
                    errors=["INVALID_DATE_RANGE"],
                    data=[],
                )

            selected_user_id = None if not user_id or user_id == "All" else user_id
            selected_provider = None if not provider or provider == "All" else provider

            query = """
                SELECT
                    "UserId",
                    %s AS "Provider",
                    MIN(DATE("CreatedAt"))::text AS "FromDate",
                    MAX(DATE("CreatedAt"))::text AS "ToDate",
                    SUM(COALESCE("PromptTokens", 0)) AS "InputTokens",
                    SUM(
                        COALESCE("CompletionTokens", 0) +
                        COALESCE("ThoughtsTokens", 0) 
                    ) AS "OutputTokens",
                    SUM(COALESCE("TotalTokens", 0)) AS "TotalTokens"
                FROM "data"."AiTokenUsage"
                WHERE "TenantId" = %s
                  AND  DATE("CreatedAt") BETWEEN %s AND %s
                  AND (%s IS NULL OR "UserId" = %s)
                  AND (%s IS NULL OR "Provider" = %s)
                GROUP BY "UserId"
                ORDER BY "UserId"
            """

            rows = self.db_service.execute_query(
                query,
                (
                    selected_provider or "All",
                    tenant_id,
                    from_date,
                    to_date,
                    selected_user_id,
                    selected_user_id,
                    selected_provider,
                    selected_provider,
                ),
            )

            return TokenConsumptionListResponse(
                success=True,
                code=200,
                message="Token consumption data fetched successfully",
                data=rows,
            )
        except Exception as e:
            logger.error("[TOKEN_CONSUMPTION] Failed to fetch summary: %s", e, exc_info=True)
            return TokenConsumptionListResponse(
                success=False,
                code=500,
                message=f"Failed to fetch token consumption data: {str(e)}",
                errors=["INTERNAL_ERROR"],
                data=[],
            )
