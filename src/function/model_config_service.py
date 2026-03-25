from typing import Optional
import logging
import psycopg2
from psycopg2 import errorcodes
from src.config.db_config import Config as DatabaseConfig
from src.config.startup import model_startup
from src.db.db_service import DatabaseService
from src.db.model_config_lookup import (
    fetch_model_definition_options,
    has_model_definition_table,
    is_valid_model_definition,
)
from src.models.model_service import ModelService
from src.models.registry import ModelRegistry
from src.schemas.schemas import (
    ModelConfigDeleteData,
    ModelConfigDeleteResponse,
    ModelConfigListResponse,
    ModelConfigOptionsData,
    ModelConfigOptionsResponse,
    ModelConfigResponse,
    ModelConfigUpsertRequest,
)

logger = logging.getLogger(__name__)
PURPOSE_OPTIONS = ("Summary", "Technical")


class ModelConfigService:

    def __init__(self):
        self.db_service = DatabaseService(DatabaseConfig.get_database_config())
        
    def _get_config_row(self, tenant_id: str, config_id: int) -> Optional[dict]:
        query = """
            SELECT
                "Id",
                "TenantId",
                "Purpose",
                "Provider",
                "ModelName",
                "ApiKey",
                "Temperature",
                "TopP",
                "TopK",
                "MaxOutputTokens",
                "CreatedAt",
                "UpdatedAt"
            FROM "data"."ModelConfig"
            WHERE "Id" = %s
              AND "TenantId" = %s
        """
        rows = self.db_service.execute_query(query, (config_id, tenant_id))
        return rows[0] if rows else None

    def _refresh_tenant_models(self, tenant_id: str) -> None:
        try:
            registry = ModelRegistry()
            registry.clear_tenant(tenant_id)
            ModelService.invalidate_tenant(tenant_id)
            model_startup.cleanup_tenant_service(tenant_id)

            if model_startup.loader is not None:
                model_startup.get_or_create_service(tenant_id)
        except Exception as e:
            logger.warning(
                "[MODEL CONFIG] Config saved but model cache refresh failed for tenant %s: %s",
                tenant_id,
                e,
            )

    def _handle_database_error(
        self,
        error: psycopg2.Error,
        default_message: str,
    ) -> ModelConfigResponse:
        logger.error("[MODEL CONFIG] Database error: %s", error, exc_info=True)

        if error.pgcode == errorcodes.UNIQUE_VIOLATION:
            return ModelConfigResponse(
                success=False,
                code=409,
                message="A model configuration already exists for this purpose",
                errors=["MODEL_CONFIG_ALREADY_EXISTS"],
                data=None,
            )

        return ModelConfigResponse(
            success=False,
            code=500,
            message=f"{default_message}: {str(error)}",
            errors=["DATABASE_ERROR"],
            data=None,
        )

    def _validate_payload(
        self,
        payload: ModelConfigUpsertRequest,
    ) -> Optional[ModelConfigResponse]:
        if payload.Purpose not in PURPOSE_OPTIONS:
            return ModelConfigResponse(
                success=False,
                code=400,
                message="Invalid purpose selected",
                errors=["INVALID_PURPOSE"],
                data=None,
            )

        if not self._model_definition_available():
            return ModelConfigResponse(
                success=False,
                code=500,
                message="ModelDefinition table is not available",
                errors=["MODEL_DEFINITION_NOT_AVAILABLE"],
                data=None,
            )

        if not is_valid_model_definition(
            self.db_service,
            payload.Provider,
            payload.ModelName,
        ):
            return ModelConfigResponse(
                success=False,
                code=400,
                message="Invalid model selected for the provider",
                errors=["INVALID_MODEL_FOR_PROVIDER"],
                data=None,
            )

        return None

    def _model_definition_available(self) -> bool:
        try:
            return has_model_definition_table(self.db_service)
        except Exception as e:
            logger.warning(
                "[MODEL CONFIG] Could not inspect ModelDefinition; using fallback options: %s",
                e,
            )
            return False


    def list_model_configs(self, tenant_id: str) -> ModelConfigListResponse:
        try:
            query = """
                SELECT
                    "Id",
                    "TenantId",
                    "Purpose",
                    "Provider",
                    "ModelName",
                    "ApiKey",
                    "Temperature",
                    "TopP",
                    "TopK",
                    "MaxOutputTokens",
                    "CreatedAt",
                    "UpdatedAt"
                FROM "data"."ModelConfig"
                WHERE "TenantId" = %s
                ORDER BY "Purpose", "Id"
            """
            rows = self.db_service.execute_query(query, (tenant_id,))
            return ModelConfigListResponse(
                success=True,
                code=200,
                message="Model configurations fetched successfully",
                data=rows,
            )
        except Exception as e:
            logger.error("[MODEL CONFIG] Failed to list configs: %s", e, exc_info=True)
            return ModelConfigListResponse(
                success=False,
                code=500,
                message=f"Failed to fetch model configurations: {str(e)}",
                errors=["INTERNAL_ERROR"],
                data=[],
            )

    def get_model_config_options(self) -> ModelConfigOptionsResponse:
        try:
            if not self._model_definition_available():
                return ModelConfigOptionsResponse(
                    success=False,
                    code=500,
                    message="ModelDefinition table is not available",
                    errors=["MODEL_DEFINITION_NOT_AVAILABLE"],
                    data=ModelConfigOptionsData(
                        purposes=list(PURPOSE_OPTIONS),
                        providers=[],
                        models_by_provider={},
                    ),
                )

            options = fetch_model_definition_options(self.db_service)
            return ModelConfigOptionsResponse(
                success=True,
                code=200,
                message="Model configuration options fetched successfully",
                data=ModelConfigOptionsData(
                    purposes=list(PURPOSE_OPTIONS),
                    providers=options["providers"],
                    models_by_provider=options["models_by_provider"],
                ),
            )
        except Exception as e:
            logger.error("[MODEL CONFIG] Failed to fetch model options: %s", e, exc_info=True)
            return ModelConfigOptionsResponse(
                success=False,
                code=500,
                message=f"Failed to fetch model configuration options: {str(e)}",
                errors=["INTERNAL_ERROR"],
                data=ModelConfigOptionsData(
                    purposes=list(PURPOSE_OPTIONS),
                    providers=[],
                    models_by_provider={},
                ),
            )

    def get_model_config(self, tenant_id: str, config_id: int) -> ModelConfigResponse:
        try:
            row = self._get_config_row(tenant_id, config_id)
            if not row:
                return ModelConfigResponse(
                    success=False,
                    code=404,
                    message="Model configuration not found",
                    errors=["MODEL_CONFIG_NOT_FOUND"],
                    data=None,
                )

            return ModelConfigResponse(
                success=True,
                code=200,
                message="Model configuration fetched successfully",
                data=row,
            )
        except Exception as e:
            logger.error("[MODEL CONFIG] Failed to fetch config: %s", e, exc_info=True)
            return ModelConfigResponse(
                success=False,
                code=500,
                message=f"Failed to fetch model configuration: {str(e)}",
                errors=["INTERNAL_ERROR"],
                data=None,
            )

    def create_model_config(
        self,
        tenant_id: str,
        payload: ModelConfigUpsertRequest,
    ) -> ModelConfigResponse:
        try:
            validation_error = self._validate_payload(payload)
            if validation_error:
                return validation_error

            query = """
                INSERT INTO "data"."ModelConfig" (
                    "TenantId",
                    "Purpose",
                    "Provider",
                    "ModelName",
                    "ApiKey",
                    "Temperature",
                    "TopP",
                    "TopK",
                    "MaxOutputTokens"
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING
                    "Id",
                    "TenantId",
                    "Purpose",
                    "Provider",
                    "ModelName",
                    "ApiKey",
                    "Temperature",
                    "TopP",
                    "TopK",
                    "MaxOutputTokens",
                    "CreatedAt",
                    "UpdatedAt"
            """
            rows = self.db_service.execute_query(
                query,
                (
                    tenant_id,
                    payload.Purpose,
                    payload.Provider,
                    payload.ModelName,
                    payload.ApiKey,
                    payload.Temperature,
                    payload.TopP,
                    payload.TopK,
                    payload.MaxOutputTokens,
                ),
            )

            self._refresh_tenant_models(tenant_id)

            return ModelConfigResponse(
                success=True,
                code=201,
                message="Model configuration created successfully",
                data=rows[0],
            )
        except psycopg2.Error as e:
            return self._handle_database_error(
                e,
                default_message="Failed to create model configuration",
            )
        except Exception as e:
            logger.error("[MODEL CONFIG] Failed to create config: %s", e, exc_info=True)
            return ModelConfigResponse(
                success=False,
                code=500,
                message=f"Failed to create model configuration: {str(e)}",
                errors=["INTERNAL_ERROR"],
                data=None,
            )

    def update_model_config(
        self,
        tenant_id: str,
        config_id: int,
        payload: ModelConfigUpsertRequest,
    ) -> ModelConfigResponse:
        try:
            validation_error = self._validate_payload(payload)
            if validation_error:
                return validation_error

            query = """
                UPDATE "data"."ModelConfig"
                SET
                    "Purpose" = %s,
                    "Provider" = %s,
                    "ModelName" = %s,
                    "ApiKey" = %s,
                    "Temperature" = %s,
                    "TopP" = %s,
                    "TopK" = %s,
                    "MaxOutputTokens" = %s,
                    "UpdatedAt" = CURRENT_TIMESTAMP
                WHERE "Id" = %s
                  AND "TenantId" = %s
                RETURNING
                    "Id",
                    "TenantId",
                    "Purpose",
                    "Provider",
                    "ModelName",
                    "ApiKey",
                    "Temperature",
                    "TopP",
                    "TopK",
                    "MaxOutputTokens",
                    "CreatedAt",
                    "UpdatedAt"
            """
            rows = self.db_service.execute_query(
                query,
                (
                    payload.Purpose,
                    payload.Provider,
                    payload.ModelName,
                    payload.ApiKey,
                    payload.Temperature,
                    payload.TopP,
                    payload.TopK,
                    payload.MaxOutputTokens,
                    config_id,
                    tenant_id,
                ),
            )

            if not rows:
                return ModelConfigResponse(
                    success=False,
                    code=404,
                    message="Model configuration not found",
                    errors=["MODEL_CONFIG_NOT_FOUND"],
                    data=None,
                )

            self._refresh_tenant_models(tenant_id)

            return ModelConfigResponse(
                success=True,
                code=200,
                message="Model configuration updated successfully",
                data=rows[0],
            )
        except psycopg2.Error as e:
            return self._handle_database_error(
                e,
                default_message="Failed to update model configuration",
            )
        except Exception as e:
            logger.error("[MODEL CONFIG] Failed to update config: %s", e, exc_info=True)
            return ModelConfigResponse(
                success=False,
                code=500,
                message=f"Failed to update model configuration: {str(e)}",
                errors=["INTERNAL_ERROR"],
                data=None,
            )

    def delete_model_config(
        self,
        tenant_id: str,
        config_id: int,
    ) -> ModelConfigDeleteResponse:
        try:
            query = """
                DELETE FROM "data"."ModelConfig"
                WHERE "Id" = %s
                  AND "TenantId" = %s
                RETURNING "Id"
            """
            rows = self.db_service.execute_query(query, (config_id, tenant_id))

            if not rows:
                return ModelConfigDeleteResponse(
                    success=False,
                    code=404,
                    message="Model configuration not found",
                    errors=["MODEL_CONFIG_NOT_FOUND"],
                    data=None,
                )

            self._refresh_tenant_models(tenant_id)

            return ModelConfigDeleteResponse(
                success=True,
                code=200,
                message="Model configuration deleted successfully",
                data=ModelConfigDeleteData(Id=rows[0]["Id"]),
            )
        except Exception as e:
            logger.error("[MODEL CONFIG] Failed to delete config: %s", e, exc_info=True)
            return ModelConfigDeleteResponse(
                success=False,
                code=500,
                message=f"Failed to delete model configuration: {str(e)}",
                errors=["INTERNAL_ERROR"],
                data=None,
            )

    