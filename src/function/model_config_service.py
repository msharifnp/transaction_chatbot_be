import json
from pathlib import Path
from typing import Any, Dict, Optional
import logging
import psycopg2
from psycopg2 import errorcodes
from psycopg2.extras import Json
from src.config.db_config import Config as DatabaseConfig
from src.config.startup import model_startup
from src.db.db_service import DatabaseService
from src.db.model_config_lookup import (
    fetch_model_definition_options,
    has_model_definition_table,
    is_valid_model_definition,
)
from src.function.azure_key_vault import AzureKeyVaultService
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
PURPOSE_OPTIONS = ("Summary", "Technical","Voice")
VOICE_PURPOSE = "Voice"
LEGACY_INLINE_SECRET_PLACEHOLDER = "legacy-inline-secret"
LEGACY_FILE_SECRET_PLACEHOLDER = "legacy-file-secret"


class ModelConfigService:

    def __init__(self):
        self.db_service = DatabaseService(DatabaseConfig.get_database_config())
        self.key_vault = AzureKeyVaultService()
        
    def _get_config_row(self, tenant_id: str, config_id: int) -> Optional[dict]:
        query = """
            SELECT
                "Id",
                "TenantId",
                "Purpose",
                "Provider",
                "ModelName",
                "CredentialsRef",
                "Config",
                "CreatedAt",
                "UpdatedAt"
            FROM "data"."ModelConfig"
            WHERE "Id" = %s
              AND "TenantId" = %s
        """
        rows = self.db_service.execute_query(query, (config_id, tenant_id))
        return self._normalize_db_row(rows[0]) if rows else None

    def _normalize_db_row(self, row: dict) -> dict:
        normalized_row = dict(row)
        config = normalized_row.get("Config")

        if isinstance(config, str):
            try:
                config = json.loads(config)
            except json.JSONDecodeError:
                logger.warning("[MODEL CONFIG] Failed to decode Config JSON for row %s", row.get("Id"))
                config = {}

        normalized_row["Config"] = config or {}
        return normalized_row

    def _normalize_db_rows(self, rows: list[dict]) -> list[dict]:
        return [self._normalize_db_row(row) for row in rows]

    def _serialize_response_row(self, row: dict) -> dict:
        normalized_row = self._normalize_db_row(row)
        normalized_row["CredentialsRef"] = self._safe_credentials_ref_for_response(
            normalized_row["TenantId"],
            normalized_row["Purpose"],
            normalized_row.get("CredentialsRef", ""),
        )
        return normalized_row

    def _serialize_response_rows(self, rows: list[dict]) -> list[dict]:
        return [self._serialize_response_row(row) for row in rows]

    def _coerce_bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _normalize_llm_config(self, raw_config: Dict[str, Any] | None) -> Dict[str, Any]:
        config = raw_config or {}
        normalized_config = {
            "temperature": float(config.get("temperature", 0.0)),
            "topP": float(config.get("topP", 0.8)),
            "topK": int(config.get("topK", 20)),
            "maxTokens": int(config.get("maxTokens", 10000)),
        }

        if not 0 <= normalized_config["temperature"] <= 2:
            raise ValueError("Temperature must be between 0 and 2")
        if not 0 <= normalized_config["topP"] <= 1:
            raise ValueError("Top P must be between 0 and 1")
        if normalized_config["topK"] < 0:
            raise ValueError("Top K must be greater than or equal to 0")
        if normalized_config["maxTokens"] < 1:
            raise ValueError("Max Tokens must be greater than 0")

        return normalized_config

    def _normalize_voice_config(self, raw_config: Dict[str, Any] | None) -> Dict[str, Any]:
        config = raw_config or {}
        alt_language_codes = config.get("alternativeLanguageCodes") or []

        if isinstance(alt_language_codes, str):
            alt_language_codes = [
                code.strip() for code in alt_language_codes.split(",") if code.strip()
            ]

        normalized_config = {
            "languageCode": str(config.get("languageCode") or "en-US").strip(),
            "alternativeLanguageCodes": alt_language_codes,
            "sampleRateHertz": int(config.get("sampleRateHertz", 16000)),
            "encoding": str(config.get("encoding") or "LINEAR16").strip().upper(),
            "enableAutomaticPunctuation": self._coerce_bool(
                config.get("enableAutomaticPunctuation", True)
            ),
            "enableWordTimeOffsets": self._coerce_bool(
                config.get("enableWordTimeOffsets", False)
            ),
            "region": str(config.get("region") or "us").strip().lower(),
        }

        if not normalized_config["languageCode"]:
            raise ValueError("Language code is required for voice configuration")
        if normalized_config["sampleRateHertz"] < 1:
            raise ValueError("Sample rate must be greater than 0")

        normalized_config["alternativeLanguageCodes"] = [
            str(code).strip()
            for code in normalized_config["alternativeLanguageCodes"]
            if str(code).strip()
        ]

        return normalized_config

    def _normalize_credentials_ref(self, credentials_ref: str) -> str:
        return str(credentials_ref or "").strip()

    def _normalize_secret_value(self, secret_value: Optional[str]) -> str:
        return str(secret_value or "").strip()

    def _safe_credentials_ref_for_response(
        self,
        tenant_id: str,
        purpose: str,
        credentials_ref: str,
    ) -> str:
        normalized_credentials_ref = self._normalize_credentials_ref(credentials_ref)
        if not normalized_credentials_ref:
            return ""

        if self.key_vault.is_managed_secret_ref(tenant_id, purpose, normalized_credentials_ref):
            return self.key_vault.extract_secret_name(normalized_credentials_ref)

        credentials_path = Path(normalized_credentials_ref)
        if credentials_path.exists() and credentials_path.is_file():
            return LEGACY_FILE_SECRET_PLACEHOLDER

        return LEGACY_INLINE_SECRET_PLACEHOLDER

    def _extract_submitted_secret_value(
        self,
        payload: ModelConfigUpsertRequest,
        existing_row: Optional[dict] = None,
    ) -> str:
        submitted_secret_value = self._normalize_secret_value(payload.SecretValue)
        if submitted_secret_value:
            return submitted_secret_value

        legacy_value = self._normalize_credentials_ref(payload.CredentialsRef)
        if not legacy_value:
            return ""

        if legacy_value in {LEGACY_INLINE_SECRET_PLACEHOLDER, LEGACY_FILE_SECRET_PLACEHOLDER}:
            return ""

        if existing_row:
            current_ref = self._normalize_credentials_ref(existing_row.get("CredentialsRef", ""))
            safe_current_ref = self._safe_credentials_ref_for_response(
                existing_row.get("TenantId", ""),
                existing_row.get("Purpose", ""),
                current_ref,
            )
            if legacy_value in {current_ref, safe_current_ref}:
                return ""

        return legacy_value

    def _resolve_secret_value_for_storage(
        self,
        tenant_id: str,
        purpose: str,
        credentials_ref: str,
    ) -> str:
        normalized_credentials_ref = self._normalize_credentials_ref(credentials_ref)
        if not normalized_credentials_ref:
            return ""

        if self.key_vault.is_managed_secret_ref(tenant_id, purpose, normalized_credentials_ref):
            return self.key_vault.get_secret_value(normalized_credentials_ref)

        credentials_path = Path(normalized_credentials_ref)
        if credentials_path.exists() and credentials_path.is_file():
            return credentials_path.read_text(encoding="utf-8").strip()

        return normalized_credentials_ref

    def _persist_secret_value(
        self,
        tenant_id: str,
        purpose: str,
        secret_value: str,
    ) -> str:
        self.key_vault.ensure_configured()
        secret_name = self.key_vault.build_secret_name(tenant_id, purpose)
        return self.key_vault.set_secret(secret_name, secret_value)

    def _delete_secret_if_managed(
        self,
        tenant_id: str,
        purpose: str,
        credentials_ref: str,
    ) -> None:
        normalized_credentials_ref = self._normalize_credentials_ref(credentials_ref)
        if not normalized_credentials_ref:
            return

        if not self.key_vault.is_managed_secret_ref(tenant_id, purpose, normalized_credentials_ref):
            return

        try:
            self.key_vault.delete_secret(normalized_credentials_ref)
        except Exception as e:
            logger.warning(
                "[MODEL CONFIG] Failed to delete Azure Key Vault secret for tenant %s purpose %s: %s",
                tenant_id,
                purpose,
                e,
            )

    def _validate_google_voice_credentials(self, credentials_ref: str) -> None:
        normalized_credentials_ref = self._normalize_credentials_ref(credentials_ref)
        raw_credentials = normalized_credentials_ref

        if not normalized_credentials_ref.startswith("{"):
            credentials_path = Path(normalized_credentials_ref)
            if credentials_path.exists() and credentials_path.is_file():
                raw_credentials = credentials_path.read_text(encoding="utf-8").strip()

        try:
            credentials_json = json.loads(raw_credentials)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "Google Cloud voice credentials must be a valid JSON object"
            ) from exc

        if not isinstance(credentials_json, dict):
            raise ValueError("Google Cloud voice credentials must be a JSON object")

    def _prepare_payload(
        self,
        payload: ModelConfigUpsertRequest,
        stored_credentials_ref: str,
        secret_value_for_validation: Optional[str] = None,
    ) -> tuple[Optional[ModelConfigResponse], Optional[Dict[str, Any]], Optional[str]]:
        normalized_credentials_ref = self._normalize_credentials_ref(stored_credentials_ref)
        normalized_secret_value = self._normalize_secret_value(secret_value_for_validation)

        if payload.Purpose not in PURPOSE_OPTIONS:
            return (
                ModelConfigResponse(
                    success=False,
                    code=400,
                    message="Invalid purpose selected",
                    errors=["INVALID_PURPOSE"],
                    data=None,
                ),
                None,
                None,
            )

        if not normalized_credentials_ref and not normalized_secret_value:
            return (
                ModelConfigResponse(
                    success=False,
                    code=400,
                    message="CredentialsRef is required",
                    errors=["MISSING_CREDENTIALS_REF"],
                    data=None,
                ),
                None,
                None,
            )

        try:
            if payload.Purpose == VOICE_PURPOSE:
                if not self._model_definition_available():
                    return (
                        ModelConfigResponse(
                            success=False,
                            code=500,
                            message="ModelDefinition table is not available",
                            errors=["MODEL_DEFINITION_NOT_AVAILABLE"],
                            data=None,
                        ),
                        None,
                        None,
                    )

                if not is_valid_model_definition(
                    self.db_service,
                    payload.Provider,
                    payload.ModelName,
                ):
                    return (
                        ModelConfigResponse(
                            success=False,
                            code=400,
                            message="Invalid model selected for the provider",
                            errors=["INVALID_MODEL_FOR_PROVIDER"],
                            data=None,
                        ),
                        None,
                        None,
                    )

                normalized_voice_config = self._normalize_voice_config(payload.Config)
                if payload.Provider == "Google Cloud":
                    if normalized_secret_value:
                        self._validate_google_voice_credentials(normalized_secret_value)
                    normalized_voice_config["modelIdentifier"] = (
                        str(payload.ModelName).replace("-", "_")
                    )

                return None, normalized_voice_config, normalized_credentials_ref

            if not self._model_definition_available():
                return (
                    ModelConfigResponse(
                        success=False,
                        code=500,
                        message="ModelDefinition table is not available",
                        errors=["MODEL_DEFINITION_NOT_AVAILABLE"],
                        data=None,
                    ),
                    None,
                    None,
                )

            if not is_valid_model_definition(
                self.db_service,
                payload.Provider,
                payload.ModelName,
            ):
                return (
                    ModelConfigResponse(
                        success=False,
                        code=400,
                        message="Invalid model selected for the provider",
                        errors=["INVALID_MODEL_FOR_PROVIDER"],
                        data=None,
                    ),
                    None,
                    None,
                )

            return None, self._normalize_llm_config(payload.Config), normalized_credentials_ref
        except (TypeError, ValueError) as e:
            return (
                ModelConfigResponse(
                    success=False,
                    code=400,
                    message=str(e),
                    errors=["INVALID_MODEL_CONFIG"],
                    data=None,
                ),
                None,
                None,
            )

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

    def _handle_key_vault_error(
        self,
        error: Exception,
        default_message: str,
    ) -> ModelConfigResponse:
        logger.error("[MODEL CONFIG] Key Vault error: %s", error, exc_info=True)
        return ModelConfigResponse(
            success=False,
            code=500,
            message=f"{default_message}: {str(error)}",
            errors=["KEY_VAULT_ERROR"],
            data=None,
        )

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
                    "CredentialsRef",
                    "Config",
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
                data=self._serialize_response_rows(rows),
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
                data=self._serialize_response_row(row),
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
            secret_value = self._extract_submitted_secret_value(payload)
            validation_error, normalized_config, _ = self._prepare_payload(
                payload,
                stored_credentials_ref="pending-key-vault-secret",
                secret_value_for_validation=secret_value,
            )
            if validation_error:
                return validation_error

            if not secret_value:
                return ModelConfigResponse(
                    success=False,
                    code=400,
                    message="Secret value is required",
                    errors=["MISSING_SECRET_VALUE"],
                    data=None,
                )

            normalized_credentials_ref = self._persist_secret_value(
                tenant_id,
                payload.Purpose,
                secret_value,
            )

            query = """
                INSERT INTO "data"."ModelConfig" (
                    "TenantId",
                    "Purpose",
                    "Provider",
                    "ModelName",
                    "CredentialsRef",
                    "Config"
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING
                    "Id",
                    "TenantId",
                    "Purpose",
                    "Provider",
                    "ModelName",
                    "CredentialsRef",
                    "Config",
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
                    normalized_credentials_ref,
                    Json(normalized_config),
                ),
            )

            self._refresh_tenant_models(tenant_id)

            return ModelConfigResponse(
                success=True,
                code=201,
                message="Model configuration created successfully",
                data=self._serialize_response_row(rows[0]),
            )
        except psycopg2.Error as e:
            return self._handle_database_error(
                e,
                default_message="Failed to create model configuration",
            )
        except (RuntimeError, ValueError) as e:
            return self._handle_key_vault_error(
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
            existing_row = self._get_config_row(tenant_id, config_id)
            if not existing_row:
                return ModelConfigResponse(
                    success=False,
                    code=404,
                    message="Model configuration not found",
                    errors=["MODEL_CONFIG_NOT_FOUND"],
                    data=None,
                )

            submitted_secret_value = self._extract_submitted_secret_value(payload, existing_row)
            current_credentials_ref = self._normalize_credentials_ref(
                existing_row.get("CredentialsRef", "")
            )
            normalized_credentials_ref = current_credentials_ref
            target_secret_name = self.key_vault.build_secret_name(tenant_id, payload.Purpose)
            should_persist_secret = bool(submitted_secret_value)

            if not should_persist_secret and (
                current_credentials_ref != target_secret_name
                or not self.key_vault.is_managed_secret_ref(
                    tenant_id,
                    existing_row.get("Purpose", ""),
                    current_credentials_ref,
                )
            ):
                submitted_secret_value = self._resolve_secret_value_for_storage(
                    tenant_id,
                    existing_row.get("Purpose", ""),
                    current_credentials_ref,
                )
                should_persist_secret = bool(submitted_secret_value)

            validation_error, normalized_config, _ = self._prepare_payload(
                payload,
                stored_credentials_ref=(
                    target_secret_name if should_persist_secret else normalized_credentials_ref
                ),
                secret_value_for_validation=submitted_secret_value,
            )
            if validation_error:
                return validation_error

            if should_persist_secret:
                normalized_credentials_ref = self._persist_secret_value(
                    tenant_id,
                    payload.Purpose,
                    submitted_secret_value,
                )

            query = """
                UPDATE "data"."ModelConfig"
                SET
                    "Purpose" = %s,
                    "Provider" = %s,
                    "ModelName" = %s,
                    "CredentialsRef" = %s,
                    "Config" = %s,
                    "UpdatedAt" = CURRENT_TIMESTAMP
                WHERE "Id" = %s
                  AND "TenantId" = %s
                RETURNING
                    "Id",
                    "TenantId",
                    "Purpose",
                    "Provider",
                    "ModelName",
                    "CredentialsRef",
                    "Config",
                    "CreatedAt",
                    "UpdatedAt"
            """
            rows = self.db_service.execute_query(
                query,
                (
                    payload.Purpose,
                    payload.Provider,
                    payload.ModelName,
                    normalized_credentials_ref,
                    Json(normalized_config),
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

            old_secret_ref = self._normalize_credentials_ref(existing_row.get("CredentialsRef", ""))
            if old_secret_ref != normalized_credentials_ref:
                self._delete_secret_if_managed(
                    tenant_id,
                    existing_row.get("Purpose", ""),
                    old_secret_ref,
                )

            self._refresh_tenant_models(tenant_id)

            return ModelConfigResponse(
                success=True,
                code=200,
                message="Model configuration updated successfully",
                data=self._serialize_response_row(rows[0]),
            )
        except psycopg2.Error as e:
            return self._handle_database_error(
                e,
                default_message="Failed to update model configuration",
            )
        except (RuntimeError, ValueError) as e:
            return self._handle_key_vault_error(
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
            existing_row = self._get_config_row(tenant_id, config_id)
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

            if existing_row:
                self._delete_secret_if_managed(
                    tenant_id,
                    existing_row.get("Purpose", ""),
                    existing_row.get("CredentialsRef", ""),
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

    
