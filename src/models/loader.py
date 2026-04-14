from src.models.config import ModelConfig
from src.models.factory import ModelProviderFactory
from src.models.registry import ModelRegistry
from src.db.db_service import DatabaseService
from src.function.azure_key_vault import AzureKeyVaultService
import json
from pathlib import Path
import logging
logger = logging.getLogger(__name__)


class ModelLoader:

    def __init__(self, db_connection: DatabaseService):
        self.db = db_connection
        self.registry = ModelRegistry()
        self.key_vault = AzureKeyVaultService()

    def _resolve_credentials_value(
        self,
        tenant_id: str,
        purpose: str,
        credentials_ref: str,
    ) -> tuple[str, str]:
        secret_ref = (credentials_ref or "").strip()

        if not secret_ref:
            return "", "missing"

        if self.key_vault.is_managed_secret_ref(tenant_id, purpose, secret_ref):
            secret_value = self.key_vault.get_secret_value(secret_ref)
            if not secret_value:
                raise ValueError(f"Azure Key Vault secret is empty: {secret_ref}")
            return secret_value, "key_vault"

        secret_path = Path(secret_ref)
        if secret_path.exists() and secret_path.is_file():
            secret_value = secret_path.read_text(encoding="utf-8").strip()
            if not secret_value:
                raise ValueError(f"Credentials file is empty: {secret_path}")
            return secret_value, "file"

        return secret_ref, "inline"
    
    def load_tenant_models(self, tenant_id: str):
        
        logger.info(f"\n{'='*70}")
        logger.info(f"[LOADER]  Loading models for tenant: {tenant_id}")
        logger.info(f"{'='*70}")
        
        query = """
            SELECT "Purpose", "Provider", "ModelName", "CredentialsRef", "Config"
            FROM "data"."ModelConfig"
            WHERE "TenantId" = %s
        """
        
        rows = self.db.execute_query(query, (tenant_id,))
        
        logger.info(f"[LOADER] Found {len(rows)} model configurations in database")
        
        loaded_count = 0
        for idx, row in enumerate(rows, 1):
            try:
                purpose = row['Purpose']
                provider = row['Provider']
                model_name = row['ModelName']
                raw_config = row.get('Config') or {}

                if isinstance(raw_config, str):
                    try:
                        raw_config = json.loads(raw_config)
                    except json.JSONDecodeError:
                        logger.warning("[LOADER] Invalid Config JSON for purpose=%s tenant=%s", purpose, tenant_id)
                        raw_config = {}
                
                logger.info(f"\n[LOADER]  Model #{idx}:")
                logger.info(f"   Purpose: {purpose}")
                logger.info(f"   Provider: {provider}")
                logger.info(f"   Model: {model_name}")
                logger.info(f"   CredentialsRef: {'configured' if row.get('CredentialsRef') else 'missing'}")
                logger.info(f"   Config: {raw_config}")
                credentials_ref = str(row.get("CredentialsRef", ""))
                resolved_secret, secret_source = self._resolve_credentials_value(
                    tenant_id,
                    purpose,
                    credentials_ref,
                )
                logger.info(
                    "   Secret source: %s",
                    secret_source,
                )
                
                config = ModelConfig(
                    provider=provider,
                    model_name=model_name,
                    api_key=resolved_secret,
                    temperature=float(raw_config.get('temperature', 0.0)),
                    top_p=float(raw_config.get('topP', 0.8)),
                    top_k=int(raw_config.get('topK', 20)),
                    max_output_tokens=int(raw_config.get('maxTokens', 10000)),
                    purpose=purpose,
                    credentials_ref=credentials_ref,
                    extra_params={
                        **raw_config,
                        "credentials_ref": credentials_ref,
                    },
                )
                
                logger.info(f"   Creating {provider} provider instance...")
                provider_instance = ModelProviderFactory.create(config)
                
                if provider_instance.is_available():
                    self.registry.register_model(tenant_id, purpose, provider_instance)
                    loaded_count += 1
                    logger.info(f"    Successfully loaded and cached")
                else:
                    logger.warning(f"     Provider not available (initialization failed)")
                    
            except Exception as e:
                logger.error(f"    Failed to load: {e}")
                import traceback
                traceback.print_exc()
        
        logger.info(f"\n{'='*70}")
        logger.info (f"[LOADER]  Successfully loaded {loaded_count}/{len(rows)} models")
        logger.info(f"{'='*70}\n")
        
        return loaded_count
