from typing import Any
from src.models.config import ModelConfig
from src.models.factory import ModelProviderFactory
from src.models.registry import ModelRegistry
from src.db.db_service import DatabaseService
import logging
logger = logging.getLogger(__name__)


class ModelLoader:

    def __init__(self, db_connection: DatabaseService):
        self.db = db_connection
        self.registry = ModelRegistry()
    
    def load_tenant_models(self, tenant_id: str):
        
        logger.info(f"\n{'='*70}")
        logger.info(f"[LOADER]  Loading models for tenant: {tenant_id}")
        logger.info(f"{'='*70}")
        
        query = """
            SELECT "Purpose", "Provider", "ModelName", "ApiKey", 
                   "Temperature", "TopP", "TopK", "MaxOutputTokens"
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
                
                logger.info(f"\n[LOADER]  Model #{idx}:")
                logger.info(f"   Purpose: {purpose}")
                logger.info(f"   Provider: {provider}")
                logger.info(f"   Model: {model_name}")
                logger.info(f"   Temperature: {row.get('Temperature')}")
                logger.info(f"   TopP: {row.get('TopP')}")
                logger.info(f"   TopK: {row.get('TopK')}")
                logger.info(f"   MaxTokens: {row.get('MaxOutputTokens')}")
                logger.info(f"   API Key: {row['ApiKey'][:10]}..." if row['ApiKey'] else "  └─ API Key: None")
                
                config = ModelConfig(
                    provider=provider,
                    model_name=model_name,
                    api_key=row['ApiKey'],
                    temperature=row.get('Temperature'),
                    top_p=row.get('TopP'),
                    top_k=row.get('TopK'),
                    max_output_tokens=row.get('MaxOutputTokens')
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