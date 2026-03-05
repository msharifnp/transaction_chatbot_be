from typing import Dict, Optional
from src.models import ModelLoader, ModelService
from src.db.db_service import DatabaseService
from src.models.registry import ModelRegistry
import logging
logger = logging.getLogger(__name__)    

class ModelStartup:

    _instance = None
    
    def __new__(cls):

        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.db_service = None
            self.loader = None
            self._tenant_services: Dict[str, ModelService] = {}
            self._initialized = True
    
    def initialize(self, db_service: DatabaseService):

        self.db_service = db_service
        self.loader = ModelLoader(db_service)
        logger.info("[STARTUP] ModelStartup initialized with database")
    
    def get_or_create_service(self, tenant_id: str) -> ModelService:

        if self.loader is None:
            raise RuntimeError(
                "ModelStartup not initialized. Call initialize(db_service) first."
            )
        
        if tenant_id in self._tenant_services:
            logger.info(f"[STARTUP]  Using cached models for tenant: {tenant_id}")
            self._show_cached_models(tenant_id)
            return self._tenant_services[tenant_id]
        
        logger.info(f"[STARTUP]  Initializing models for tenant: {tenant_id}")
        
        self.loader.load_tenant_models(tenant_id)
        
        model_service = ModelService(tenant_id)
        
        self._tenant_services[tenant_id] = model_service
        
        self._show_cached_models(tenant_id)
        
        return model_service
    
    def _show_cached_models(self, tenant_id: str):
    
        registry = ModelRegistry()
        tenant_models = registry.get_all_for_tenant(tenant_id)
        
        if tenant_models:
            logger.info(f"[STARTUP]  Cached models for tenant {tenant_id}:")
            for purpose, provider in tenant_models.items():
                logger.info(f"   {purpose}: {provider.config.provider} ({provider.config.model_name})")

    def cleanup_tenant_service(self, tenant_id: str):

        if tenant_id in self._tenant_services:
            service = self._tenant_services[tenant_id]
            
            if hasattr(service, 'cleanup'):
                try:
                    service.cleanup()
                except Exception as e:
                    logger.warning(f"[STARTUP]  Service cleanup error: {e}")
            
            del self._tenant_services[tenant_id]
            logger.info(f"[STARTUP]  Cleaned up service for tenant: {tenant_id}")
        else:
            logger.warning(f"[STARTUP]  No cached service found for tenant: {tenant_id}")
        
model_startup = ModelStartup()