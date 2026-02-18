from typing import Optional, Dict
from src.models.base import BaseModelProvider
import logging
from threading import Lock

logger = logging.getLogger(__name__)


class ModelRegistry:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._models = {}   # <-- IMPORTANT
            self._lock = Lock()
            self._initialized = True

    def register_model(self, tenant_id: str, purpose: str, provider: BaseModelProvider):
        """Register a model instance in cache"""
        key = (tenant_id, purpose)
        self._models[key] = provider
        logger.info(
            f"[REGISTRY] Registered {provider.config.provider} for "
            f"tenant={tenant_id}, purpose={purpose}"
        )
    
    def get_model(self, tenant_id: str, purpose: str) -> Optional[BaseModelProvider]:
        """Get a cached model instance"""
        return self._models.get((tenant_id, purpose))
    
    def get_all_for_tenant(self, tenant_id: str) -> Dict[str, BaseModelProvider]:
        """Get all models for a specific tenant"""
        return {
            purpose: provider 
            for (tid, purpose), provider in self._models.items() 
            if tid == tenant_id
        }
    
    def remove_model(self, tenant_id: str, purpose: str):
        """Remove a model from cache"""
        key = (tenant_id, purpose)
        if key in self._models:
            del self._models[key]
            logger.info(f"[REGISTRY] Removed model for tenant={tenant_id}, purpose={purpose}")
    
    def clear_tenant(self, tenant_id: str):
        """Clear all models for a tenant"""
        keys_to_remove = [
            key for key in self._models.keys() 
            if key[0] == tenant_id
        ]
        for key in keys_to_remove:
            del self._models[key]
        logger.info(f"[REGISTRY] Cleared {len(keys_to_remove)} models for tenant={tenant_id}")
    
    def unload_tenant_models(self, tenant_id: str):
        """
        Remove all models for a tenant from memory.
        Calls cleanup methods on providers if available.
        """
        # ✅ FIXED: Use self._models instead of self._registry
        tenant_models = self.get_all_for_tenant(tenant_id)
        
        if not tenant_models:
            logger.warning(f"[REGISTRY] No models found for tenant={tenant_id}")
            return
        
        # Call cleanup on each provider
        for purpose, provider in tenant_models.items():
            try:
                if hasattr(provider, 'unload'):
                    provider.unload()
                elif hasattr(provider, 'cleanup'):
                    provider.cleanup()
                logger.debug(f"[REGISTRY] Cleaned up {purpose} for tenant={tenant_id}")
            except Exception as e:
                logger.error(f"[REGISTRY] Cleanup failed for {purpose}: {e}")
        
        # Remove all keys for this tenant
        self.clear_tenant(tenant_id)
        
        logger.info(f"[REGISTRY] ✅ Unloaded {len(tenant_models)} models for tenant={tenant_id}")
        
    
    
 

    def get_all_tenants(self):
        with self._lock:
            return list(set(key[0] for key in self._models.keys()))
    
    def clear_all(self):
        """Clear all models from registry (for shutdown)"""
        count = len(self._models)
        self._models.clear()
        logger.info(f"[REGISTRY] Cleared all models ({count} total)")