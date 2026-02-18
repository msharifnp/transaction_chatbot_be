from typing import Dict, Optional
from src.models import ModelLoader, ModelService
from src.db.db_service import DatabaseService

class ModelStartup:
    """Initialize models on application startup"""
    
    _instance = None
    
    def __new__(cls):
        """Singleton pattern"""
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
        """Initialize with database service"""
        self.db_service = db_service
        self.loader = ModelLoader()
        print("[STARTUP] ModelStartup initialized with database")
    
    def get_or_create_service(self, tenant_id: str) -> ModelService:
        """Get or create ModelService for a tenant"""
        
        if self.loader is None:
            raise RuntimeError(
                "ModelStartup not initialized. Call initialize(db_service) first."
            )
        
        # Return cached if exists
        if tenant_id in self._tenant_services:
            print(f"[STARTUP] 🔄 Using cached models for tenant: {tenant_id}")
            self._show_cached_models(tenant_id)
            return self._tenant_services[tenant_id]
        
        print(f"[STARTUP] 🚀 Initializing models for tenant: {tenant_id}")
        
        # Load models from database into registry
        self.loader.load_tenant_models(tenant_id)
        
        # Create ModelService for this tenant
        model_service = ModelService(tenant_id)
        
        # Cache it
        self._tenant_services[tenant_id] = model_service
        
        # Show what's cached
        self._show_cached_models(tenant_id)
        
        return model_service
    
    def _show_cached_models(self, tenant_id: str):
        """Show cached models for a tenant"""
        from src.models.registry import ModelRegistry
        
        registry = ModelRegistry()
        tenant_models = registry.get_all_for_tenant(tenant_id)
        
        if tenant_models:
            print(f"[STARTUP] 💾 Cached models for tenant {tenant_id}:")
            for purpose, provider in tenant_models.items():
                print(f"  ├─ {purpose}: {provider.config.provider} ({provider.config.model_name})")

    def cleanup_tenant_service(self, tenant_id: str):
        """Remove tenant service from cache"""
        # ✅ FIXED: Use correct attribute name
        if tenant_id in self._tenant_services:
            service = self._tenant_services[tenant_id]
            
            # Call cleanup if method exists
            if hasattr(service, 'cleanup'):
                try:
                    service.cleanup()
                except Exception as e:
                    print(f"[STARTUP] ⚠️ Service cleanup error: {e}")
            
            # Remove from cache
            del self._tenant_services[tenant_id]
            print(f"[STARTUP] ✅ Cleaned up service for tenant: {tenant_id}")
        else:
            print(f"[STARTUP] ⚠️ No cached service found for tenant: {tenant_id}")
        
# Global instance
model_startup = ModelStartup()