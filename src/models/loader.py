from typing import Any
from src.models.config import ModelConfig
from src.models.factory import ModelProviderFactory
from src.models.registry import ModelRegistry
import json
import os

class ModelLoader:
    """Load models from JSON file and populate registry"""

    def __init__(self):
        self.registry = ModelRegistry()

    def load_tenant_models(self, tenant_id: str):
        """Load all models for a specific tenant from JSON config file"""
        print(f"\n{'='*70}")
        print(f"[LOADER] 📥 Loading models for tenant: {tenant_id}")
        print(f"{'='*70}")

        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'model_config.json')
        config_path = os.path.abspath(config_path)
        with open(config_path, 'r', encoding='utf-8') as f:
            all_models = json.load(f)

        rows = [row for row in all_models if row.get('TenantId') == tenant_id]
        print(f"[LOADER] Found {len(rows)} model configurations in JSON file")

        loaded_count = 0
        for idx, row in enumerate(rows, 1):
            try:
                purpose = row['Purpose']
                provider = row['Provider']
                model_name = row['ModelName']

                print(f"\n[LOADER] 📋 Model #{idx}:")
                print(f"  ├─ Purpose: {purpose}")
                print(f"  ├─ Provider: {provider}")
                print(f"  ├─ Model: {model_name}")
                print(f"  ├─ Temperature: {row.get('Temperature', 0.7)}")
                print(f"  ├─ TopP: {row.get('TopP', 0.9)}")
                print(f"  ├─ TopK: {row.get('TopK', 40)}")
                print(f"  ├─ MaxTokens: {row.get('MaxOutputTokens', 18192)}")
                print(f"  └─ API Key: {row['ApiKey'][:10]}..." if row['ApiKey'] else "  └─ API Key: None")

                # Create config from JSON row
                config = ModelConfig(
                    provider=provider,
                    model_name=model_name,
                    api_key=row['ApiKey'],
                    temperature=row.get('Temperature', 0.7),
                    top_p=row.get('TopP', 0.9),
                    top_k=row.get('TopK', 40),
                    max_output_tokens=row.get('MaxOutputTokens', 18192)
                )

                # Create provider instance
                print(f"  └─ Creating {provider} provider instance...")
                provider_instance = ModelProviderFactory.create(config)

                # Register in cache
                if provider_instance.is_available():
                    self.registry.register_model(tenant_id, purpose, provider_instance)
                    loaded_count += 1
                    print(f"  └─ ✅ Successfully loaded and cached")
                else:
                    print(f"  └─ ⚠️  Provider not available (initialization failed)")

            except Exception as e:
                print(f"  └─ ❌ Failed to load: {e}")
                import traceback
                traceback.print_exc()

        print(f"\n{'='*70}")
        print(f"[LOADER] ✅ Successfully loaded {loaded_count}/{len(rows)} models")
        print(f"{'='*70}\n")

        return loaded_count