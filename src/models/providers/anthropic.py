from typing import Any

from src.models.base import BaseModelProvider
from anthropic import Anthropic
import logging
logger = logging.getLogger(__name__)

class AnthropicProvider(BaseModelProvider):

    def _initialize(self):
                      
        if not self.config.api_key:
            logger.warning(f"[ANTHROPIC]  API key missing - skipping initialization")
            self.enabled = False
            return
        try:    
            self.client = Anthropic(api_key=self.config.api_key)
            self.enabled = True
            logger.info(f"[ANTHROPIC]  Initialized - Model: {self.config.model_name}")  
        except Exception as e:
            logger.error(f"[ANTHROPIC]  Initialization failed: {e}")
            self.enabled = False
    
    def generate_text(self, prompt: str, **kwargs) -> Any:
        if not self.is_available():
            raise RuntimeError("Anthropic not available")
        
        messages = kwargs.get('messages', [{"role": "user", "content": prompt}])
        
        response = self.client.messages.create(
            model=self.config.model_name,
            max_tokens=self.config.max_output_tokens,
            temperature=self.config.temperature,
            top_p=self.config.top_p,
            messages=messages,
            **kwargs.get('extra_params', {})
        )

        usage_data = getattr(response, "usage", None)
        prompt_tokens = getattr(usage_data, "input_tokens", None)
        completion_tokens = getattr(usage_data, "output_tokens", None)
        cache_creation_tokens = getattr(usage_data, "cache_creation_input_tokens", None) or 0
        cache_read_tokens = getattr(usage_data, "cache_read_input_tokens", None) or 0
        total_tokens = None

        if prompt_tokens is not None or completion_tokens is not None:
            total_tokens = (prompt_tokens or 0) + (completion_tokens or 0)

        text_parts = [
            block.text
            for block in getattr(response, "content", [])
            if getattr(block, "type", None) == "text" and getattr(block, "text", None)
        ]

        return {
            "text": "".join(text_parts),
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "thoughts_tokens": None,
                "cache_tokens": cache_creation_tokens + cache_read_tokens,
                "total_tokens": total_tokens,
            },
        }
