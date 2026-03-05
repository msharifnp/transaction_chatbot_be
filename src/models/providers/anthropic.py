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
    
    def generate_text(self, prompt: str, **kwargs) -> str:
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
        
        return response.content[0].text or ""
