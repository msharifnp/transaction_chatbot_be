from src.models.base import BaseModelProvider
from openai import OpenAI
import logging
logger = logging.getLogger(__name__)

class OpenAIProvider(BaseModelProvider):

    def _initialize(self):
        
        if not self.config.api_key:
            logger.warning(f"[OPENAI]  API key missing - skipping initialization")
            self.enabled = False
            return
        try:    
            self.client = OpenAI(api_key=self.config.api_key)
            self.enabled = True
            logger.info(f"[OPENAI]  Initialized - Model: {self.config.model_name}")  
        except Exception as e:
            logger.error(f"[OPENAI]  Initialization failed: {e}")
            self.enabled = False
    
    def generate_text(self, prompt: str, **kwargs) -> str:
        if not self.is_available():
            raise RuntimeError("OpenAI not available")
        
        messages = kwargs.get('messages', [{"role": "user", "content": prompt}])
        
        response = self.client.chat.completions.create(
            model=self.config.model_name,
            messages=messages,
            temperature=self.config.temperature,
            top_p=self.config.top_p,
            max_tokens=self.config.max_output_tokens,
            **kwargs.get('extra_params', {})
        )
        
        return response.choices[0].message.content or ""