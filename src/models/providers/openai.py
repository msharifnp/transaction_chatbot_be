from src.models.base import BaseModelProvider
from openai import OpenAI
import logging
logger = logging.getLogger(__name__)

class OpenAIProvider(BaseModelProvider):

    _MODELS_USING_MAX_COMPLETION_TOKENS_PREFIXES = (
        "gpt-5",
        "o1",
        "o3",
        "o4",
    )

    def _initialize(self):
        
        if not self.config.api_key:
            logger.warning(f"[OPENAI]  API key missing - skipping initialization")
            self.enabled = False
            return
        try:    
            self.client = OpenAI(api_key=self.config.api_key)
            self.enabled = True
            logger.info(
                f"[OPENAI]  Initialized - Model: {self.config.model_name}"
            )
        except Exception as e:
            logger.error(f"[OPENAI]  Initialization failed: {e}")
            self.enabled = False
    
    def generate_text(self, prompt: str, **kwargs) -> str:
        if not self.is_available():
            raise RuntimeError("OpenAI not available")
        messages = kwargs.get('messages', [{"role": "user", "content": prompt}])

        request_kwargs = {
            "model": self.config.model_name,
            "messages": messages,
            "temperature": self.config.temperature,
            "top_p": self.config.top_p,
            **kwargs.get('extra_params', {}),
        }

        token_param = (
            "max_completion_tokens"
            if self._uses_max_completion_tokens()
            else "max_tokens"
        )
        request_kwargs[token_param] = self.config.max_output_tokens

        response = self.client.chat.completions.create(**request_kwargs)
        
        return response.choices[0].message.content or ""

    def _uses_max_completion_tokens(self) -> bool:
        model_name = (self.config.model_name or "").lower()
        return model_name.startswith(self._MODELS_USING_MAX_COMPLETION_TOKENS_PREFIXES)
