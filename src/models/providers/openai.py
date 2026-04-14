from typing import Any

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
    
    def generate_text(self, prompt: str, **kwargs) -> Any:
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

        usage_data = getattr(response, "usage", None)
        prompt_details = getattr(usage_data, "prompt_tokens_details", None) if usage_data else None
        completion_details = getattr(usage_data, "completion_tokens_details", None) if usage_data else None

        return {
            "text": response.choices[0].message.content or "",
            "usage": {
                "prompt_tokens": getattr(usage_data, "prompt_tokens", None),
                "completion_tokens": getattr(usage_data, "completion_tokens", None),
                "thoughts_tokens": getattr(completion_details, "reasoning_tokens", None),
                "cache_tokens": getattr(prompt_details, "cached_tokens", None),
                "total_tokens": getattr(usage_data, "total_tokens", None),
            },
        }

    def _uses_max_completion_tokens(self) -> bool:
        model_name = (self.config.model_name or "").lower()
        return model_name.startswith(self._MODELS_USING_MAX_COMPLETION_TOKENS_PREFIXES)

    def transcribe_audio(self, audio_bytes: bytes, **kwargs) -> Any:
        if not self.is_available():
            raise RuntimeError("OpenAI not available")

        import io

        audio_file = io.BytesIO(audio_bytes)
        audio_file.name = kwargs.get("filename", "voice-input.wav")

        response = self.client.audio.transcriptions.create(
            model=self.config.model_name,
            file=audio_file,
        )

        return getattr(response, "text", "") or ""
