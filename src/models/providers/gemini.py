from src.models.base import BaseModelProvider

class GeminiProvider(BaseModelProvider):
    """Gemini provider implementation"""
    
    def _initialize(self):
        try:
            from google import genai
            
            if not self.config.api_key:
                print(f"[GEMINI] No API key provided")
                return
            
            self.client = genai.Client(api_key=self.config.api_key)
            self.enabled = True
            print(f"[GEMINI] ✅ Initialized - Model: {self.config.model_name}")
            
        except Exception as e:
            print(f"[GEMINI] ❌ Initialization failed: {e}")
            self.enabled = False
    
    def generate_text(self, prompt: str, **kwargs) -> str:
        if not self.is_available():
            raise RuntimeError("Gemini not available")
        
        response = self.client.models.generate_content(
            model=self.config.model_name,
            contents=prompt,
            config={
                "temperature": self.config.temperature,
                "top_p": self.config.top_p,
                "top_k": self.config.top_k,
                "max_output_tokens": self.config.max_output_tokens,
                "safety_settings": [
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
                ],
                **(kwargs.get('extra_config', {}))
            },
        )
        
        return getattr(response, "text", "") or ""