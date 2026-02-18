from src.models.base import BaseModelProvider

class AnthropicProvider(BaseModelProvider):
    """Anthropic (Claude) provider implementation"""
    
    def _initialize(self):
        try:
            from anthropic import Anthropic
            
            if not self.config.api_key:
                print(f"[ANTHROPIC] No API key provided")
                return
            
            self.client = Anthropic(api_key=self.config.api_key)
            self.enabled = True
            print(f"[ANTHROPIC] ✅ Initialized - Model: {self.config.model_name}")
            
        except Exception as e:
            print(f"[ANTHROPIC] ❌ Initialization failed: {e}")
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
