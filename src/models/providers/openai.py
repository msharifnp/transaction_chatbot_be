from src.models.base import BaseModelProvider

class OpenAIProvider(BaseModelProvider):
    """OpenAI (GPT) provider implementation"""
    
    def _initialize(self):
        try:
            from openai import OpenAI
            
            if not self.config.api_key:
                print(f"[OPENAI] No API key provided")
                return
            
            self.client = OpenAI(api_key=self.config.api_key)
            self.enabled = True
            print(f"[OPENAI] ✅ Initialized - Model: {self.config.model_name}")
            
        except Exception as e:
            print(f"[OPENAI] ❌ Initialization failed: {e}")
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