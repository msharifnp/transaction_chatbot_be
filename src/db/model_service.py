try:
    from google import genai
    GEMINI_AVAILABLE = True
except Exception:
    GEMINI_AVAILABLE = False

from src.config.model_config import GeminiConfig


class ModelService:
    def __init__(self, config: GeminiConfig):
        self.api_key = config.api_key
        self.model_name = config.model
        self.temperature = config.temperature
        self.top_p = config.top_p
        self.top_k = config.top_k
        self.max_output_tokens = config.max_output_tokens

        self.enabled = GEMINI_AVAILABLE and bool(self.api_key)
        self.client = None

        if not self.enabled:
            print("[GEMINI] : Service disabled")
            return

        self.client = genai.Client(api_key=self.api_key)

        self.generation_config = {
            "temperature": self.temperature,
            "top_p": self.top_p,
            "top_k": self.top_k,
            "max_output_tokens": self.max_output_tokens,
        }

        print(f"[GEMINI] ✅ Service enabled - Model: {self.model_name}")

    def generate_text(self, prompt: str) -> str:
        if not self.is_available():
            raise RuntimeError("Gemini not available")

        response = self.client.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config={
                **self.generation_config,
                "safety_settings": [
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
                ],
            },
        )

        return getattr(response, "text", "") or ""

    def is_available(self) -> bool:
        return self.enabled and self.client is not None
