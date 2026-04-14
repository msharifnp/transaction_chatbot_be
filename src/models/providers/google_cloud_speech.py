import json
import logging
from pathlib import Path
from json import JSONDecodeError

from google.api_core.client_options import ClientOptions
from google.oauth2 import service_account

from src.models.base import BaseModelProvider

logger = logging.getLogger(__name__)


class GoogleCloudSpeechProvider(BaseModelProvider):
    def _load_service_account_info(self) -> dict:
        raw_credentials = (self.config.api_key or "").strip()
        credentials_ref = (self.config.credentials_ref or "").strip()

        if not raw_credentials:
            raw_credentials = credentials_ref

        if raw_credentials and not raw_credentials.startswith("{"):
            credentials_path = Path(credentials_ref)
            if credentials_path.exists() and credentials_path.is_file():
                raw_credentials = credentials_path.read_text(encoding="utf-8").strip()

        credentials_info = json.loads(raw_credentials)
        if not isinstance(credentials_info, dict):
            raise ValueError("Google service account credentials must be a JSON object")

        return credentials_info

    def _build_credentials(self):
        return service_account.Credentials.from_service_account_info(
            self._load_service_account_info()
        )

    def _resolve_model_identifier(self) -> str:
        configured_identifier = str(
            (self.config.extra_params or {}).get("modelIdentifier") or ""
        ).strip()
        if configured_identifier:
            return configured_identifier

        model_name = str(self.config.model_name or "").strip()
        if model_name.lower() == "chirp-3":
            return "chirp_3"

        return model_name.replace("-", "_")

    def _initialize(self):
        credentials_ref = (self.config.credentials_ref or "").strip()

        if not credentials_ref:
            logger.warning("[GOOGLE_CLOUD_SPEECH] CredentialsRef missing - skipping initialization")
            self.enabled = False
            return

        try:
            region = str((self.config.extra_params or {}).get("region") or "us").lower()
            credentials = self._build_credentials()

            from google.cloud.speech_v2 import SpeechClient

            self.client = SpeechClient(
                credentials=credentials,
                client_options=ClientOptions(
                    api_endpoint=f"{region}-speech.googleapis.com"
                ),
            )
            self.enabled = True
            logger.info(
                "[GOOGLE_CLOUD_SPEECH] Initialized - Model: %s",
                self.config.model_name,
            )
        except JSONDecodeError as e:
            logger.error("[GOOGLE_CLOUD_SPEECH] Invalid JSON credentials: %s", e)
            self.enabled = False
        except Exception as e:
            logger.error("[GOOGLE_CLOUD_SPEECH] Initialization failed: %s", e)
            self.enabled = False

    def generate_text(self, prompt: str, **kwargs):
        raise NotImplementedError("Google Cloud Speech provider does not support text generation")

    def transcribe_audio(self, audio_bytes: bytes, **kwargs):
        if not self.is_available():
            raise RuntimeError("Google Cloud Speech provider is not available")

        from google.cloud.speech_v2.types import cloud_speech

        config = self.config.extra_params or {}
        credentials = self._build_credentials()
        project_id = credentials.project_id
        if not project_id:
            raise ValueError("Google service account JSON does not contain project_id")

        language_codes = [str(config.get("languageCode") or "en-US")]
        language_codes.extend(
            [
                str(code).strip()
                for code in (config.get("alternativeLanguageCodes") or [])
                if str(code).strip()
            ]
        )

        recognition_config = cloud_speech.RecognitionConfig(
            auto_decoding_config=cloud_speech.AutoDetectDecodingConfig(),
            language_codes=language_codes,
            model=self._resolve_model_identifier(),
            features=cloud_speech.RecognitionFeatures(
                enable_automatic_punctuation=bool(
                    config.get("enableAutomaticPunctuation", True)
                ),
                enable_word_time_offsets=bool(
                    config.get("enableWordTimeOffsets", False)
                ),
            ),
        )

        region = str(config.get("region") or "us").lower()
        request = cloud_speech.RecognizeRequest(
            recognizer=f"projects/{project_id}/locations/{region}/recognizers/_",
            config=recognition_config,
            content=audio_bytes,
        )
        response = self.client.recognize(request=request)

        transcript_parts: list[str] = []
        for result in response.results:
            if result.alternatives:
                transcript = (result.alternatives[0].transcript or "").strip()
                if transcript:
                    transcript_parts.append(transcript)

        return " ".join(transcript_parts).strip()
