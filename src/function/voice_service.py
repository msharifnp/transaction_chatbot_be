import logging
from json import JSONDecodeError

from src.config.startup import model_startup
from src.schemas.schemas import VoiceTranscriptionResponse

logger = logging.getLogger(__name__)

VOICE_PURPOSE = "Voice"


class VoiceService:
    def transcribe_audio(
        self,
        tenant_id: str,
        audio_bytes: bytes,
    ) -> VoiceTranscriptionResponse:
        if not audio_bytes:
            return VoiceTranscriptionResponse(
                success=False,
                code=400,
                message="Audio content is empty",
                errors=["EMPTY_AUDIO"],
                data=None,
            )

        try:
            model_service = model_startup.get_or_create_service(tenant_id)
            model = model_service.get_model(VOICE_PURPOSE)

            transcript_text = (
                model_service.transcribe(
                    VOICE_PURPOSE,
                    audio_bytes,
                    filename="voice-input.wav",
                )
                or ""
            ).strip()

            if not transcript_text:
                return VoiceTranscriptionResponse(
                    success=False,
                    code=422,
                    message="No speech could be transcribed from the audio",
                    errors=["VOICE_TRANSCRIPT_EMPTY"],
                    data=None,
                )

            return VoiceTranscriptionResponse(
                success=True,
                code=200,
                message="Audio transcribed successfully",
                data={
                    "transcript": transcript_text,
                    "provider": model.config.provider,
                    "model_name": model.config.model_name,
                },
            )
        except JSONDecodeError as e:
            logger.error("[VOICE] Invalid service account JSON: %s", e, exc_info=True)
            return VoiceTranscriptionResponse(
                success=False,
                code=400,
                message=(
                    "Voice credentials JSON is invalid. Paste exactly one "
                    "service-account JSON object with no extra text or duplicate JSON blocks."
                ),
                errors=["VOICE_CREDENTIALS_INVALID_JSON"],
                data=None,
            )
        except NotImplementedError as e:
            logger.warning("[VOICE] Unsupported transcription provider for tenant %s: %s", tenant_id, e)
            return VoiceTranscriptionResponse(
                success=False,
                code=400,
                message=str(e),
                errors=["VOICE_PROVIDER_NOT_SUPPORTED"],
                data=None,
            )
        except ValueError as e:
            logger.error("[VOICE] Voice model unavailable: %s", e, exc_info=True)
            return VoiceTranscriptionResponse(
                success=False,
                code=404,
                message=str(e),
                errors=["VOICE_CONFIG_NOT_FOUND"],
                data=None,
            )
        except Exception as e:
            logger.error("[VOICE] Failed to transcribe audio: %s", e, exc_info=True)
            return VoiceTranscriptionResponse(
                success=False,
                code=500,
                message=f"Failed to transcribe audio: {str(e)}",
                errors=["VOICE_TRANSCRIPTION_FAILED"],
                data=None,
            )
