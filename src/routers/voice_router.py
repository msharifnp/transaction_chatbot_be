from fastapi import APIRouter, File, HTTPException, Request, UploadFile, status

from src.function.voice_service import VoiceService
from src.schemas.schemas import VoiceTranscriptionResponse

router = APIRouter(prefix="/api/voice", tags=["Voice"])

voice_service = VoiceService()


def _raise_if_failed(response: VoiceTranscriptionResponse):
    if response.success:
        return

    raise HTTPException(
        status_code=response.code,
        detail={
            "success": response.success,
            "message": response.message,
            "errors": response.errors,
        },
    )


@router.post(
    "/transcribe",
    response_model=VoiceTranscriptionResponse,
    status_code=status.HTTP_200_OK,
)
async def transcribe_voice(
    request: Request,
    audio_file: UploadFile = File(...),
):
    if not (audio_file.content_type or "").startswith("audio/"):
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Only audio uploads are supported",
                "errors": ["INVALID_AUDIO_CONTENT_TYPE"],
            },
        )

    audio_bytes = await audio_file.read()
    response = voice_service.transcribe_audio(request.state.TenantId, audio_bytes)
    _raise_if_failed(response)

    if request.state.SessionCreated:
        response.metadata = {"new_session_id": request.state.SessionId}

    return response
