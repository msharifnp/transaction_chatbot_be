import logging
from typing import Any, Optional
from src.config.db_config import Config as DatabaseConfig
from src.db.db_service import DatabaseService

logger = logging.getLogger(__name__)


class AiTokenUsageService:

    def __init__(self):
        self.db_service = DatabaseService(DatabaseConfig.get_database_config())

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        if value is None:
            return None

        try:
            return int(value)
        except (TypeError, ValueError):
            logger.warning("[AI_TOKEN_USAGE] Unable to convert %r to int", value)
            return None

    def store_usage(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str,
        purpose: str,
        prompt_tokens: Any,
        completion_tokens: Any,
        thoughts_tokens: Any,
        cache_tokens: Any,
        total_tokens: Any,
        model_name: str,
        provider: str,
        latency_ms: Any,
    ) -> None:
        normalized_cache_tokens = 0 if cache_tokens is None else cache_tokens

        query = """
            INSERT INTO "data"."AiTokenUsage" (
                "TenantId",
                "UserId",
                "SessionId",
                "Purpose",
                "PromptTokens",
                "CompletionTokens",
                "ThoughtsTokens",
                "CacheTokens",
                "TotalTokens",
                "ModelName",
                "Provider",
                "LatencyMs"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.db_service.execute_query(
            query,
            (
                tenant_id,
                user_id,
                session_id,
                purpose,
                self._to_int(prompt_tokens),
                self._to_int(completion_tokens),
                self._to_int(thoughts_tokens),
                self._to_int(normalized_cache_tokens),
                self._to_int(total_tokens),
                model_name,
                provider,
                self._to_int(latency_ms),
            ),
        )
