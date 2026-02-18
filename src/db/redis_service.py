from datetime import datetime
import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class _InMemoryRedisClient:
    """Tiny compatibility layer for existing call-sites using redis_client.*."""

    def __init__(self, service: "RedisService"):
        self._service = service

    def ping(self) -> bool:
        return True

    def info(self) -> Dict[str, Any]:
        return {
            "redis_version": "in-memory",
            "mode": "langchain-demo-memory",
        }

    def exists(self, key: str) -> int:
        return 1 if self._service._exists_key(key) else 0


class RedisService:
    """
    In-memory session/message store used for demo mode.

    This keeps the previous RedisService API so routers/services can remain unchanged.
    Data is process-local and non-persistent.
    """

    _lock = threading.RLock()
    _messages_by_key: Dict[str, List[Dict[str, Any]]] = {}
    _key_expires_at: Dict[str, float] = {}
    _active_sessions_by_tenant: Dict[str, int] = {}
    _session_activity: Dict[Tuple[str, str], float] = {}
    _tenant_activity: Dict[str, float] = {}

    def __init__(self, config):
        self.config = config
        self.memory_ttl = 72000
        self.redis_client = _InMemoryRedisClient(self)
        logger.info("Using in-memory chat memory (Redis disabled)")

    # ==================== INTERNAL HELPERS ====================

    def _now(self) -> float:
        return time.time()

    def _expire_if_needed(self, key: str) -> None:
        expires_at = self._key_expires_at.get(key)
        if expires_at and self._now() > expires_at:
            self._messages_by_key.pop(key, None)
            self._key_expires_at.pop(key, None)

    def _exists_key(self, key: str) -> bool:
        with self._lock:
            self._expire_if_needed(key)
            return key in self._messages_by_key

    # ==================== MESSAGE STORAGE ====================

    def get_messages_key(self, TenantId: str, SessionId: str) -> str:
        return f"ivpsql:{TenantId}:session:{SessionId}:messages"

    def validate_tenant_session(self, TenantId: str, SessionId: str) -> bool:
        if not TenantId or not TenantId.strip():
            raise ValueError("TenantId is required and cannot be empty")
        if not SessionId or not SessionId.strip():
            raise ValueError("SessionId is required and cannot be empty")
        return True

    def store_message(
        self,
        TenantId: str,
        SessionId: str,
        role: str,
        content: str,
        metadata: Optional[Dict] = None,
    ) -> int:
        self.validate_tenant_session(TenantId, SessionId)
        messages_key = self.get_messages_key(TenantId, SessionId)

        with self._lock:
            self._expire_if_needed(messages_key)
            messages = self._messages_by_key.setdefault(messages_key, [])

            if len(messages) == 0:
                self.incr_active_sessions(TenantId)
                self.set_session_activity(TenantId, SessionId)

            index = len(messages)
            message = {
                "index": index,
                "role": role,
                "content": "" if content is None else str(content),
                "timestamp": datetime.now().isoformat(),
                "metadata": metadata or {},
            }
            messages.append(message)

            self._key_expires_at[messages_key] = self._now() + self.memory_ttl

            return index

    def get_message_by_index(self, TenantId: str, SessionId: str, index: int) -> Optional[Dict]:
        messages_key = self.get_messages_key(TenantId, SessionId)
        with self._lock:
            self._expire_if_needed(messages_key)
            messages = self._messages_by_key.get(messages_key, [])
            if 0 <= index < len(messages):
                return messages[index]
            return None

    def get_all_messages(self, TenantId: str, SessionId: str) -> List[Dict]:
        self.validate_tenant_session(TenantId, SessionId)
        messages_key = self.get_messages_key(TenantId, SessionId)
        with self._lock:
            self._expire_if_needed(messages_key)
            return list(self._messages_by_key.get(messages_key, []))

    def get_context_for_ai(self, TenantId: str, SessionId: str) -> Tuple[Optional[List[Dict]], List[Dict]]:
        all_messages = self.get_all_messages(TenantId, SessionId)
        if not all_messages:
            return None, []

        last_system_data: Optional[List[Dict]] = None
        for msg in reversed(all_messages):
            if msg.get("role") != "system":
                continue
            try:
                data_arr = json.loads(msg.get("content", ""))
                if isinstance(data_arr, list) and len(data_arr) > 0:
                    last_system_data = data_arr
                    break
            except json.JSONDecodeError:
                continue

        if not last_system_data:
            return None, []

        # Keep recent conversational context for follow-up AI questions:
        # include up to the last 2 user messages and last 2 assistant messages.
        user_messages = [m for m in all_messages if m.get("role") == "user"][-2:]
        ai_messages = [m for m in all_messages if m.get("role") == "assistant"][-2:]

        selected_indexes = {
            m.get("index")
            for m in (user_messages + ai_messages)
            if m.get("index") is not None
        }

        conversation: List[Dict] = []
        if selected_indexes:
            for msg in all_messages:
                if msg.get("role") not in {"user", "assistant"}:
                    continue
                if msg.get("index") in selected_indexes:
                    conversation.append(
                        {"role": msg.get("role"), "content": msg.get("content", "")}
                    )
        else:
            # Fallback if indexes are unavailable for any reason.
            for msg in (user_messages + ai_messages):
                conversation.append(
                    {"role": msg.get("role"), "content": msg.get("content", "")}
                )

        return last_system_data, conversation

    def get_data_by_index(self, TenantId: str, SessionId: str, index: int) -> Optional[Dict[str, Any]]:
        self.validate_tenant_session(TenantId, SessionId)
        message = self.get_message_by_index(TenantId, SessionId, index)
        if not message:
            return None

        result: Dict[str, Any] = {
            "role": message.get("role"),
            "content": message.get("content"),
            "metadata": message.get("metadata", {}),
            "timestamp": message.get("timestamp"),
        }
        if message.get("role") == "system":
            try:
                result["data"] = json.loads(message.get("content", ""))
            except json.JSONDecodeError:
                result["data"] = None
        return result

    # ==================== SESSION MANAGEMENT ====================

    def delete_session(self, TenantId: str, SessionId: str) -> int:
        self.validate_tenant_session(TenantId, SessionId)
        messages_key = self.get_messages_key(TenantId, SessionId)

        with self._lock:
            deleted_count = 0
            if messages_key in self._messages_by_key:
                self._messages_by_key.pop(messages_key, None)
                self._key_expires_at.pop(messages_key, None)
                deleted_count += 1

            self._session_activity.pop((TenantId, SessionId), None)

            if self._active_sessions_by_tenant.get(TenantId, 0) > 0:
                self._active_sessions_by_tenant[TenantId] -= 1
                if self._active_sessions_by_tenant[TenantId] <= 0:
                    self._active_sessions_by_tenant.pop(TenantId, None)

            return deleted_count

    def get_tenant_sessions(self, TenantId: str) -> List[str]:
        if not TenantId or not TenantId.strip():
            raise ValueError("TenantId is required")
        prefix = f"ivpsql:{TenantId}:session:"
        suffix = ":messages"
        with self._lock:
            session_ids: List[str] = []
            for key in self._messages_by_key.keys():
                self._expire_if_needed(key)
                if key.startswith(prefix) and key.endswith(suffix):
                    session_ids.append(key[len(prefix) : -len(suffix)])
            return session_ids

    # ==================== ACTIVE SESSION COUNTER ====================

    def get_active_sessions_key(self, tenant_id: str) -> str:
        return f"ivpsql:{tenant_id}:metadata:active_sessions"

    def incr_active_sessions(self, tenant_id: str) -> int:
        with self._lock:
            count = self._active_sessions_by_tenant.get(tenant_id, 0) + 1
            self._active_sessions_by_tenant[tenant_id] = count
            return count

    def decr_active_sessions(self, tenant_id: str) -> int:
        with self._lock:
            count = max(self._active_sessions_by_tenant.get(tenant_id, 0) - 1, 0)
            if count == 0:
                self._active_sessions_by_tenant.pop(tenant_id, None)
            else:
                self._active_sessions_by_tenant[tenant_id] = count
            return count

    def get_active_sessions(self, tenant_id: str) -> int:
        with self._lock:
            return self._active_sessions_by_tenant.get(tenant_id, 0)

    # ==================== SESSION ACTIVITY TRACKING ====================

    def get_session_keys(self, tenant_id: str) -> list:
        with self._lock:
            return [session_id for (tid, session_id) in self._session_activity.keys() if tid == tenant_id]

    def get_session_last_activity(self, tenant_id: str, session_id: str) -> Optional[float]:
        with self._lock:
            return self._session_activity.get((tenant_id, session_id))

    def set_session_activity(self, tenant_id: str, session_id: str):
        with self._lock:
            self._session_activity[(tenant_id, session_id)] = self._now()

    def remove_session(self, tenant_id: str, session_id: str):
        with self._lock:
            self._session_activity.pop((tenant_id, session_id), None)

    # ==================== TENANT ACTIVITY TRACKING ====================

    def get_tenant_last_activity(self, tenant_id: str) -> Optional[float]:
        with self._lock:
            return self._tenant_activity.get(tenant_id)

    def update_tenant_activity(self, tenant_id: str):
        with self._lock:
            self._tenant_activity[tenant_id] = self._now()

    def clear_tenant_activity(self, tenant_id: str):
        with self._lock:
            self._tenant_activity.pop(tenant_id, None)

    def delete_all_tenant_data(self, tenant_id: str) -> int:
        if not tenant_id or not tenant_id.strip():
            raise ValueError("TenantId is required")

        prefix = f"ivpsql:{tenant_id}:"
        with self._lock:
            keys_to_delete = [k for k in self._messages_by_key.keys() if k.startswith(prefix)]
            for key in keys_to_delete:
                self._messages_by_key.pop(key, None)
                self._key_expires_at.pop(key, None)

            for k in [k for k in self._session_activity.keys() if k[0] == tenant_id]:
                self._session_activity.pop(k, None)

            self._active_sessions_by_tenant.pop(tenant_id, None)
            self._tenant_activity.pop(tenant_id, None)
            return len(keys_to_delete)

    # ==================== CONNECTION MANAGEMENT ====================

    def close(self):
        return

    @classmethod
    def close_all_pools(cls):
        return
