from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import json
import redis
from src.config.redis_config import RedisConfig


class RedisService:
      
    def __init__(self,Config:RedisConfig):
        
        self.config = Config
        self.redis_client = redis.Redis(
            host=self.config.host,
            port=self.config.port,
            db=self.config.db,
            username=self.config.username,
            password=self.config.password,
            decode_responses=self.config.decode_responses,
            socket_timeout=self.config.socket_timeout,
            socket_connect_timeout=self.config.socket_connect_timeout
        )
        
        try:
            self.redis_client.ping()
            print(f"[REDIS] ✅ Connected successfully to Redis DB {self.config.db}")
        except redis.ConnectionError as e:
            print(f"[REDIS] ❌ Connection failed: {e}")
            raise RuntimeError("Redis connection failed")
        
        self.memory_ttl = 3600  # 24 hours
    
    def get_messages_key(self, TenantId: str, SessionId: str) -> str:
        """
        Generate Redis key for messages list.
        Format: ivpsql:{TenantId}:{SessionId}:messages
        """
        return f"ivpsql:{TenantId}:{SessionId}:messages"
    
    def validate_tenant_session(self, TenantId: str, SessionId: str) -> bool:
        """Validate that TenantId and SessionId are not empty."""
        if not TenantId or TenantId.strip() == "":
            raise ValueError("TenantId is required and cannot be empty")
        if not SessionId or SessionId.strip() == "":
            raise ValueError("SessionId is required and cannot be empty")
        return True
    
    
    def store_message(self, TenantId: str, SessionId: str, role: str, content: str, metadata: Optional[Dict] = None) -> int:

        self.validate_tenant_session(TenantId, SessionId)
        messages_key = self.get_messages_key(TenantId, SessionId)

        # 🔥 CRITICAL SAFETY
        if content is None:
            content = ""

        content = str(content)  # FORCE STRING

        index = self.redis_client.llen(messages_key)

        message = {
            "index": index,
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat(),
            "metadata": metadata or {}
        }

        # 🔥 THE MOST IMPORTANT LINE
        self.redis_client.rpush(
            messages_key,
            json.dumps(message, ensure_ascii=False)  # 👈 DO NOT REMOVE
        )

        self.redis_client.expire(messages_key, self.memory_ttl)

        print(f"[REDIS] ✅ Stored {role} message at index {index} for tenant:{TenantId}, session:{SessionId}")
        return index

    
    def get_message_by_index(self, TenantId: str, SessionId: str, index: int) -> Optional[Dict]:
        """Retrieve a single message by index from Redis LIST."""
        messages_key = self.get_messages_key(TenantId, SessionId)
        
        try:
            # LINDEX returns the element at index
            data = self.redis_client.lindex(messages_key, index)
            if not data:
                return None
            return json.loads(data)
        except Exception as e:
            print(f"[REDIS] ⚠️ Failed to retrieve message at index {index}: {e}")
            return None
    
    def get_all_messages(self, TenantId: str, SessionId: str) -> List[Dict]:
        """
        Retrieve all messages for a session from Redis LIST.
        """
        self.validate_tenant_session(TenantId, SessionId)
        
        messages_key = self.get_messages_key(TenantId, SessionId)
        
        try:
            # LRANGE 0 -1 returns all elements
            raw_messages = self.redis_client.lrange(messages_key, 0, -1)
            
            if not raw_messages:
                return []
            
            messages = [json.loads(msg) for msg in raw_messages]
            
            print(f"[REDIS] Retrieved {len(messages)} messages for tenant:{TenantId}, session:{SessionId}")
            return messages
            
        except Exception as e:
            print(f"[REDIS] ⚠️ Failed to retrieve messages: {e}")
            return []
    
    def get_context_for_ai(self, TenantId: str, SessionId: str) -> Tuple[Optional[List[Dict]], List[Dict]]:
        """
        Retrieve context for AI mode:
        - Last 1 SYSTEM message (latest database results as raw JSON array)
        - Last 1 USER message
        - Last 1 ASSISTANT message
        
        Returns:
            (system_data, conversation_messages)
            system_data = List[Dict] from last system message
            conversation_messages = [{"role": "user/assistant", "content": "..."}]
        """
        all_messages = self.get_all_messages(TenantId, SessionId)
        
        if not all_messages:
            print(f"[REDIS] ⚠️ No messages found for tenant:{TenantId}, session:{SessionId}")
            return None, []
        
        print(f"[REDIS] Found {len(all_messages)} total messages")
        
        # Find last system message
        last_system_data = None
        system_index = None
        for msg in reversed(all_messages):
            if msg["role"] == "system":
                try:
                    data_arr = json.loads(msg["content"])
                    if isinstance(data_arr, list) and len(data_arr) ==0:
                        continue  # Skip empty data
                    # Content is now a raw JSON array of data
                    last_system_data = data_arr
                    system_index = msg["index"]
                    print(f"[REDIS] ✅ Found system data at index {system_index}: {len(last_system_data)} rows")
                    break
                except json.JSONDecodeError as e:
                    print(f"[REDIS] ⚠️ Failed to parse system JSON at index {msg['index']}: {e}")
                    print(f"[REDIS] Content preview: {msg['content'][:200]}")
                    continue
        
        if not last_system_data:
            print(f"[REDIS] ⚠️ No valid system data found")
            return None, []
        
        # Get last 1 user and last 1 assistant message
        user_messages = [m for m in all_messages if m["role"] == "user"]
        ai_messages = [m for m in all_messages if m["role"] == "assistant"]
        
        conversation = []
        
        # Add last user message
        if user_messages:
            last_user = user_messages[-1]
            conversation.append({"role": "user", "content": last_user["content"]})
            print(f"[REDIS] Added last user message from index {last_user['index']}")
        
        # Add last AI message
        if ai_messages:
            last_ai = ai_messages[-1]
            conversation.append({"role": "assistant", "content": last_ai["content"]})
            print(f"[REDIS] Added last AI message from index {last_ai['index']}")
        
        print(f"[REDIS] Context summary: system_data={len(last_system_data)} rows, conversation={len(conversation)} msgs")
        
        return last_system_data, conversation
    
    
    def get_data_by_index(self, TenantId: str, SessionId: str, index: int) -> Optional[Dict[str, Any]]:
        """
        Retrieve specific message data by index for downloading.
        
        Returns:
            {
                "role": "system" | "assistant" | "user",
                "content": str (JSON string for system, text for others),
                "data": List[Dict] (parsed if system role),
                "metadata": Dict,
                "timestamp": str
            }
        """
        self.validate_tenant_session(TenantId, SessionId)
        
        message = self.get_message_by_index(TenantId, SessionId, index)
        
        if not message:
            return None
        
        result = {
            "role": message["role"],
            "content": message["content"],
            "metadata": message.get("metadata", {}),
            "timestamp": message.get("timestamp")
        }
        
        # If system role, parse the data for easy download
        if message["role"] == "system":
            try:
                result["data"] = json.loads(message["content"])
            except json.JSONDecodeError:
                result["data"] = None
        
        return result
    
    def get_session_history(self, TenantId: str, SessionId: str) -> List[Dict]:
        """Get full conversation history with tenant validation."""
        self._validate_tenant_session(TenantId, SessionId)
        
        messages = self.get_all_messages(TenantId, SessionId)
        
        history = []
        for msg in messages:
            if msg["role"] == "system":
                content = msg["content"][:200] + "..." if len(msg["content"]) > 200 else msg["content"]
                history.append({
                    "index": msg["index"],
                    "role": "system",
                    "content": content,
                    "timestamp": msg.get("timestamp")
                })
            else:
                history.append({
                    "index": msg["index"],
                    "role": msg["role"],
                    "content": msg["content"],
                    "timestamp": msg.get("timestamp")
                })
        
        return history
    
    def clear_session(self, TenantId: str, SessionId: str) -> None:
        """Clear session memory by deleting the messages list."""
        self.validate_tenant_session(TenantId, SessionId)
        
        messages_key = self._get_messages_key(TenantId, SessionId)
        
        try:
            self.redis_client.delete(messages_key)
            print(f"[REDIS] ✅ Cleared session for tenant:{TenantId}, session:{SessionId}")
        except Exception as e:
            print(f"[REDIS] ⚠️ Failed to clear session: {e}")
    
    def get_tenant_sessions(self, TenantId: str) -> List[str]:
        """Get all session IDs for a tenant."""
        if not TenantId or TenantId.strip() == "":
            raise ValueError("TenantId is required")
        
        pattern = f"ivpsql:{TenantId}:*:messages"
        
        try:
            keys = self.redis_client.keys(pattern)
            sessions = []
            for key in keys:
                parts = key.split(":")
                if len(parts) >= 3:
                    sessions.append(parts[2])
            
            return list(set(sessions))
        except Exception as e:
            print(f"[REDIS] ⚠️ Failed to get tenant sessions: {e}")
            return []
    
    def close(self):
        """Close database and Redis connections."""
        self.db.close()
        try:
            self.redis_client.close()
            print("[REDIS] Connection closed")
        except Exception as e:
            print(f"[REDIS] ⚠️ Failed to close connection: {e}")
        
        print("[AGENT] All connections closed")
