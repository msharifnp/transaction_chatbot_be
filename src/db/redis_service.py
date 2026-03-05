from datetime import datetime
import time
from typing import Dict, List, Optional, Tuple, Any
import json
import redis
import logging

logger = logging.getLogger(__name__)

class RedisService:
    _connection_pools: Dict[str, redis.ConnectionPool] = {}
    
    def __init__(self, config):
        self.config = config
        
        pool_key = f"{config.host}:{config.port}:{config.db}"
        
        if pool_key not in RedisService._connection_pools:
            logger.info(f"Creating new Redis connection pool for {pool_key}")
            
            RedisService._connection_pools[pool_key] = redis.ConnectionPool(
                host=config.host,
                port=config.port,
                db=config.db,
                username=config.username,
                password=config.password,
                decode_responses=config.decode_responses,
                socket_timeout=config.socket_timeout,
                socket_connect_timeout=config.socket_connect_timeout,
                max_connections=50,
                socket_keepalive=True,
                health_check_interval=30
                
            )
        
        self.redis_client = redis.Redis(
            connection_pool=RedisService._connection_pools[pool_key]
        )
        
        try:
            self.redis_client.ping()
            logger.info(f" Redis connected - Database {config.db} via pool {pool_key}")
        except redis.ConnectionError as e:
            logger.error(f"Redis connection failed: {e}")
            raise RuntimeError(f"Redis connection failed: {e}")
        
        self.memory_ttl = 36000
    
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
        metadata: Optional[Dict] = None
    ) -> int:
        self.validate_tenant_session(TenantId, SessionId)
        messages_key = self.get_messages_key(TenantId, SessionId)
        
        if self.redis_client.llen(messages_key) == 0:
            self.incr_active_sessions(TenantId)
            self.set_session_activity(TenantId, SessionId)
            logger.info(f"[REDIS]  New session created: {TenantId}:{SessionId}")

        if content is None:
            content = ""
        content = str(content)

        index = self.redis_client.llen(messages_key)

        message = {
            "index": index,
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat(),
            "metadata": metadata or {}
        }

        self.redis_client.rpush(
            messages_key,
            json.dumps(message, ensure_ascii=False, default=str)
        )

        self.redis_client.expire(messages_key, self.memory_ttl)

        logger.debug(f"Stored {role} message at index {index}")
        return index
    
    def get_message_by_index(
        self, 
        TenantId: str, 
        SessionId: str, 
        index: int
    ) -> Optional[Dict]:
        
        messages_key = self.get_messages_key(TenantId, SessionId)
        
        try:
            data = self.redis_client.lindex(messages_key, index)
            if not data:
                return None
            return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to retrieve message at index {index}: {e}")
            return None
    
    def get_all_messages(self, TenantId: str, SessionId: str) -> List[Dict]:
        
        self.validate_tenant_session(TenantId, SessionId)
        messages_key = self.get_messages_key(TenantId, SessionId)
        
        try:
            raw_messages = self.redis_client.lrange(messages_key, 0, -1)        
            if not raw_messages:
                return []
            
            messages = [json.loads(msg) for msg in raw_messages]
            logger.debug(f"Retrieved {len(messages)} messages")
            return messages
            
        except Exception as e:
            logger.error(f"Failed to retrieve messages: {e}")
            return []
    
    def get_context_for_ai(
        self, 
        TenantId: str, 
        SessionId: str
    ) -> Tuple[Optional[List[Dict]], List[Dict]]:
    
        all_messages = self.get_all_messages(TenantId, SessionId)
        
        if not all_messages:
            logger.warning("No messages found")
            return None, []
        
        last_system_data = None
        for msg in reversed(all_messages):
            if msg["role"] == "system":
                try:
                    data_arr = json.loads(msg["content"])
                    if isinstance(data_arr, list) and len(data_arr) > 0:
                        last_system_data = data_arr
                        logger.debug(f"Found system data: {len(last_system_data)} rows")
                        break
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse system JSON: {e}")
                    continue
        
        if not last_system_data:
            logger.warning("No valid system data found")
            return None, []
        
        user_messages = [m for m in all_messages if m["role"] == "user"][-2:]
        ai_messages = [m for m in all_messages if m["role"] == "assistant"][-2:]
        
        conversation = []
        
        selected_indexes = {
            m["index"]
            for m in (user_messages + ai_messages)
            if "index" in m
        }

        conversation = []
        for msg in all_messages:
            if msg["role"] in {"user", "assistant"} and msg["index"] in selected_indexes:
                conversation.append({
                    "role": msg["role"],
                    "content": msg["content"]
                })
        
        return last_system_data, conversation
    
    def get_data_by_index(
        self, 
        TenantId: str, 
        SessionId: str, 
        index: int
    ) -> Optional[Dict[str, Any]]:
        
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
        
        if message["role"] == "system":
            try:
                result["data"] = json.loads(message["content"])
            except json.JSONDecodeError:
                result["data"] = None
        
        return result
    
    
    def delete_session(self, TenantId: str, SessionId: str) -> int:
        
        self.validate_tenant_session(TenantId, SessionId)
        pattern = f"ivpsql:{TenantId}:session:{SessionId}:*"
        
        try:
            deleted_count = 0
            cursor = 0
            
            while True:
                cursor, keys = self.redis_client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=100
                )
                
                if keys:
                    deleted_count += self.redis_client.delete(*keys)
                
                if cursor == 0:
                    break
        
            session_activity_key = f"ivpsql:{TenantId}:metadata:session:{SessionId}:activity"
            self.redis_client.delete(session_activity_key)
            
            logger.info(f"Deleted {deleted_count} keys for session {SessionId}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to delete session: {e}")
            raise
        
    def get_tenant_sessions(self, TenantId: str) -> List[str]:
        
        if not TenantId or not TenantId.strip():
            raise ValueError("TenantId is required")
        
        pattern = f"ivpsql:{TenantId}:session:*:messages"
        
        try:
            sessions = set()
            cursor = 0
            
            while True:
                cursor, keys = self.redis_client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=100
                )
                
                for key in keys:
                    parts = key.split(":")
                    if len(parts) >= 4:
                        sessions.add(parts[3]) 
                
                if cursor == 0:
                    break
            
            return list(sessions)
            
        except Exception as e:
            logger.error(f"Failed to get tenant sessions: {e}")
            return []
    
    def get_active_sessions_key(self, tenant_id: str) -> str:
        
        return f"ivpsql:{tenant_id}:metadata:active_sessions"
    
    def incr_active_sessions(self, tenant_id: str) -> int:
        
        key = self.get_active_sessions_key(tenant_id)
        count = self.redis_client.incr(key)
        self.redis_client.expire(key, 86400)
        logger.debug(f"[REDIS] Incremented active sessions for {tenant_id}: {count}")
        return count
    
    def decr_active_sessions(self, tenant_id: str) -> int:
        
        key = self.get_active_sessions_key(tenant_id)
        count = self.redis_client.decr(key)
        
        if count < 0:
            self.redis_client.set(key, 0)
            count = 0

        if count > 0:
            self.redis_client.expire(key, 86400)
        else:
            self.redis_client.delete(key)
        
        logger.debug(f"[REDIS] Decremented active sessions for {tenant_id}: {count}")
        return count
    
    def get_active_sessions(self, tenant_id: str) -> int:
        
        key = self.get_active_sessions_key(tenant_id)
        count = self.redis_client.get(key)
        return int(count) if count else 0
    
    
    def get_session_keys(self, tenant_id: str) -> list:
        
        pattern = f"ivpsql:{tenant_id}:metadata:session:*:activity"
        
        try:
            keys = []
            cursor = 0
            
            while True:
                cursor, matched_keys = self.redis_client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=100
                )
                keys.extend(matched_keys)
                
                if cursor == 0:
                    break

            session_ids = []
            for key in keys:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                parts = key_str.split(':')
                if len(parts) >= 5:
                    session_ids.append(parts[4])  
            
            return session_ids
            
        except Exception as e:
            logger.error(f"Failed to get session keys: {e}")
            return []
    
    def get_session_last_activity(self, tenant_id: str, session_id: str) -> Optional[float]:
        
        key = f"ivpsql:{tenant_id}:metadata:session:{session_id}:activity"
        timestamp = self.redis_client.get(key)
        
        if timestamp:
            return float(timestamp)
        return None
    
    def set_session_activity(self, tenant_id: str, session_id: str):

        key = f"ivpsql:{tenant_id}:metadata:session:{session_id}:activity"
        timestamp = time.time()
        
        self.redis_client.set(key, timestamp, ex=86400)  # 24 hour expiration
        logger.debug(f"Updated session activity: {tenant_id}:{session_id}")
    
    def remove_session(self, tenant_id: str, session_id: str):
   
        key = f"ivpsql:{tenant_id}:metadata:session:{session_id}:activity"
        deleted = self.redis_client.delete(key)
        
        if deleted:
            logger.debug(f"Removed session activity tracking: {tenant_id}:{session_id}")
    
    def get_tenant_last_activity(self, tenant_id: str) -> Optional[float]:

        key = f"ivpsql:{tenant_id}:metadata:last_activity"
        value = self.redis_client.get(key)
        return float(value) if value else None
    
    def update_tenant_activity(self, tenant_id: str):

        key = f"ivpsql:{tenant_id}:metadata:last_activity"
        timestamp = time.time()
        self.redis_client.set(key, timestamp, ex=86400) 
        logger.debug(f"[REDIS] Updated activity for tenant: {tenant_id}")
    
    def clear_tenant_activity(self, tenant_id: str):
 
        try:
            # Remove tenant-level activity
            self.redis_client.delete(f"ivpsql:{tenant_id}:metadata:last_activity")
            
            logger.debug(f"[REDIS] Cleared activity metadata for tenant: {tenant_id}")
        except Exception as e:
            logger.error(f"[REDIS] Failed to clear tenant activity: {e}")
    
    def delete_all_tenant_data(self, tenant_id: str) -> int:
 
        if not tenant_id or not tenant_id.strip():
            raise ValueError("TenantId is required")
        
        try:
            total_deleted = 0
            
            pattern = f"ivpsql:{tenant_id}:*"
            
            cursor = 0
            while True:
                cursor, keys = self.redis_client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=100
                )
                
                if keys:
                    total_deleted += self.redis_client.delete(*keys)
                
                if cursor == 0:
                    break
            
            logger.info(f"[REDIS] Deleted all data for tenant {tenant_id}: "
                       f"{total_deleted} keys removed")
            return total_deleted
            
        except Exception as e:
            logger.error(f"[REDIS] Failed to delete tenant sessions: {e}")
            raise
    
    def close(self):
    
        try:
            self.redis_client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.warning(f"Failed to close Redis connection: {e}")
    
    @classmethod
    def close_all_pools(cls):
    
        for pool_key, pool in cls._connection_pools.items():
            try:
                pool.disconnect()
                logger.info(f"Closed Redis pool: {pool_key}")
            except Exception as e:
                logger.warning(f"Failed to close pool {pool_key}: {e}")
        cls._connection_pools.clear()
