from decimal import Decimal
import logging
from threading import Lock
from typing import Dict, List, Optional, Tuple
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from src.config.db_config import DatabaseConfig

logger = logging.getLogger(__name__)


class DatabaseService:
    _pools: Dict[str, ThreadedConnectionPool] = {}
    _lock = Lock()

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool: Optional[ThreadedConnectionPool] = None
        self._pool_key = (
            f"{self.config.host}:{self.config.port}:"
            f"{self.config.database}:{self.config.user}"
        )
        self._init_pool()

    def _init_pool(self) -> None:
        try:
            with self._lock:
                if self._pool_key not in self._pools:
                    logger.info(
                        "[DB] Initializing connection pool %s/%s",
                        f"{self.config.host}:{self.config.port}",
                        self.config.database,
                    )
                    self._pools[self._pool_key] = ThreadedConnectionPool(
                        minconn=1,
                        maxconn=10,
                        host=self.config.host,
                        port=self.config.port,
                        database=self.config.database,
                        user=self.config.user,
                        password=self.config.password,
                        connect_timeout=self.config.timeout,
                    )
                    logger.info("[DB] Connection pool created successfully")

                self.pool = self._pools[self._pool_key]
        except psycopg2.Error as e:
            logger.error("[DB] Pool initialization failed: %s", e)
            self.pool = None
            raise

    def _get_connection(self):
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        return self.pool.getconn()

    def _release_connection(self, conn) -> None:
        if self.pool and conn:
            self.pool.putconn(conn)
            
    
    def _serialize_row(self, columns: List[str], row: Tuple) -> Dict:
        row_dict = {}
        for i, value in enumerate(row):
            if hasattr(value, "isoformat"):
                row_dict[columns[i]] = value.isoformat()
            elif isinstance(value, Decimal):
                row_dict[columns[i]] = float(value)
            else:
                row_dict[columns[i]] = value
        return row_dict

    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict]:
        conn = None
        cursor = None

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            logger.info("[DB] Executing query")

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            if cursor.description is None:
                affected_rows = cursor.rowcount
                conn.commit()
                logger.info("[DB] Write query affected %s row(s)", affected_rows)
                return []

            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            conn.commit()

            results = [self._serialize_row(columns, row) for row in rows]

            logger.info("[DB] Returned %s row(s)", len(results))
            return results
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            logger.error("[DB] Query failed: %s", e)
            logger.error("[DB] SQL: %s", sql)
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self._release_connection(conn)


    def close(self) -> None:
        with self._lock:
            pool = self._pools.pop(self._pool_key, None)
            if pool:
                pool.closeall()
                logger.info("[DB] Connection pool closed for %s", self._pool_key)
        self.pool = None

    @classmethod
    def close_all_pools(cls) -> None:
        with cls._lock:
            for pool_key, pool in cls._pools.items():
                try:
                    pool.closeall()
                    logger.info("[DB] Closed connection pool: %s", pool_key)
                except Exception as exc:
                    logger.warning("[DB] Failed to close pool %s: %s", pool_key, exc)
            cls._pools.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
