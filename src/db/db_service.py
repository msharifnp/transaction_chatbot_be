
# from typing import List, Dict, Optional, Tuple
# from src.config.db_config import DatabaseConfig
# import oracledb
# from decimal import Decimal

# class DatabaseService:
#     """Oracle database service for queries."""

#     def __init__(self, config: DatabaseConfig):
#         """Initialize database service."""
#         self.config = config
#         self.connection = None
#         self._connect()

#     def _connect(self) -> None:
#         """Establish Oracle database connection."""
#         try:
#             if self.connection is not None:
#                 try:
#                     if self.connection.ping() is None:
#                         return
#                 except Exception:
#                     pass

#             # Check if database config is in Easy Connect format (host:port/service)
#             if ':' in self.config.database and '/' in self.config.database:
#                 # Easy Connect format: "host:port/service"
#                 dsn = self.config.database
#                 print(f"[DB] Connecting to Oracle using Easy Connect: {dsn}...")
#             else:
#                 # Traditional format: separate host, port, service
#                 print(f"[DB] Connecting to Oracle at {self.config.host}:{self.config.port}/{self.config.database}...")
#                 dsn = oracledb.makedsn(self.config.host, self.config.port, service_name=self.config.database)
            
#             self.connection = oracledb.connect(
#                 user=self.config.user,
#                 password=self.config.password,
#                 dsn=dsn
#             )

#             print("[DB] Connection established")
#         except oracledb.Error as e:
#             print(f"[DB] Connection failed: {e}")
#             self.connection = None

#     def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict]:
#         """
#         Execute SQL query and return results as list of dictionaries.
#         """
#         try:
#             if self.connection is None:
#                 raise RuntimeError("Database connection is not available")

#             cursor = self.connection.cursor()

#             print(f"\n[DB] Executing SQL:")
#             print(f"[DB] {sql}")
#             if params:
#                 print(f"[DB] Params: {params}")

#             if params:
#                 cursor.execute(sql, params)
#             else:
#                 cursor.execute(sql)

#             columns = [col[0] for col in cursor.description]
#             rows = cursor.fetchall()

#             results = []
#             for row in rows:
#                 row_dict = {}
#                 for i, value in enumerate(row):
#                     if hasattr(value, 'isoformat'):
#                         row_dict[columns[i]] = value.isoformat()
#                     elif isinstance(value, Decimal):
#                         row_dict[columns[i]] = float(value)
#                     else:
#                         row_dict[columns[i]] = value
#                 results.append(row_dict)

#             cursor.close()
#             print(f"[DB] ✅ Query returned {len(results)} rows")
#             return results

#         except oracledb.Error as e:
#             print(f"[DB] ❌ Query execution failed: {e}")
#             print(f"[DB] SQL: {sql}")
#             raise

#     def execute_update(self, sql: str, params: Optional[Tuple] = None) -> int:
#         """
#         Execute UPDATE/INSERT/DELETE query without returning data.
#         """
#         try:
#             if self.connection is None:
#                 raise RuntimeError("Database connection is not available")

#             cursor = self.connection.cursor()

#             print(f"\n[DB] Executing SQL:")
#             print(f"[DB] {sql}")
#             if params:
#                 print(f"[DB] Params: {params}")
#             if params:
#                 cursor.execute(sql, params)
#             else:
#                 cursor.execute(sql)

#             affected_rows = cursor.rowcount
#             self.connection.commit()
#             cursor.close()
#             print(f"[DB] ✅ Updated {affected_rows} row(s)")
#             return affected_rows

#         except oracledb.Error as e:
#             print(f"[DB] ❌ Query execution failed: {e}")
#             print(f"[DB] SQL: {sql}")
#             raise

#     def close(self) -> None:
#         """Close database connection."""
#         if self.connection:
#             self.connection.close()
#             print("[DB] Connection closed")

#     def __enter__(self):
#         return self

#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.close()



















from typing import List, Dict, Optional, Tuple
from src.config.db_config import DatabaseConfig
import oracledb
from decimal import Decimal

class DatabaseService:
    """Oracle database service with connection pooling."""
    
    _pool = None
    _pool_config = None

    def __init__(self, config: DatabaseConfig):
        """Initialize database service."""
        self.config = config
        self.connection = None
        self._connect()

    def _connect(self) -> None:
        """Establish Oracle database connection using connection pool."""
        try:
            # Create pool once (class-level singleton)
            if DatabaseService._pool is None:
                # Check if database config is in Easy Connect format (host:port/service)
                if ':' in self.config.database and '/' in self.config.database:
                    # Easy Connect format: "host:port/service"
                    dsn = self.config.database
                    print(f"[DB POOL] Creating connection pool for Oracle: {dsn}")
                else:
                    # Traditional format: separate host, port, service
                    dsn = oracledb.makedsn(self.config.host, self.config.port, service_name=self.config.database)
                    print(f"[DB POOL] Creating connection pool for Oracle: {self.config.host}:{self.config.port}/{self.config.database}")
                
                DatabaseService._pool = oracledb.create_pool(
                    user=self.config.user,
                    password=self.config.password,
                    dsn=dsn,
                    min=2,          # Minimum connections in pool
                    max=10,         # Maximum connections in pool
                    increment=1,    # Increment when pool needs to grow
                    getmode=oracledb.POOL_GETMODE_WAIT,  # Wait if no connection available
                    timeout=30      # Wait timeout in seconds
                )
                DatabaseService._pool_config = {
                    'dsn': dsn,
                    'min': 2,
                    'max': 10
                }
                print(f"[DB POOL] ✅ Connection pool created (min=2, max=10)")
            
            # Acquire connection from pool
            if self.connection is None:
                self.connection = DatabaseService._pool.acquire()
                print(f"[DB POOL] ✅ Connection acquired from pool (open={DatabaseService._pool.opened}, busy={DatabaseService._pool.busy})")
                
        except oracledb.Error as e:
            print(f"[DB POOL] ❌ Connection failed: {e}")
            self.connection = None

    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict]:
        """
        Execute SQL query and return results as list of dictionaries.
        """
        try:
            if self.connection is None:
                raise RuntimeError("Database connection is not available")

            cursor = self.connection.cursor()

            print(f"\n[DB] Executing SQL:")
            print(f"[DB] {sql}")
            if params:
                print(f"[DB] Params: {params}")

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

            results = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    if hasattr(value, 'isoformat'):
                        row_dict[columns[i]] = value.isoformat()
                    elif isinstance(value, Decimal):
                        row_dict[columns[i]] = float(value)
                    else:
                        row_dict[columns[i]] = value
                results.append(row_dict)

            cursor.close()
            print(f"[DB] ✅ Query returned {len(results)} rows")
            return results

        except oracledb.Error as e:
            print(f"[DB] ❌ Query execution failed: {e}")
            print(f"[DB] SQL: {sql}")
            raise

    def execute_update(self, sql: str, params: Optional[Tuple] = None) -> int:
        """
        Execute UPDATE/INSERT/DELETE query without returning data.
        """
        try:
            if self.connection is None:
                raise RuntimeError("Database connection is not available")

            cursor = self.connection.cursor()

            print(f"\n[DB] Executing SQL:")
            print(f"[DB] {sql}")
            if params:
                print(f"[DB] Params: {params}")
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            affected_rows = cursor.rowcount
            self.connection.commit()
            cursor.close()
            print(f"[DB] ✅ Updated {affected_rows} row(s)")
            return affected_rows

        except oracledb.Error as e:
            print(f"[DB] ❌ Query execution failed: {e}")
            print(f"[DB] SQL: {sql}")
            raise

    def close(self) -> None:
        """Release connection back to pool."""
        if self.connection:
            self.connection.close()
            if DatabaseService._pool:
                print(f"[DB POOL] Connection released (open={DatabaseService._pool.opened}, busy={DatabaseService._pool.busy})")
            else:
                print("[DB] Connection closed")
            self.connection = None

    @classmethod
    def close_pool(cls):
        """Close the entire connection pool (call on application shutdown)."""
        if cls._pool:
            cls._pool.close()
            print("[DB POOL] ✅ Connection pool closed")
            cls._pool = None
            cls._pool_config = None

    @classmethod
    def get_pool_stats(cls) -> Dict:
        """Get current pool statistics."""
        if cls._pool:
            return {
                'opened': cls._pool.opened,
                'busy': cls._pool.busy,
                'min': cls._pool_config.get('min', 0),
                'max': cls._pool_config.get('max', 0),
                'dsn': cls._pool_config.get('dsn', 'Unknown')
            }
        return {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()