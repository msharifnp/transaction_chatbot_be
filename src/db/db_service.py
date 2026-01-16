
from typing import List, Dict, Optional, Tuple
from src.config.db_config import DatabaseConfig
import psycopg2
from decimal import Decimal


class DatabaseService:
    """PostgreSQL database service for invoice queries."""
    
    def __init__(self, config: DatabaseConfig):
        """Initialize database service."""
        self.config = config
        self.connection = None
        self._connect()
    
    def _connect(self) -> None:
        """Establish database connection."""
        try:
            if self.connection is not None:
                # If already connected and not closed, reuse it
                try:
                    if self.connection.closed == 0:
                        return
                except AttributeError:
                    # If connection object doesn't have .closed, ignore and reconnect
                    pass
                
            print(f"[DB] Connecting to PostgreSQL at {self.config.host}:{self.config.port}:{self.config.database}...")
            
            self.connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                connect_timeout=self.config.timeout
            )
            # autocommit mode (optional but safe for SELECT-only workloads)
            self.connection.autocommit = True
            
            print("[DB] Connection established")
        except psycopg2.Error as e:
            print(f"[DB] Connection failed: {e}")
            self.connection = None
            
     
    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict]:
        """
        Execute SQL query and return results as list of dictionaries.
        
        Args:
            sql: SQL query string
            params: Query parameters (optional)
            
        Returns:
            List of row dictionaries
        """
        try:
            if self.connection is None or getattr(self.connection, "closed",False):
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
            
            # Get column names
            columns = [column[0] for column in cursor.description]
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            
            # Convert to list of dictionaries
            results = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    # Handle datetime serialization
                    if hasattr(value, 'isoformat'):
                        row_dict[columns[i]] = value.isoformat()
                    elif isinstance(value,Decimal):
                        row_dict[columns[i]] = float(value)
                    else:
                        row_dict[columns[i]] = value
                results.append(row_dict)
            
            cursor.close()
            
            print(f"[DB] ✅ Query returned {len(results)} rows")
                      
            return results
            
        except psycopg2.Error as e:
            print(f"[DB] ❌ Query execution failed: {e}")
            print(f"[DB] SQL: {sql}")
            raise
    
    def close(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            print("[DB] Connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()