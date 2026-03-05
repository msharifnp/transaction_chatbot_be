from typing import List, Dict, Optional, Tuple
from src.config.db_config import DatabaseConfig
import psycopg2
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

class DatabaseService:
        
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self._connect()
    
    def _connect(self) -> None:
        try:
            if self.connection is not None:
                try:
                    if self.connection.closed == 0:
                        return
                except AttributeError:
                    pass
                
            logger.info(f"Connecting to PostgreSQL at {self.config.host}:{self.config.port}:{self.config.database}")
            
            self.connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                connect_timeout=self.config.timeout
            )
            
            self.connection.autocommit = True
            
            logger.info("Database Connection established")
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            self.connection = None
            
     
    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict]:
        try:
            if self.connection is None or getattr(self.connection, "closed",False):
                raise RuntimeError("Database connection is not available")
        
            cursor = self.connection.cursor()
            
            logger.info("Database Executing SQL:")
           
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            columns = [column[0] for column in cursor.description]
            
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    if hasattr(value, 'isoformat'):
                        row_dict[columns[i]] = value.isoformat()
                    elif isinstance(value,Decimal):
                        row_dict[columns[i]] = float(value)
                    else:
                        row_dict[columns[i]] = value
                results.append(row_dict)
            
            cursor.close()
            
            logger.info(f"Database Query returned {len(results)} rows")
                      
            return results
            
        except psycopg2.Error as e:
            logger.error(f"Database Query execution failed: {e}")
            logger.info(f"  SQL: {sql}")
            raise
        
        
    def execute_update(self, sql: str, params: Optional[Tuple] = None) -> int:    
        try:
            if self.connection is None or getattr(self.connection, "closed", False):
                raise RuntimeError("Database connection is not available")
        
            cursor = self.connection.cursor()
            
            logger.info(f"Database Executing SQL:")
            logger.info(f"Database {sql}")
            if params:
                logger.info(f"Database Params: {params}")
            
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            affected_rows = cursor.rowcount
          
            cursor.close()
            
            logger.info(f"Database Updated {affected_rows} row(s)")
                    
            return affected_rows
            
        except psycopg2.Error as e:
            logger.error(f"Database Query execution failed: {e}")
            logger.info(f"Database SQL: {sql}")
            raise
        
    
    def close(self) -> None:
        if self.connection:
            self.connection.close()
            logger.info("Database Connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()