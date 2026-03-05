from datetime import datetime
from typing import Dict
from src.config.field_constant import FIELD_TYPES, FIELD_ALIASES, CANON_VALUES, MONTH_NAMES, DEFAULT_SELECT_FIELDS
from src.utils.utils import safe_json_from_model, get_zone, extract_month_and_year,retry_with_backoff
from src.config.db_config import DatabaseConfig
from src.models.model_service import ModelService
from src.ai.promt.sql_query_promt import build_sql_prompt
import logging

logger = logging.getLogger(__name__)

class SQLQueryGenerator:
    
    PURPOSE = "Technical"
       
    def __init__(self, config: DatabaseConfig, model_service:ModelService ):
        
        self.table_name = config.full_table_name
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)

    def _safe_generate(self, prompt: str, TenantId: str, SessionId: str) -> str:
        text = self.model_service.generate(self.PURPOSE, prompt)
        if not text or not text.strip():
            raise ValueError(f"[tenant={TenantId}, session={SessionId}] Empty response received from model.")
        return text
    
    def generate_sql(self, user_query: str, TenantId: str, SessionId: str) -> Dict:
        if not self.enabled:  
            raise RuntimeError(f"[SQL_GEN] Purpose '{self.PURPOSE}' not enabled for tenant={TenantId}")
       
        if not TenantId or TenantId.strip() == "":
            raise ValueError("TenantId is required and cannot be empty")
        
        detected_month, detected_year = extract_month_and_year(user_query)
        
        schema_context = {
            "table_name": self.table_name,
            "field_types": FIELD_TYPES,
            "field_aliases": FIELD_ALIASES,
            "canonical_values": CANON_VALUES,
            "default_select_fields": DEFAULT_SELECT_FIELDS
        }
        
        now = datetime.now(get_zone())
        date_hint = ""
        explicit_filter = None
        
        if detected_month:
            year = detected_year or now.year
            month_name = list(MONTH_NAMES.keys())[detected_month - 1].title()
            start_date = f"{year}-{detected_month:02d}-01"
            end_date = f"{year}-{detected_month+1:02d}-01" if detected_month < 12 else f"{year+1}-01-01"
            explicit_filter = f"InvoiceDate >= '{start_date}' AND InvoiceDate < '{end_date}'"
            date_hint = f"\n MONTH DETECTED: {month_name} {year}\n USE THIS EXACT FILTER: {explicit_filter}"
                    
        prompt = build_sql_prompt(
            table_name=self.table_name,
            TenantId=TenantId,
            schema_context=schema_context,
            user_query=user_query,
            now=now,
            DEFAULT_SELECT_FIELDS=DEFAULT_SELECT_FIELDS,
            explicit_filter=explicit_filter,
            date_hint=date_hint,
        )
        
        try:
            text = retry_with_backoff(
                lambda: self._safe_generate(prompt, TenantId, SessionId),
                max_retries=3,
                initial_delay=1,)
            
            result = safe_json_from_model(text)

            if TenantId not in result.get('sql', ''):
                raise RuntimeError(f"Generated SQL missing required TenantId filter: {TenantId}")                
            logger.info(f"[SQL_GEN][tenant={TenantId}][session={SessionId}] Success. SQL: {result.get('sql', '')}")     
            return result       
              
        except Exception as e:
            logger.error(f"[SQL_GEN][tenant={TenantId}][session={SessionId}] Failed: {e}", exc_info=True)
            raise RuntimeError(f"SQL generation failed: {e}")
            