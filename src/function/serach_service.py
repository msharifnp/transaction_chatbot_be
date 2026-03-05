import json
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Any
from fastapi import HTTPException, status
import logging
import base64

logger = logging.getLogger(__name__)

from src.schemas.schemas import (
    DatabaseResponseWrapper,
    MessageResponseWrapper,
    ChatResponseWrapper,
    HybridResponseWrapper,
    UnifiedSearchRequest
)
from src.utils.utils import (
    safe_serialize,
    is_refresh_request,
    get_last_real_user_query,
    extract_additional_context
)
from src.config.db_config import Config as DatabaseConfig
from src.config.redis_config import Config as RedisConfig
from src.ai.query_router import QueryRouter
from src.ai.forecast import ForecastService
from src.ai.chart import ChartService
from src.ai.summary import SummaryService
from src.ai.general_qa import QAService
from src.db.redis_service import RedisService
from src.ai.sql_query_generator import SQLQueryGenerator
from src.db.db_service import DatabaseService
from src.config.startup import model_startup 


class SearchService:
         
    MAX_UI_ROWS = 5
    
    def __init__(self, tenant_id: str = None):
        self.tenant_id = tenant_id
        self.db_config = DatabaseConfig.get_database_config()
        self.redis_config = RedisConfig.get_redis_config()
        self._init_services()
        self.router = QueryRouter(model_service=self.model_service)  

    
    def _init_services(self):
        
        if self.tenant_id:
            self.model_service = model_startup.get_or_create_service(self.tenant_id)
        self.forecast_service = ForecastService(self.model_service)
        self.chart_service = ChartService(self.model_service)
        self.summary_service = SummaryService(self.model_service)
        self.QA_service = QAService(self.model_service)
        self.sql_generator = SQLQueryGenerator(config=self.db_config, model_service=self.model_service)
        self.db_service = DatabaseService(self.db_config)        
        self.redis_service = RedisService(self.redis_config)
        self.db_settings = self.db_config
    
    @property
    def use_gemini(self) -> bool:
        return self.model_service.is_available()
    
    def search(self, query: str, TenantId: str, SessionId: str ) -> Dict[str, Any]:
        
        self.redis_service.validate_tenant_session(TenantId, SessionId)
        
        self.redis_service.store_message(
            TenantId=TenantId,
            SessionId=SessionId,
            role="user",
            content=query,
            metadata={"source": "database_search"}
        )

        logger.info(" Generating SQL Query with tenant filter...")
        try:
            sql_result = self.sql_generator.generate_sql(query, TenantId=TenantId, SessionId=SessionId)
            sql = sql_result.get("sql")
        except Exception:
            logger.error("Error generating SQL query", exc_info=True)
            return {
                "ok": False,
                "error_type": "SQL_GENERATION_FAILED",
                "user_message":(
                    "Failed to generate database query."
                    "Please try again with accurate prompt."
                )       
            }
        
        if not sql or TenantId not in sql:
            logger.error("Generated SQL query is invalid or missing tenant filter")
            return {
                "ok": False,
                "error_type": "SQL_GENERATION_FAILED",
                "user_message":(
                    "Failed to generate database query."
                    "Please try again with accurate prompt."
                )       
            }
        
        logger.info(" Executing on PostgreSQL...")
        try:
            rows = self.db_service.execute_query(sql)
            count = len(rows)
            logger.info(f"[RESULT] Retrieved {count} rows for tenant:{TenantId}")
        except Exception as e:
            logger.error(f"Error executing SQL query: {e}")
            return {
                "ok": False,
                "error_type": "DB_EXECUTION_FAILED",
                "user_message":(
                    "Failed to execute database query."
                    "Please try again with accurate prompt."
                )
            }

        system_index = self.redis_service.store_message(
            TenantId=TenantId,
            SessionId=SessionId,
            role="system",
            content=json.dumps(rows, default=str),
            metadata={
                "query": query,
                "row_count": count,
                "TenantId": TenantId,
                "timestamp": datetime.now().isoformat()
            }
        )
              
        return {
            "rows": rows,
            "count": count,
            "index": system_index
        }
    
    
    def process_model_query(self, TenantId: str, SessionId: str, query: str, intent: str) -> Dict[str, Any]:

        self.redis_service.validate_tenant_session(TenantId, SessionId)

        try:
            self.redis_service.store_message(
                TenantId=TenantId,
                SessionId=SessionId,
                role="user",
                content=query,
                metadata={"intent": intent, "source": "ai_chat"}
            )

            system_data, _ = self.redis_service.get_context_for_ai(
                TenantId, SessionId
            )
            logger.info(f"[AI MODE] Processing with {len(system_data)} stored rows")


            if intent == "summary":
                analysis = self.summary_service.generate_summary(
                    user_query=query,
                    rows=system_data,
                    TenantId=TenantId,
                    SessionId=SessionId
                )

                index = self.redis_service.store_message(
                    TenantId=TenantId,
                    SessionId=SessionId,
                    role="assistant",
                    content=analysis,
                    metadata={"intent": "summary"}
                )

                return {
                    "analysis_text": {
                        "text":analysis,
                        "index": index
                    },
                    "chart":None
                }

            elif intent == "forecast":
                forecast_result = self.forecast_service.generate_forecast(
                    user_query=query,
                    rows=system_data,
                    TenantId=TenantId,
                    SessionId=SessionId   
                )
                
                forecast_text= forecast_result["text"]
                forecast_rows = forecast_result["forecast_rows"]

                forecast_index = self.redis_service.store_message(
                    TenantId=TenantId,
                    SessionId=SessionId,
                    role="assistant",
                    content=forecast_text,
                    metadata={"intent": "forecast", "type": "text"}
                )

                chart = self.forecast_service.generate_forecast_chart(
                    user_query= query,
                    forecast_rows= forecast_rows,
                    TenantId=TenantId,
                    SessionId=SessionId
                )

                chart_index = None
                if chart and not chart.get("error"):
                    svg = base64.b64decode(chart["image_b64"]).decode("utf-8")

                    chart_index = self.redis_service.store_message(
                        TenantId=TenantId,
                        SessionId=SessionId,
                        role="assistant",
                        content=svg,
                        metadata={
                            "intent": "forecast",
                            "type": "chart",
                            "image_mime": "image/svg+xml"
                        }
                    )

                return {
                    "analysis_text": {
                        "text":forecast_text,
                        "index": forecast_index
                    },
                    "chart":{
                        "svg":svg,
                        "index":chart_index
                    } if chart_index else None               
                }
               
            elif intent == "chart":
                chart = self.chart_service.generate_chart(
                    user_query=query,
                    rows=system_data,
                    TenantId=TenantId,
                    SessionId=SessionId
                )

                if not chart or chart.get("error"):
                    content = "Failed to generate chart"
                    metadata = {"intent": "chart", "error": True}
                else:
                    content = base64.b64decode(chart["image_b64"]).decode("utf-8")
                    metadata = {
                        "intent": "chart",
                        "type": "chart",
                        "image_mime": "image/svg+xml"
                    }

                index = self.redis_service.store_message(
                    TenantId=TenantId,
                    SessionId=SessionId,
                    role="assistant",
                    content=content,
                    metadata=metadata
                )

                return {
                    "analysis_text": None,
                    "chart":{
                        "svg":content,
                        "index":index
                    }
                }

            else:
                analysis = self.QA_service.generate_general_qa(
                    user_query=query,
                    rows= system_data,
                    TenantId=TenantId,
                    SessionId=SessionId
                )

                index = self.redis_service.store_message(
                    TenantId=TenantId,
                    SessionId=SessionId,
                    role="assistant",
                    content=analysis,
                    metadata={"intent": "general_qa"}
                )

                return {
                    "analysis_text": {
                        "text": analysis,
                        "index": index
                    },
                    "chart":None
                }

        except Exception as e:
            logger.error(f"[AI MODE]  Error: {e}")
            traceback.print_exc()

            index = self.redis_service.store_message(
                TenantId=TenantId,
                SessionId=SessionId,
                role="assistant",
                content=f"Error: {str(e)}",
                metadata={"intent": intent, "error": True}
            )

            return {
                "ok": False,
                "error_type": "AI_INTERNAL_ERROR",
                "user_message": (
                    "I'm having trouble processing this request right now. "
                    "Please try again in a few moments."
                ),
                "analysis_text": None,
                "chart": None
                
            }

    
    def unified_search(self, req: UnifiedSearchRequest, session_id: str) -> Any:

        try:

            if not self.use_gemini:
                return MessageResponseWrapper(
                    success=False,
                    code=503,
                    message="Service temporarily unavailable",
                    errors=["AI_UNAVAILABLE"],
                    data={
                        "response_type": "message",
                        "response_message": "AI service is temporarily unavailable."
                    }
                )
            
            TenantId = self.tenant_id
            query = (req.query or "").strip()
            
            if not query:
                raise HTTPException(status_code=400, detail="Query cannot be empty")
            
            if is_refresh_request(query):
                logger.info("[REFRESH] User requested fresh data (YES/OK detected).")
                
                all_messages = self.redis_service.get_all_messages(TenantId, session_id) 
                last_user_query = get_last_real_user_query(all_messages)
                
                if not last_user_query:
                    logger.info("[REFRESH] No previous user query found to refresh.")
                    return MessageResponseWrapper(
                        success=True,
                        code=200,
                        message="Nothing to refresh.",
                        data={
                            "response_type": "message",
                            "response_message": (
                                "I couldn't find a previous invoice query to refresh. "
                                "Please ask me a question about your invoices to get started!"
                            ),
                        },
                    )
                
                additional_context = extract_additional_context(query)

                if additional_context:
                    combined_query = f"{last_user_query} {additional_context}"
                    logger.info(f"[REFRESH] Last query: {last_user_query!r}")
                    logger.info(f"[REFRESH] Additional context: {additional_context!r}")
                    logger.info(f"[REFRESH] Combined query: {combined_query!r}")
                else:
                    combined_query = last_user_query
                    logger.info(f"[REFRESH] No additional context, using: {combined_query!r}")
                
                result = self.search(
                    query=combined_query,
                    TenantId=TenantId,
                    SessionId=session_id,
                )
                   
                if result.get("error_type"):
                    return MessageResponseWrapper(
                        success=False,
                        code=503,
                        message="Database temporarily unavailable",
                        errors=[result["error_type"]],
                        data={
                            "response_type": "message",
                            "response_message": result.get("user_message", "Database error occurred.")
                        }
                    )
                
                rows = result["rows"]
                count = result["count"]
                index = result["index"]
                
                logger.info(f"[RESULT] Retrieved {count} rows, stored at index {index}")
                
                if not rows:
                    return DatabaseResponseWrapper(
                        success=True,
                        code=200,
                        message="No data found, please try again.",
                        data={
                            "response_type": "database",
                            "columns": [],
                            "rows": [],
                            "count": 0,
                            "index": index,
                        },
                    )
                
                columns = list(rows[0].keys())
                ui_rows = [
                    safe_serialize({col: row.get(col) for col in columns})
                    for row in rows[:self.MAX_UI_ROWS]
                ]
                
                return DatabaseResponseWrapper(
                    success=True,
                    code=200,
                    message="Database data fetched successfully.",
                    data={
                        "response_type": "database",
                        "columns": columns,
                        "rows": ui_rows,
                        "count": count,
                        "index": index,
                    },
                )
            
            logger.info("Analyzing query for intelligent routing...")
            
            routing_decision = self.router.intelligent_route(query, TenantId, session_id)
            if routing_decision.get("router_error"):
                error_detail = routing_decision.get("router_error_detail")
                logger.error(f"[ROUTER]  Router failure: {error_detail}")

                return MessageResponseWrapper(
                    success=False,
                    code=503,
                    message="Service temporarily unavailable",
                    data={
                        "response_type": "message",
                        "response_message": (
                            "The AI model is temporarily unavailable. Please try again later."
                            )
                    },
                    errors=["ROUTER_ERROR"]
                )
            
            mode = routing_decision["mode"]
            reasoning = routing_decision["reasoning"]
            
            logger.info(f"[ROUTER] Decision: {mode.upper()}")
            logger.info(f"[ROUTER] Reasoning: {reasoning}")

            if mode == "message":
                return MessageResponseWrapper(
                    success=True,
                    code=200,
                    message="Message fetched successfully.",
                    data={
                        "response_type": "message",
                        "response_message": routing_decision.get(
                            "message",
                            "Hello! I'm here to help with invoice data. How can I assist you?"
                        )
                    }
                )

            elif mode == "database":
                logger.info(" Executing DATABASE search...")
                
                refined_query = routing_decision.get("refined_query", query)
                
                result = self.search(
                    query=refined_query,
                    TenantId=TenantId,
                    SessionId=session_id
                )
                
                if result.get("error_type"):
                    return MessageResponseWrapper(
                        success=False,
                        code=503,
                        message="Database temporarily unavailable",
                        errors=[result["error_type"]],
                        data={
                            "response_type": "message",
                            "response_message": result.get("user_message", "Database error occurred.")
                        }
                    )
                
                rows = result["rows"]
                count = result["count"]
                index = result["index"]
                
                logger.info(f"[RESULT] Retrieved {count} rows, stored at index {index}")
                
                if not rows:
                    return DatabaseResponseWrapper(
                        success=True,
                        code=200,
                        message="No data found, please try again.",
                        data={
                            "response_type": "database",
                            "columns": [],
                            "rows": [],
                            "count": 0,
                            "index": index
                        }
                    )
                
                columns = list(rows[0].keys())
                ui_rows = [
                    safe_serialize({col: row.get(col) for col in columns})
                    for row in rows[:self.MAX_UI_ROWS]
                ]
                
                return DatabaseResponseWrapper(
                    success=True,
                    code=200,
                    message="Database data fetched successfully.",
                    data={
                        "response_type": "database",
                        "columns": columns,
                        "rows": ui_rows,
                        "count": count,
                        "index": index
                    }
                )

            elif mode == "ai_cached":
                logger.info("Executing AI_CACHED mode (checking cache for AI task)...")
                
                all_messages = self.redis_service.get_all_messages(TenantId, session_id)
                has_cached_data = any(
                    msg.get("role") == "system" and bool(msg.get("content"))
                    for msg in all_messages
                )
                
                if not has_cached_data:
                    logger.info("[CACHE] No cached data found - asking user to fetch")
                    
                    return MessageResponseWrapper(
                        success=True,
                        code=200,
                        message="No cached data available.",
                        data={
                            "response_type": "message",
                            "response_message": (
                                "I don't have any invoice data loaded yet. "
                                "Would you like me to fetch the latest invoice data from the database? "
                                "Reply with **YES** to retrieve fresh data."
                            )
                        }
                    )
                
                logger.info("[CACHE] Found cached data - performing AI analysis")
                
                refined_query = routing_decision.get("refined_query", query)
                intent = routing_decision.get("intent", "general_qa")
                
                logger.info(f"[AI] Intent: {intent}")
                
                result = self.process_model_query(
                    TenantId=TenantId,
                    SessionId=session_id,
                    query=refined_query,
                    intent=intent
                )
                
                
                if result.get("error_type"):
                    return MessageResponseWrapper(
                        success=False,
                        code=503,
                        message="AI processing failed",
                        errors=[result["error_type"]],
                        data={
                            "response_type": "message",
                            "response_message": result.get("user_message", "AI processing error occurred.")
                        }
                    )
                summary = result.get("analysis_text")
                chart = result.get("chart")
                
                summary_index = summary["index"] if summary else None
                chart_index = chart["index"] if chart else None
                
                logger.info(
                    f"[RESULT] AI response generated from cache "
                    f"(summary_index={summary_index}, chart_index={chart_index})"
                )
                
                return ChatResponseWrapper(
                    success=True,
                    code=200,
                    message="AI analysis complete using cached data.",
                    data={
                        "response_type": "ai",
                        "analysis_text": summary,
                        "chart": chart
                    }

                )
            
            elif mode == "filter_cached":
                logger.info("Executing FILTER_CACHED mode (using last system data)...")
                
                all_messages = self.redis_service.get_all_messages(TenantId, session_id)
                has_cached_data = any(
                    msg.get("role") == "system" and bool(msg.get("content"))
                    for msg in all_messages
                )
                
                if not has_cached_data:
                    logger.info("[CACHE] ❌ No cached data found - asking user to fetch")
                    
                    return MessageResponseWrapper(
                        success=True,
                        code=200,
                        message="No cached data available.",
                        data={
                            "response_type": "message",
                            "response_message": (
                                "I don't have any invoice data loaded yet. "
                                "Would you like me to fetch the latest invoice data from the database? "
                                "Reply with **YES** to retrieve fresh data."
                            )
                        }
                    )
                    
                logger.info("[CACHE] Using last retrieved data for analysis")
                
                refined_query = routing_decision.get("refined_query", query)
                intent = routing_decision.get("intent", "general_qa")
                
                logger.info(f"[AI] Intent: {intent}")
                
                result = self.process_model_query(
                    TenantId=TenantId,
                    SessionId=session_id,
                    query=refined_query,
                    intent=intent
                )
                
                if result.get("error_type"):
                    return MessageResponseWrapper(
                        success=False,
                        code=503,
                        message="AI processing failed",
                        errors=[result["error_type"]],
                        data={
                            "response_type": "message",
                            "response_message": result["user_message"]
                        }
                    )
                
                summary = result.get("analysis_text")
                chart = result.get("chart")
                
                footer_message = (
                    "\n\n"
                    "ℹ️ *Results based on last retrieved data. "
                    "Would you like to fetch fresh data from the database? "
                    "Reply with* **YES** *to refresh.*"
                )
                
                if summary:
                    summary = {
                        "text": summary["text"] + footer_message,
                        "index": summary["index"]
                }
                
                summary_index = summary["index"] if summary else None
                chart_index = chart["index"] if chart else None

                logger.info(
                    f"[RESULT] AI response generated from last cache "
                    f"(summary_index={summary_index}, chart_index={chart_index})"
                )

                return ChatResponseWrapper(
                    success=True,
                    code=200,
                    message="Analysis complete using last retrieved data.",
                    data={
                        "response_type": "ai",
                        "analysis_text": summary,
                        "chart": chart
                    }

                )
            
            elif mode == "hybrid":
                logger.info("Executing HYBRID mode (Database → AI, returning BOTH)...")

                db_query = routing_decision.get("database_query", query)
                
                logger.info(f"[HYBRID-DB] Fetching fresh data with: {db_query}")
                
                db_result = self.search(
                    query=db_query,
                    TenantId=TenantId,
                    SessionId=session_id
                )
                
                if db_result.get("error_type"):
                    return MessageResponseWrapper(
                        success=False,
                        code=503,
                        message="Database temporarily unavailable",
                        errors=[db_result["error_type"]],
                        data={
                            "response_type": "message",
                            "response_message": db_result.get("user_message", "Database error occurred.")
                        }
                    )
                
                rows = db_result["rows"]
                count = db_result["count"]
                db_index = db_result["index"]
                
                logger.info(f"[HYBRID-DB] Retrieved {count} rows")
                             
                if not rows:
                    return DatabaseResponseWrapper(
                        success=True,
                        code=200,
                        message="No data found for your query.",
                        data={
                            "response_type": "database",
                            "columns": [],
                            "rows": [],
                            "count": 0,
                            "index": db_index
                        }
                    )

                columns = list(rows[0].keys())
                ui_rows = [
                    safe_serialize({col: row.get(col) for col in columns})
                    for row in rows[:self.MAX_UI_ROWS]
                ]
                
                ai_query = routing_decision.get("ai_query", query)
                intent = routing_decision.get("intent", "general_qa")
                
                logger.info(f"[HYBRID-AI] Analyzing with intent: {intent}")
                
                ai_result = self.process_model_query(
                        TenantId=TenantId,
                        SessionId=session_id,
                        query=ai_query,
                        intent=intent
                    )
                    
                if ai_result.get("error_type"):
                    logger.warning(f"[HYBRID-AI] AI processing failed: {ai_result['error_type']}")
                    return HybridResponseWrapper(
                        success=True,
                        code=200,
                        message="Database data fetched, AI analysis unavailable.",
                        errors=[ai_result["error_type"]],
                        data={
                            "response_type": "hybrid",
                            "database": {
                                "columns": columns,
                                "rows": ui_rows,
                                "count": count,
                                "index": db_index,
                        },
                            "ai": {
                                "analysis_text": None,
                                "chart": None
                            }
                        }
                    )
                
                summary = ai_result.get("analysis_text")
                chart = ai_result.get("chart")

                logger.info(
                    f"[HYBRID] AI artifacts "
                    f"(summary_index={summary['index'] if summary else None}, "
                    f"chart_index={chart['index'] if chart else None})"
                )
                
                return HybridResponseWrapper(
                    success=True,
                    code=200,
                    message="Hybrid analysis complete with fresh data.",
                    errors=[],
                    data={
                        "response_type": "hybrid",
                        "database": {
                            "columns": columns,
                            "rows": ui_rows,
                            "count": count,
                            "index": db_index,
                        },
                        "ai": {
                            "analysis_text": ai_result.get("analysis_text"),
                            "chart": ai_result.get("chart")
                        },
                    },
                )

            raise HTTPException(
                status_code=500,
                detail=f"Unknown routing mode: {mode}"
            )
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"[API] Unified search failed: {e}", exc_info=True)

            return MessageResponseWrapper(
                success=False,
                code=500,
                message="Service temporarily unavailable",
                errors=["UNIFIED_SEARCH_FAILED"],
                data={
                    "response_type": "message",
                    "response_message": (
                        "Something went wrong while processing your request. "
                        "Please try again in a few moments."
                    )
                }
            )

    