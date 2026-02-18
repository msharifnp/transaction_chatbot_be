import json
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Any
import re
from fastapi import HTTPException, status
import logging
import os
from src.utils.transaction_lookups import enrich_transaction_data

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
from src.ai.gemini_service import GeminiService
from src.db.redis_service import RedisService
from src.ai.sql_query_generator import SQLQueryGenerator
from src.db.db_service import DatabaseService
from src.config.startup import model_startup 


class SearchService:
       
    MAX_ROWS = 100
    MAX_UI_ROWS = 5
    
    def __init__(self, tenant_id: str = None):
        self.tenant_id = tenant_id
       
        self.db_config = DatabaseConfig.get_database_config()
        self.redis_config = RedisConfig.get_redis_config()
        self._init_services()
        self.router = QueryRouter(model_service=self.model_service)  

    
    def _init_services(self):
        """Initialize all dependent services."""
        
        # Step 1: Initialize ModelService first (required by others)
        
        if self.tenant_id:
            self.model_service = model_startup.get_or_create_service(self.tenant_id)
        
        
        # Step 2: Initialize services that depend on ModelService
        self.gemini_service = GeminiService(self.model_service)
        self.sql_generator = SQLQueryGenerator(model_service=self.model_service)
        
        # Step 3: Initialize independent services
        self.db_service = DatabaseService(self.db_config)
        
        
        self.redis_service = RedisService(self.redis_config)
        
        # Get database config
        self.db_settings = self.db_config
    
    @property
    def use_gemini(self) -> bool:
        """Check if Gemini service is available."""
        return self.model_service.is_available()
    
    
    # ===============================================================
    # DATABASE MODE
    # ===============================================================
    
    def search(self, query: str, TenantId: str, SessionId: str ) -> Dict[str, Any]:
        
        self.redis_service.validate_tenant_session(TenantId, SessionId)
        
        # Step 1: Store user query
        self.redis_service.store_message(
            TenantId=TenantId,
            SessionId=SessionId,
            role="user",
            content=query,
            metadata={"source": "database_search"}
        )
        
        # Step 2: Generate SQL with tenant filter
        print("[STEP 1] Generating SQL Query with tenant filter...")
        try:
            sql_result = self.sql_generator.generate_sql(query, TenantId=TenantId)
            sql = sql_result.get("sql")
            print(f"[SQL GENERATED] {sql}")
        except Exception as e:
            print(f"[SQL GENERATION ERROR] {str(e)}")
            logging.error(f"SQL generation failed: {e}", exc_info=True)
            return {
                "ok": False,
                "error_type": "SQL_GENERATION_FAILED",
                "user_message": (
                    "Failed to generate database query. "
                    "Please try again with accurate prompt."
                )
            }
        
        if not sql or TenantId not in sql:
            return {
                "ok": False,
                "error_type": "SQL_GENERATION_FAILED",
                "user_message":(
                    "Failed to generate database query."
                    "Please try again with accurate prompt."
                )
            
            }
        
        # Step 3: Execute SQL
        print("[STEP 2] Executing on Oracle...")
        try:
            rows = self.db_service.execute_query(sql)
            count = len(rows)
            if rows and any(key in rows[0] for key in ["Source Type", "Source Qualifier"]):
                print("[SEARCH] Detected transaction data - enriching with descriptions...")
                rows = enrich_transaction_data(rows)
                print("[SEARCH] ✅ Transaction data enriched")
            print(f"[RESULT] Retrieved {count} rows for tenant:{TenantId}")
        except Exception as e:
            return {
                "ok": False,
                "error_type": "DB_EXECUTION_FAILED",
                "user_message":(
                    "Failed to generate database query."
                    "Please try again with accurate prompt."
                )
            }

        
        # Step 4: Store results as system message (ONLY DATA)
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
    



    
    
    def _build_contextual_ai_query(self, query: str, conversation: Optional[List[Dict]]) -> str:
        if not conversation:
            return query

        lines: List[str] = []
        for msg in conversation:
            role = msg.get("role", "")
            content = (msg.get("content") or "").strip()
            if not content:
                continue
            if role == "user":
                lines.append(f"User: {content}")
            elif role == "assistant":
                lines.append(f"Assistant: {content}")

        if not lines:
            return query

        context_lines = "\n".join(lines)
        return (
            "Conversation Context (last 2 user + last 2 assistant turns):\n"
            f"{context_lines}\n\n"
            "Current User Question:\n"
            f"{query}"
        )

    @staticmethod
    def _is_hybrid_requested_by_user(query: str) -> bool:
        q = (query or "").lower()
        summary_terms = [
            "summarize", "summary", "insight", "insights", "explain", "analysis", "analyze", "trend", "trends"
        ]
        forecast_terms = ["forecast", "predict", "projection", "future", "next month", "next quarter", "next year"]
        chart_terms = ["chart", "graph", "plot", "visualization", "visualize", "dashboard"]
        tokens = summary_terms + forecast_terms + chart_terms
        return any(re.search(rf"\b{re.escape(t)}\b", q) for t in tokens)

    def process_model_query(self, TenantId: str, SessionId: str, query: str, intent: str = "summary") -> Dict[str, Any]:

        self.redis_service.validate_tenant_session(TenantId, SessionId)

        try:
            
            # 1️⃣ Store user query
            self.redis_service.store_message(
                TenantId=TenantId,
                SessionId=SessionId,
                role="user",
                content=query,
                metadata={"intent": intent, "source": "ai_chat"}
            )

            # 2️⃣ Get context
            system_data, conversation = self.redis_service.get_context_for_ai(
                TenantId, SessionId
            )

            if not system_data:
                raise ValueError("No cached data found for AI analysis")

            contextual_query = self._build_contextual_ai_query(
                query=query,
                conversation=conversation
            )


            print(f"[AI MODE] Processing with {len(system_data)} stored rows")

            # =====================================================
            # 🟢 SUMMARY
            # =====================================================
            if intent == "summary":
                analysis = self.gemini_service.generate_summary(
                    user_query=contextual_query,
                    rows=system_data[:self.MAX_ROWS]
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

            # =====================================================
            # 🟢 FORECAST (TEXT + SVG CHART)
            # =====================================================
            elif intent == "forecast":
                forecast_result = self.gemini_service.generate_forecast(
                    user_query=contextual_query,
                    rows=system_data[:self.MAX_ROWS]
                    
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
                
                

                chart = self.gemini_service.generate_forecast_chart(
                    user_query=contextual_query,
                    forecast_rows= forecast_rows
                )

                chart_index = None
                if chart and not chart.get("error"):
                    import base64
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

            # =====================================================
            # 🟢 CHART ONLY (SVG)
            # =====================================================
            
            elif intent == "chart":
                chart = self.gemini_service.generate_chart(
                    contextual_query,
                    system_data[:self.MAX_ROWS]
                )

                if not chart or chart.get("error"):
                    content = "❌ Failed to generate chart"
                    metadata = {"intent": "chart", "error": True}
                else:
                    import base64
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

                # ✅ IMPORTANT: return SVG as content
                return {
                    "analysis_text": None,
                    "chart":{
                        "svg":content,
                        "index":index
                    }
                }

            # =====================================================
            # 🟢 GENERAL QA
            # =====================================================
            else:
                raise ValueError(f"Unsupported AI intent: {intent}")

        except Exception as e:
            print(f"[AI MODE] ❌ Error: {e}")
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



    # ===============================================================
    # UNIFIED SEARCH (MAIN ORCHESTRATOR)
    # ===============================================================
    
    def unified_search(self, req: UnifiedSearchRequest, session_id: str) -> Any:
        """
        Main orchestrator for unified search.
        
        Args:
            req: Request containing the user's query
            session_id: Session ID from request headers
            
        Returns:
            One of: DatabaseResponseWrapper, MessageResponseWrapper, 
                    ChatResponseWrapper, or HybridResponseWrapper
        """
        try:
            # Validate Gemini availability
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
            
            # ===== HANDLE REFRESH REQUESTS =====
            if is_refresh_request(query):
                print("[REFRESH] User requested fresh data (YES/OK detected).")
                
                all_messages = self.redis_service.get_all_messages(TenantId, session_id)  # ✅ Fixed
                last_user_query = get_last_real_user_query(all_messages)
                
                if not last_user_query:
                    print("[REFRESH] No previous user query found to refresh.")
                    return MessageResponseWrapper(
                        success=True,
                        code=200,
                        message="Nothing to refresh.",
                        data={
                            "response_type": "message",
                            "response_message": (
                                "I couldn't find a previous invoice query to refresh. "
                                "Please ask something like 'pending invoices' or "
                                "'approved invoices for last month' first."
                            ),
                        },
                    )
                
                # Extract additional context from current query
                additional_context = extract_additional_context(query)
                
                # Combine last query with new context
                if additional_context:
                    combined_query = f"{last_user_query} {additional_context}"
                    print(f"[REFRESH] Last query: {last_user_query!r}")
                    print(f"[REFRESH] Additional context: {additional_context!r}")
                    print(f"[REFRESH] Combined query: {combined_query!r}")
                else:
                    combined_query = last_user_query
                    print(f"[REFRESH] No additional context, using: {combined_query!r}")
                
                # Direct DATABASE search with combined query
                result = self.search(
                    query=combined_query,
                    TenantId=TenantId,
                    SessionId=session_id,  # ✅ Fixed
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
                
                print(f"[RESULT] Retrieved {count} rows, stored at index {index}")
                
                

                
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
            
            # ===== STEP 1: INTELLIGENT ROUTING =====
            print("[STEP 1] Analyzing query for intelligent routing...")
            
            routing_decision = self.router.intelligent_route(query, TenantId, session_id)
            if routing_decision.get("router_error"):
                error_detail = routing_decision.get("router_error_detail")
                print(f"[ROUTER] ❌ Router failure: {error_detail}")

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

            # Legacy mode is no longer supported; treat as database request.
            if mode == "__unused_filter_cached__":
                mode = "database"

            # Enforce strict HYBRID usage:
            # Only use hybrid when user explicitly asks for summarize/forecast/chart-style output.
            if mode == "hybrid" and not self._is_hybrid_requested_by_user(query):
                print("[ROUTER] ⚠️ HYBRID downgraded to DATABASE (no explicit analysis/forecast/chart intent).")
                mode = "database"
                routing_decision["refined_query"] = routing_decision.get("database_query", query)
            
            print(f"[ROUTER] 🎯 Decision: {mode.upper()}")
            print(f"[ROUTER] 📝 Reasoning: {reasoning}")
            
            # ===== HANDLE GREETINGS / NON-INVOICE QUERIES =====
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
            
            # ===== DATABASE MODE =====
            elif mode == "database":
                print("[STEP 2] Executing DATABASE search...")
                
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
                
                print(f"[RESULT] Retrieved {count} rows, stored at index {index}")
                
                

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
            
            # ===== AI_CACHED MODE (Summarize/Forecast/Chart without date) =====
            elif mode == "ai_cached":
                print("[STEP 2] Executing AI_CACHED mode (checking cache for AI task)...")
                
                # Check if data exists in cache
                all_messages = self.redis_service.get_all_messages(TenantId, session_id)
                has_cached_data = any(
                    msg.get("role") == "system" and bool(msg.get("content"))
                    for msg in all_messages
                )
                
                if not has_cached_data:
                    # NO CACHE - Ask user to fetch data first
                    print("[CACHE] No cached data found - asking user to fetch")
                    
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
                
                # CACHE EXISTS - Perform AI analysis
                print(f"[CACHE] Found cached data - performing AI analysis")
                
                refined_query = routing_decision.get("refined_query", query)
                intent = routing_decision.get("intent", "summary")
                
                print(f"[AI] Intent: {intent}")
                
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
                
                print(
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
            
            # ===== FILTER_CACHED MODE (Pending/Verified/Expired without date) =====
            elif mode == "__unused_filter_cached__":
                print("[STEP 2] Executing FILTER_CACHED mode (using last system data)...")
                
                # Check if data exists in cache
                all_messages = self.redis_service.get_all_messages(TenantId, session_id)
                has_cached_data = any(
                    msg.get("role") == "system" and bool(msg.get("content"))
                    for msg in all_messages
                )
                
                if not has_cached_data:
                    # NO CACHE - Ask user to fetch data first
                    print("[CACHE] ❌ No cached data found - asking user to fetch")
                    
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
                
                # CACHE EXISTS - Perform analysis on cached data
                print(f"[CACHE] Using last retrieved data for analysis")
                
                refined_query = routing_decision.get("refined_query", query)
                intent = routing_decision.get("intent", "summary")
                
                print(f"[AI] Intent: {intent}")
                
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

                

                
                # Add footer indicating data source and offer refresh
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

                print(
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
            
            # ===== HYBRID MODE (Database + AI) - RETURN BOTH =====
            elif mode == "hybrid":
                print("[STEP 2] Executing HYBRID mode (Database → AI, returning BOTH)...")
                
                # Step 2a: Fetch fresh data from database
                db_query = routing_decision.get("database_query", query)
                
                print(f"[HYBRID-DB] Fetching fresh data with: {db_query}")
                
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
                
                print(f"[HYBRID-DB] ✅ Retrieved {count} rows")
                
                                
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
                
                # Prepare database response data
                columns = list(rows[0].keys())
                ui_rows = [
                    safe_serialize({col: row.get(col) for col in columns})
                    for row in rows[:self.MAX_UI_ROWS]
                ]
                
                # Step 2b: Perform AI analysis on the fresh data
                ai_query = routing_decision.get("ai_query", query)
                intent = routing_decision.get("intent", "summary")
                
                print(f"[HYBRID-AI] Analyzing with intent: {intent}")
                
                
                                
                
                
                ai_result = self.process_model_query(
                        TenantId=TenantId,
                        SessionId=session_id,
                        query=ai_query,
                        intent=intent
                    )
                    
                    
                    
                
                    
                
                if ai_result.get("error_type"):
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

                print(
                    f"[HYBRID] AI artifacts "
                    f"(summary_index={summary['index'] if summary else None}, "
                    f"chart_index={chart['index'] if chart else None})"
                )

                
                # Return HYBRID response with BOTH database and AI data
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
            
            # Fallback
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

    
