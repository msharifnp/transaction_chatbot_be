import json
from typing import Dict
from src.utils.utils import retry_with_backoff
from src.db.model_service import ModelService


class QueryRouter:
    
    def __init__(self, model_service:ModelService):
        
        self.model_service = model_service
        self.enabled = model_service.is_available()
        


    def intelligent_route(self, user_query: str, TenantId: str = "", SessionId: str = "") -> Dict:
   
        prompt = self._build_routing_prompt(user_query)

        try:
            def generate():
                text = self.model_service.generate_text(prompt)
                if not text or not text.strip():
                    raise Exception("Empty response from Gemini")
                return text

            text = retry_with_backoff(generate)

            text = text.strip()
            if text.startswith("```"):
                text = text.replace("```json", "").replace("```", "").strip()

            result = json.loads(text)

            mode = result.get("mode", "message")
            result.setdefault("reasoning", "No reasoning provided by LLM")
            result["should_proceed"] = mode != "message"

            print(f"[ROUTER] ✅ Mode: {mode}")
            print(f"[ROUTER] 📝 Reasoning: {result['reasoning']}")

            return result

        except Exception as e:
            print(f"[ROUTER] ❌ Routing error: {e}")
            return {
                "mode": "message",
                "message": "Hello! I'm here to help with invoice data. How can I assist you?",
                "reasoning": "router_llm_failure",
                "should_proceed": False,
                "router_error": True,
                "router_error_detail": str(e),
            }

    # ------------------------------------------------------------------ #
    # Prompt builder
    # ------------------------------------------------------------------ #

    def _build_routing_prompt(self, user_query: str) -> str:
        """
        IMPORTANT: ALL logic is here.
        Gemini must classify the query into one of the modes and fill fields.
        """

        return f"""You are a query router for an invoice analysis system.

Your job: read the user query and decide how it should be processed.
You must output ONLY a single-line JSON object. No markdown, no code fences.

USER_QUERY: "{user_query}"

The system has:
- A relational invoice database (SQL)
- A cache of the last retrieved invoice rows (system messages)
- An AI analysis engine (summary, forecast, trends, charts)
- A previous step where the assistant may have asked: 
  "Would you like to fetch fresh data from the database? Reply with YES to refresh."

You must choose EXACTLY ONE of these modes:

1. "message"
   Use when:
   - The query is a greeting or small talk:
     ("hi", "hello", "good morning", "how are you", etc.)
   - The query is about the assistant itself:
     ("who are you", "what can you do", "help", etc.)
   - The query is clearly NOT related to invoices, bills, payments or finance.
   Fields:
   - mode: "message"
   - message: friendly, helpful short response
   - reasoning: short explanation

2. "database"
   Use when:
   - The user explicitly wants FRESH data from the database,
   - OR explicitly specifies a time range or point in time
     (e.g. dates, months, years, "last 3 months", "this month", "last year",
      "between X and Y", "from ... to ...", "today", "yesterday"),
   - AND the main goal is to retrieve invoice rows (not only a summary).
   Examples:
   - "Show invoices for the last 4 months"
   - "Invoices between 2024-01-01 and 2024-03-31"
   - "All Etisalat invoices from last year"
   - "Get all pending invoices for this month"
   Fields:
   - mode: "database"
   - refined_query: cleaned, grammatically correct version of the query,
                    suitable to pass into an AI-to-SQL generator
   - reasoning: why database fetch is needed
   - if user sum of grand total or total of grand total or any field
   - if user query has database or all 
   strict rule:
   - if user say future month, next month, next 3 months , predict next or next 3 months  mean if any word like next, future before months dont conisder database

3. "hybrid"
   Use when:
   - The query BOTH:
       a) clearly involves a time range or fresh data (as above), AND
       b) clearly asks for AI analysis: summary, trends, insight, forecast, chart, etc.
   - The system will:
       1) fetch fresh data from the database, and
       2) run AI analysis on that fresh dataset.
   Examples:
   - "Summarize last month's invoices"
   - "Forecast future spend based on last year's invoices"
   - "Give me trends for Etisalat invoices in the past 6 months"
   - "Show a chart of last year's invoices"
   Fields:
   - mode: "hybrid"
   - database_query: clean query focused on what data to fetch
   - ai_query: clean query describing the analysis task
   - intent: one of "summary", "chart", "forecast", "general_qa"
   - reasoning: why both DB and AI are needed

4. "ai_cached"
   Use when:
   - The user asks for AI ANALYSIS (summary / forecast / trends / insights / charts)
   - BUT does NOT specify any explicit time range or date.
   - The AI should analyze EXISTING cached data (last retrieved system data).
   Examples:
   - "Summarize the invoices"
   - "Give me insights from this data"
   - "Forecast my future spend"
   - "Show a chart of these invoices"
   Fields:
   - mode: "ai_cached"
   - refined_query: cleaned version of the query
   - intent: "summary" | "chart" | "forecast" | "general_qa"
     (choose based on the wording: summary → summary, forecast → forecast, 
      chart/graph → chart, otherwise → general_qa)
   - reasoning: why AI analysis on cached data
   - if user say future month, next month, next 3 months , predict next or next 3 months  mean if any word like next, future before months dont conisder ai_cached


5. "filter_cached"
   Use when:
   - other than "summary" | "chart" | "forecast"  any field witout date
   - The query is clearly about selecting/filtering invoices,
     e.g. by status, provider, location, account, amount, etc.
   - There is NO explicit time range.
   - There is NO explicit request for summary/forecast/trend/chart/insights.
   - The user just wants a list or specific items (e.g. highest/lowest, unpaid, disputed).
   Examples:
   - "pending invoices"
   - "unpaid invoices"
   - "disputed invoices"
   - "invoices from Etisalat"
   - "Abu Dhabi invoices"
   - "highest invoice"
   Fields:
   - mode: "filter_cached"
   - refined_query: cleaned version of the query
   - intent: "general_qa"
   - reasoning: why this is a filter/selection on cached data

6. "database" with refresh_previous_filter = true
   Use when:
   - The previous response from the system said something like:
     "Results based on last retrieved data. Would you like to fetch fresh data from the database? Reply with YES to refresh."
   - And the CURRENT user query is clearly a confirmation to refresh:
     e.g. "yes", "ok", "okay", "sure", "yeah", "yep", "refresh", "get fresh data", etc.
   - In that case, assume we should rerun the **previous filter query** against the database.
   Fields:
   - mode: "database"
   - refined_query: "REFRESH_PREVIOUS_FILTER"
     (exactly this token, so the backend knows to reuse the last filter query)
   - refresh_previous_filter: true
   - reasoning: "User confirmed to refresh data from the database"

INTENTS:
- "summary"    → overview, summary, explanation, business report
- "chart"      → visualization, graph, chart, dashboard
- "forecast"   → prediction, future trend, next period
- "general_qa" → specific facts, counts, totals, top/least, highest/lowest, filters

RESPONSE RULES:
- Return ONLY valid JSON, no markdown, no code fences.
- Single JSON object on one line.
- Do NOT include newline characters inside any string.
- ALWAYS include:
    - "mode"
    - "reasoning"
- Include other fields only when relevant for that mode.
- Be strict and deterministic.

Now, respond with the JSON object for this USER_QUERY.
"""
