import json
from typing import Dict

from src.models.model_service import ModelService
from src.utils.utils import retry_with_backoff


class QueryRouter:
    PURPOSE = "Summary"

    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)

    def intelligent_route(self, user_query: str, TenantId: str = "", SessionId: str = "") -> Dict:
        prompt = self._build_routing_prompt(user_query)

        try:
            def generate():
                text = self.model_service.generate(self.PURPOSE, prompt)
                if not text or not text.strip():
                    raise Exception("Empty response from Gemini")
                return text

            text = retry_with_backoff(generate).strip()
            if text.startswith("```"):
                text = text.replace("```json", "").replace("```", "").strip()

            result = json.loads(text)

            mode = result.get("mode", "message")
            if mode == "filter_cached":
                mode = "database"
                result["mode"] = "database"

            if mode in {"ai_cached", "hybrid"}:
                raw_intent = str(result.get("intent", "summary")).strip().lower()
                if raw_intent in {"trend", "trending", "insight", "insights", "explain"}:
                    raw_intent = "summary"
                if raw_intent not in {"summary", "forecast", "chart"}:
                    raw_intent = "summary"
                result["intent"] = raw_intent

            result.setdefault("reasoning", "No reasoning provided by LLM")
            result["should_proceed"] = mode != "message"

            print(f"[ROUTER] Mode: {mode}")
            print(f"[ROUTER] Reasoning: {result['reasoning']}")
            return result

        except Exception as e:
            print(f"[ROUTER] Routing error: {e}")
            return {
                "mode": "message",
                "message": "Hello! I'm here to help with invoice data. How can I assist you?",
                "reasoning": "router_llm_failure",
                "should_proceed": False,
                "router_error": True,
                "router_error_detail": str(e),
            }

    def _build_routing_prompt(self, user_query: str) -> str:
        return f"""You are a query router for an invoice analysis system.

Your job: read the user query and decide how it should be processed.
Output ONLY one-line JSON (no markdown, no code fences).

USER_QUERY: "{user_query}"

Choose exactly one mode:
1) "message"
- Greetings, small talk, or assistant-help questions.

2) "database"
- Any invoice retrieval/filter/search query.
- Any invoice query that is not a pure analysis task.
- Refresh confirmations ("yes/ok/sure/refresh") after refresh prompt.

3) "ai_cached"
- Analysis on existing cached rows only.
- Use for summarize/explain/why/insights/trends/trending/charts/forecast follow-ups
  when no fresh fetch or new explicit date range is requested.

4) "hybrid"
- Query needs BOTH fresh database fetch and AI analysis together.
- Use ONLY when the same user question explicitly requests analysis output
  (summary/insights/explain), forecasting, or charts/graphs together with fresh data retrieval.
- If user only asks to fetch/list/show records (even with date filters), use "database" not "hybrid".

Intent rules for ai_cached/hybrid:
- Allowed intents only: "summary", "forecast", "chart"
- summary keywords: summarize, overview, explain, why, what should we do, insight, trend
- forecast keywords: predict, forecast, future, next
- chart keywords: chart, graph, visualization
- If uncertain, use "summary"

Output fields:
- Always include: "mode", "reasoning"
- message: include "message"
- database: include "refined_query"
- ai_cached: include "refined_query", "intent"
- hybrid: include "database_query", "ai_query", "intent"

Return valid single-line JSON only.
"""
