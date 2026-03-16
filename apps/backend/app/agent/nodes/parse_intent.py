"""
Node 1: parse_intent_node
─────────────────────────
Extracts structured intent from the user's natural language query using Claude.
Output: ParsedIntent — feeds into map_procedure_node.
"""

import json

from anthropic import AsyncAnthropic
from app.agent.state import AgentState, ParsedIntent
from app.core.config import get_settings
from app.core.logging import get_logger

log = get_logger(__name__)

SYSTEM_PROMPT = """You are a healthcare data expert. Parse user queries about hospital pricing into structured intent.

Extract:
- intent_type: one of [compare, lookup, cost_model, network_design, general]
- networks: specific hospital networks mentioned (e.g. Inova, HCA Virginia, UVA Health)
- procedure_terms: medical procedure names or CPT/billing codes mentioned
- geographic_scope: state | city | zip | national
- city_filter: city name if mentioned
- zip_filter: zip code if mentioned
- plan_filter: insurance plan if mentioned
- requires_aggregation: true if they want averages/medians/totals
- requires_comparison: true if comparing multiple things

Respond ONLY with valid JSON matching this schema. No markdown, no explanation.
"""


async def parse_intent_node(state: AgentState) -> AgentState:
    log.info("parse_intent_node", session=state.get("session_id"))
    settings = get_settings()
    client = AsyncAnthropic(api_key=settings.anthropic_api_key)

    try:
        response = await client.messages.create(
            model=settings.anthropic_model,
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": state["user_query"]}],
        )

        raw = response.content[0].text.strip()
        intent_data = json.loads(raw)

        intent: ParsedIntent = {
            "intent_type": intent_data.get("intent_type", "general"),
            "networks": intent_data.get("networks", []),
            "procedure_terms": intent_data.get("procedure_terms", []),
            "geographic_scope": intent_data.get("geographic_scope", "state"),
            "city_filter": intent_data.get("city_filter"),
            "zip_filter": intent_data.get("zip_filter"),
            "plan_filter": intent_data.get("plan_filter"),
            "time_range": intent_data.get("time_range"),
            "requires_aggregation": intent_data.get("requires_aggregation", False),
            "requires_comparison": intent_data.get("requires_comparison", False),
        }

        log.info("Intent parsed", intent_type=intent["intent_type"],
                 networks=intent["networks"], procedures=intent["procedure_terms"])

        return {
            **state,
            "intent": intent,
            "nodes_visited": state.get("nodes_visited", []) + ["parse_intent"],
        }

    except Exception as e:
        log.error("parse_intent_node failed", error=str(e))
        return {
            **state,
            "intent": {
                "intent_type": "general",
                "networks": [],
                "procedure_terms": [],
                "geographic_scope": "state",
                "city_filter": None,
                "zip_filter": None,
                "plan_filter": None,
                "time_range": None,
                "requires_aggregation": False,
                "requires_comparison": False,
            },
            "nodes_visited": state.get("nodes_visited", []) + ["parse_intent"],
            "error": str(e),
        }
