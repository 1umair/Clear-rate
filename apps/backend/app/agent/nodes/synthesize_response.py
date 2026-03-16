"""
Node 5: synthesize_response_node
─────────────────────────────────
Takes query results and generates a natural language response tailored to
the user's original question and intent type.

This is the final node before returning to the user.
"""

import json

from anthropic import AsyncAnthropic
from app.agent.state import AgentState
from app.core.config import get_settings
from app.core.logging import get_logger

log = get_logger(__name__)

SYSTEM_PROMPT = """You are a healthcare cost advisor for enterprise buyers (self-funded employers, TPAs, insurers).

Your job is to synthesize database query results into clear, actionable insights.

Formatting rules:
- Do NOT use em dashes (—) anywhere in your response
- Do NOT use bullet-point lists with dashes (-) when a table would be clearer
- Format dollar amounts as $X,XXX (no cents unless the amount is under $100)
- Use markdown tables for comparisons across multiple hospitals or payers
- Keep responses to 2-4 paragraphs plus a summary table when applicable

Content rules:
- Lead with the single most important number or finding
- Compare across networks and hospitals when data supports it
- Flag outliers: call out the highest and lowest rates explicitly
- Use plain language; spell out what CPT codes mean when you reference them
- If results are empty, say so plainly and suggest a different way to ask
- Do NOT make clinical recommendations, only financial and cost observations
- Do NOT include SQL, technical details, or internal system notes in your answer

Disclaimer to append to every response:
"*Data sourced from CMS machine-readable files. Rates represent negotiated prices between payers and providers and do not reflect actual patient out-of-pocket costs, deductibles, or copays.*"
"""


async def synthesize_response_node(state: AgentState) -> AgentState:
    log.info("synthesize_response_node", session=state.get("session_id"))

    # Short-circuit on error with a helpful message
    if state.get("error") and not state.get("query_results"):
        return {
            **state,
            "final_answer": (
                "I encountered an issue processing your request. "
                "Please try rephrasing your question or check that the backend is running.\n\n"
                f"*Technical detail: {state['error']}*"
            ),
            "nodes_visited": state.get("nodes_visited", []) + ["synthesize_response"],
        }

    settings = get_settings()
    client = AsyncAnthropic(api_key=settings.anthropic_api_key)

    results = state.get("query_results", [])
    intent = state.get("intent", {})

    # Truncate results for the prompt (avoid token explosion)
    sample_results = results[:50] if results else []
    results_json = json.dumps(sample_results, indent=2, default=str)

    total_count = len(results)
    truncation_note = f"\n(Showing 50 of {total_count} total records)" if total_count > 50 else ""

    user_message = f"""
Original question: {state['user_query']}

Intent: {intent.get('intent_type', 'general')}
State filter applied: {state.get('state_code', 'None (national)')}
Total records returned: {total_count}

Query results:{truncation_note}
{results_json}

Please synthesize these results into a clear, actionable response.
"""

    try:
        response = await client.messages.create(
            model=settings.anthropic_model,
            max_tokens=settings.anthropic_max_tokens,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )

        answer = response.content[0].text.strip()
        log.info("Response synthesized", length=len(answer))

        return {
            **state,
            "final_answer": answer,
            "nodes_visited": state.get("nodes_visited", []) + ["synthesize_response"],
        }

    except Exception as e:
        log.error("synthesize_response_node failed", error=str(e))
        return {
            **state,
            "final_answer": "I was unable to generate a response. Please try again.",
            "nodes_visited": state.get("nodes_visited", []) + ["synthesize_response"],
            "error": str(e),
        }
