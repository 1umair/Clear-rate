"""
LangGraph state machine — wires the 5 nodes into a sequential pipeline
with error-aware conditional routing.

Graph topology:
  START
    └─► parse_intent_node
          └─► map_procedure_node
                └─► generate_sql_node
                      └─► execute_sql_node
                            └─► synthesize_response_node
                                  └─► END

On error in any node, we skip ahead to synthesize_response_node
to return a graceful error message rather than crashing.
"""

from functools import lru_cache

from langgraph.graph import StateGraph, END

from app.agent.state import AgentState
from app.agent.nodes.parse_intent import parse_intent_node
from app.agent.nodes.map_procedure import map_procedure_node
from app.agent.nodes.generate_sql import generate_sql_node
from app.agent.nodes.execute_sql import execute_sql_node
from app.agent.nodes.synthesize_response import synthesize_response_node
from app.core.logging import get_logger

log = get_logger(__name__)

NODE_PARSE_INTENT     = "parse_intent"
NODE_MAP_PROCEDURE    = "map_procedure"
NODE_GENERATE_SQL     = "generate_sql"
NODE_EXECUTE_SQL      = "execute_sql"
NODE_SYNTHESIZE       = "synthesize_response"


def _route_after_sql_gen(state: AgentState) -> str:
    """
    If SQL generation failed (no SQL produced), skip execute_sql and go straight
    to synthesis — which will handle the error gracefully.
    """
    if not state.get("generated_sql"):
        log.warning("No SQL generated — routing to synthesize directly")
        return NODE_SYNTHESIZE
    return NODE_EXECUTE_SQL


@lru_cache(maxsize=1)
def build_agent_graph():
    """
    Build and compile the LangGraph agent.
    Cached — only built once per process.
    """
    builder = StateGraph(AgentState)

    # ── Register nodes ─────────────────────────────────────
    builder.add_node(NODE_PARSE_INTENT,  parse_intent_node)
    builder.add_node(NODE_MAP_PROCEDURE, map_procedure_node)
    builder.add_node(NODE_GENERATE_SQL,  generate_sql_node)
    builder.add_node(NODE_EXECUTE_SQL,   execute_sql_node)
    builder.add_node(NODE_SYNTHESIZE,    synthesize_response_node)

    # ── Edges ──────────────────────────────────────────────
    builder.set_entry_point(NODE_PARSE_INTENT)

    builder.add_edge(NODE_PARSE_INTENT, NODE_MAP_PROCEDURE)
    builder.add_edge(NODE_MAP_PROCEDURE, NODE_GENERATE_SQL)

    # Conditional: if SQL was generated → execute; else → synthesize (error)
    builder.add_conditional_edges(
        NODE_GENERATE_SQL,
        _route_after_sql_gen,
        {
            NODE_EXECUTE_SQL: NODE_EXECUTE_SQL,
            NODE_SYNTHESIZE:  NODE_SYNTHESIZE,
        },
    )

    builder.add_edge(NODE_EXECUTE_SQL, NODE_SYNTHESIZE)
    builder.add_edge(NODE_SYNTHESIZE, END)

    graph = builder.compile()
    log.info("LangGraph agent compiled", nodes=5)
    return graph
