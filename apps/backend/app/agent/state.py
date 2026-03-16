"""
LangGraph AgentState — the shared state object that flows through all 5 nodes.

Design decisions:
- TypedDict for LangGraph compatibility
- All fields Optional to allow partial state at any node entry
- nodes_visited tracks execution path for observability
- error propagates failures cleanly through the graph
"""

from typing import Any, Optional, TypedDict


class ProcedureCode(TypedDict):
    billing_code: str
    billing_code_type: str          # CPT | HCPCS | MS-DRG | APC | RC | ICD | NDC
    confidence: float               # 0.0 - 1.0 from The Normalizer
    normalized_name: str


class ParsedIntent(TypedDict):
    intent_type: str                # compare | lookup | cost_model | network_design | general
    networks: list[str]             # e.g. ["Inova", "HCA Virginia"]
    procedure_terms: list[str]      # Raw procedure terms from query
    geographic_scope: str           # "state" | "city" | "zip" | "national"
    city_filter: Optional[str]
    zip_filter: Optional[str]
    plan_filter: Optional[str]
    time_range: Optional[str]
    requires_aggregation: bool
    requires_comparison: bool


class AgentState(TypedDict):
    # ── Input ──────────────────────────────────────────
    user_query: str
    state_code: Optional[str]       # e.g. "VA" — injected by the API route
    session_id: str

    # ── Conversation history (for multi-turn) ──────────
    messages: list[dict]

    # ── Node outputs (built up progressively) ──────────
    intent: Optional[ParsedIntent]
    procedure_codes: list[ProcedureCode]
    generated_sql: Optional[str]
    query_results: Optional[list[dict[str, Any]]]
    final_answer: Optional[str]

    # ── Observability ───────────────────────────────────
    nodes_visited: list[str]
    error: Optional[str]
