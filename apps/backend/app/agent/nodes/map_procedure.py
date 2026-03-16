"""
Node 2: map_procedure_node
──────────────────────────
Maps natural language procedure terms to billing codes.

In production this delegates to The Normalizer (proprietary NLP model).
In MVP/fallback mode, it uses Claude to make a best-effort code suggestion
and then validates against what's actually in the database.

The Normalizer interface (proprietary — not in this repo):
    from normalizer import ProcedureNormalizer
    normalizer = ProcedureNormalizer.load("./models/v2")
    results = normalizer.map(terms)
"""

import json

from anthropic import AsyncAnthropic
from app.agent.state import AgentState, ProcedureCode
from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.connection import get_db_connection

log = get_logger(__name__)

SYSTEM_PROMPT = """You are a medical coding expert. Map procedure descriptions to their billing codes.

For each procedure term, provide:
- billing_code: the most likely CPT, HCPCS, or MS-DRG code
- billing_code_type: CPT | HCPCS | MS-DRG | APC | RC | ICD | NDC
- normalized_name: standardized procedure name
- confidence: 0.0-1.0 (1.0 = very confident, 0.5 = likely match)

If the input IS already a billing code (e.g. "93454", "99213"), return it as-is with confidence 1.0.

Respond ONLY with a JSON array. No markdown.

Example:
[
  {"billing_code": "70553", "billing_code_type": "CPT", "normalized_name": "MRI Brain with and without contrast", "confidence": 0.95},
  {"billing_code": "27447", "billing_code_type": "CPT", "normalized_name": "Total Knee Arthroplasty", "confidence": 0.9}
]
"""


async def map_procedure_node(state: AgentState) -> AgentState:
    log.info("map_procedure_node", session=state.get("session_id"))

    intent = state.get("intent")
    if not intent or not intent.get("procedure_terms"):
        # No procedure terms to map — proceed with empty codes (SQL gen will handle)
        return {
            **state,
            "procedure_codes": [],
            "nodes_visited": state.get("nodes_visited", []) + ["map_procedure"],
        }

    settings = get_settings()

    # ── Try The Normalizer first (proprietary — optional dep) ─
    codes = await _try_normalizer(intent["procedure_terms"])

    # ── Fallback: Claude-based mapping ────────────────────────
    if not codes:
        codes = await _claude_mapping(intent["procedure_terms"], settings)

    # ── Validate codes exist in DB ─────────────────────────────
    codes = _validate_codes_in_db(codes, state.get("state_code", "VA"))

    log.info("Procedures mapped", count=len(codes),
             codes=[c["billing_code"] for c in codes])

    return {
        **state,
        "procedure_codes": codes,
        "nodes_visited": state.get("nodes_visited", []) + ["map_procedure"],
    }


async def _try_normalizer(terms: list[str]) -> list[ProcedureCode]:
    """Attempt to use the proprietary Normalizer. Returns [] if not available."""
    try:
        from normalizer import ProcedureNormalizer  # type: ignore[import]
        normalizer = ProcedureNormalizer.load()
        return normalizer.map(terms)
    except ImportError:
        log.debug("Normalizer not available — using Claude fallback")
        return []


async def _claude_mapping(terms: list[str], settings) -> list[ProcedureCode]:
    """Use Claude as a fallback for procedure-to-code mapping."""
    client = AsyncAnthropic(api_key=settings.anthropic_api_key)
    try:
        resp = await client.messages.create(
            model=settings.anthropic_model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": "\n".join(terms)}],
        )
        raw = resp.content[0].text.strip()
        mapped = json.loads(raw)
        return [
            ProcedureCode(
                billing_code=item["billing_code"],
                billing_code_type=item["billing_code_type"],
                normalized_name=item["normalized_name"],
                confidence=float(item.get("confidence", 0.7)),
            )
            for item in mapped
        ]
    except Exception as e:
        log.warning("Claude procedure mapping failed", error=str(e))
        return []


def _validate_codes_in_db(codes: list[ProcedureCode], state_code: str | None) -> list[ProcedureCode]:
    """
    Filter/confirm codes that actually exist in the database.
    Returns original list if DB check fails (graceful degradation).
    """
    if not codes:
        return codes
    try:
        conn = get_db_connection()
        billing_codes = [c["billing_code"] for c in codes]
        placeholders = ", ".join(["?" for _ in billing_codes])
        params = billing_codes + ([state_code] if state_code else [])
        state_filter = f"AND state_code = ?" if state_code else ""

        result = conn.execute(
            f"""
            SELECT DISTINCT billing_code
            FROM master_price_graph
            WHERE billing_code IN ({placeholders})
            {state_filter}
            """,
            params,
        ).fetchall()

        found = {row[0] for row in result}
        # Keep codes found in DB; keep high-confidence codes that aren't yet (may be loading)
        return [c for c in codes if c["billing_code"] in found or c["confidence"] >= 0.9]
    except Exception:
        return codes  # Graceful fallback — let SQL gen try anyway
