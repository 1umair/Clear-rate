"""
Node 3: generate_sql_node
─────────────────────────
Generates a DuckDB SQL query from the parsed intent and procedure codes.

CRITICAL SECURITY RULES enforced here:
1. WHERE state_code = '{state_code}' is ALWAYS injected unless intent explicitly
   says geographic_scope = "national" AND state_code is None.
2. SQL is validated to be read-only (SELECT only — no INSERT/UPDATE/DELETE/DROP).
3. Results are capped at 500 rows to prevent runaway queries.
"""

import re

from anthropic import AsyncAnthropic
from app.agent.state import AgentState
from app.core.config import get_settings
from app.core.logging import get_logger

log = get_logger(__name__)

# These are the columns the LLM knows about — limits hallucination
SCHEMA_DESCRIPTION = """
Table: master_price_graph
Columns (only use columns listed here — others may be NULL):
  reporting_entity_name  VARCHAR    -- Hospital name from MRF (e.g. 'Inova Fairfax Hospital')
                                      Use ILIKE '%Inova%' to match Inova hospitals broadly.
  billing_code_type      VARCHAR    -- Code type: 'CPT' | 'HCPCS' | 'MS-DRG' | 'APR-DRG' | 'RC' | 'APC' | 'NDC'
  billing_code           VARCHAR    -- The code value e.g. '70553', '27447', '99213'
  name                   VARCHAR    -- Raw procedure name from MRF (use ILIKE for text search)
  negotiated_type        VARCHAR    -- 'negotiated' | 'bundle' | 'percentage' | 'per_diem' | 'other'
  negotiated_rate        DOUBLE     -- Negotiated dollar amount
  plan_name              VARCHAR    -- Payer and plan e.g. 'Aetna — Hmo', 'United — Options'
  billing_class          VARCHAR    -- 'professional' | 'institutional'
  state_code             VARCHAR    -- 'VA' (Virginia) | 'MD' (Maryland) | 'DC' (Washington DC)
  is_current             BOOLEAN    -- true = current rate, false = superseded
  last_updated_on        DATE       -- Date the MRF was last updated

IMPORTANT — columns that are NULL in current data (DO NOT filter on these):
  city, zip_code, normalized_name, expiration_date

Data notes:
  - Current data: 30 hospitals across 4 networks in the DC Metro region
  - Inova Health (VA): 'Inova Fairfax Hospital', 'Inova Alexandria Hospital',
    'Inova Fair Oaks Hospital', 'Inova Loudoun Hospital', 'Inova Mount Vernon Hospital'
  - HCA Virginia (VA): 'Chippenham Hospital', 'Dominion Hospital', 'Henrico Doctors Hospital',
    'Johnston-Willis Hospital', 'Reston Hospital Center', 'Spotsylvania Regional Medical Center',
    'StoneSprings Hospital Center', 'LewisGale Medical Center', 'LewisGale Hospital Montgomery',
    'LewisGale Hospital Alleghany', 'LewisGale Hospital Pulaski'
  - UVA Health (VA): 'UVA Health Medical Center', 'UVA Culpeper Medical Center',
    'UVA Haymarket Medical Center', 'UVA Prince William Medical Center'
  - MedStar Health (DC + MD): 'MedStar Washington Hospital Center', 'MedStar Georgetown University Hospital',
    'MedStar National Rehabilitation Hospital', 'MedStar Franklin Square Medical Center',
    'MedStar Good Samaritan Hospital', 'MedStar Harbor Hospital', 'MedStar Montgomery Medical Center',
    'MedStar Southern Maryland Hospital Center', "MedStar St. Mary's Hospital", 'MedStar Union Memorial Hospital'
  - Billing codes: HCPCS, APR-DRG (inpatient bundles), CPT, MS-DRG, RC
  - To search by procedure name: use ILIKE '%keyword%' on the `name` column
  - To search by code: filter billing_code = 'XXXXX'
  - To filter by network: use ILIKE on reporting_entity_name (e.g. '%Inova%', '%MedStar%')
  - To compare across networks: GROUP BY reporting_entity_name
  - Multiple payer/plans per procedure — use GROUP BY plan_name for payer comparisons

Useful DuckDB functions:
  MEDIAN(col)            -- Use for median rates
  ROUND(x, 2)            -- Round to 2 decimal places
"""

SYSTEM_PROMPT = f"""You are a DuckDB SQL expert for a healthcare price transparency platform.

{SCHEMA_DESCRIPTION}

Rules:
1. ALWAYS filter WHERE is_current = true
2. ALWAYS cap results with LIMIT (max 500)
3. Write DuckDB-compatible SQL only (no MySQL/Postgres-specific syntax)
4. Use ILIKE for case-insensitive text matching on name and reporting_entity_name
5. Prefer MEDIAN() over AVG() for rate comparisons (median is more meaningful for skewed pricing data)
6. When comparing networks or payers, GROUP BY the relevant column
7. Round all monetary values to 2 decimal places
8. Output ONLY the SQL query — no markdown, no explanation, no semicolons at end
9. NEVER filter on city, zip_code, or normalized_name — these columns are NULL in current data
10. To find procedures: use ILIKE '%keyword%' on the `name` column
11. To find a hospital: use ILIKE '%hospital-name%' on reporting_entity_name

The WHERE state_code filter will be injected automatically — do NOT include it yourself.
"""


# ── Read-only guard ────────────────────────────────────────

WRITE_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|MERGE|COPY)\b",
    re.IGNORECASE,
)


def _is_safe_sql(sql: str) -> bool:
    """Reject any SQL containing write operations."""
    return not bool(WRITE_PATTERN.search(sql))


def _inject_state_filter(sql: str, state_code: str | None) -> str:
    """
    Inject WHERE state_code = '...' into the query.
    Handles both queries that already have a WHERE clause and those that don't.
    """
    if not state_code:
        return sql  # National query — no geo filter

    state_filter = f"state_code = '{state_code}'"

    # If query has a WHERE clause, append with AND
    if re.search(r"\bWHERE\b", sql, re.IGNORECASE):
        # Insert after the last WHERE
        sql = re.sub(
            r"(\bWHERE\b)",
            f"\\1 {state_filter} AND",
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
    else:
        # Insert before GROUP BY / ORDER BY / LIMIT or at end
        for keyword in ["GROUP BY", "ORDER BY", "LIMIT", "HAVING"]:
            pattern = re.compile(rf"\b{keyword}\b", re.IGNORECASE)
            match = pattern.search(sql)
            if match:
                idx = match.start()
                sql = sql[:idx] + f"\nWHERE {state_filter}\n" + sql[idx:]
                break
        else:
            sql = sql.rstrip() + f"\nWHERE {state_filter}"

    return sql


async def generate_sql_node(state: AgentState) -> AgentState:
    log.info("generate_sql_node", session=state.get("session_id"))
    settings = get_settings()
    client = AsyncAnthropic(api_key=settings.anthropic_api_key)

    intent = state.get("intent", {})
    procedure_codes = state.get("procedure_codes", [])
    state_code = state.get("state_code")  # Injected by the API route

    # Build context for the LLM
    context_parts = [f"User query: {state['user_query']}"]

    if intent:
        context_parts.append(f"Intent type: {intent.get('intent_type', 'general')}")
        if intent.get("networks"):
            context_parts.append(f"Networks of interest: {', '.join(intent['networks'])}")
        if intent.get("city_filter"):
            context_parts.append(f"City filter: {intent['city_filter']}")

    if procedure_codes:
        code_strs = [
            f"{c['billing_code_type']} {c['billing_code']} ({c['normalized_name']})"
            for c in procedure_codes
        ]
        context_parts.append(f"Procedure codes identified: {'; '.join(code_strs)}")
    elif intent and intent.get("procedure_terms"):
        context_parts.append(
            f"Procedure terms (no codes mapped): {', '.join(intent['procedure_terms'])}"
            " — use ILIKE '%term%' against normalized_name"
        )

    if intent and intent.get("requires_comparison"):
        context_parts.append("This requires comparison across multiple groups — use GROUP BY.")

    user_message = "\n".join(context_parts)

    try:
        response = await client.messages.create(
            model=settings.anthropic_model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )

        sql = response.content[0].text.strip().rstrip(";")

        # Safety check
        if not _is_safe_sql(sql):
            log.warning("Unsafe SQL rejected", sql=sql)
            sql = "SELECT 'Query rejected: non-SELECT operations not permitted' AS error"

        # Inject mandatory state filter
        sql = _inject_state_filter(sql, state_code)

        log.info("SQL generated", sql_preview=sql[:200])

        return {
            **state,
            "generated_sql": sql,
            "nodes_visited": state.get("nodes_visited", []) + ["generate_sql"],
        }

    except Exception as e:
        log.error("generate_sql_node failed", error=str(e))
        return {
            **state,
            "generated_sql": None,
            "nodes_visited": state.get("nodes_visited", []) + ["generate_sql"],
            "error": str(e),
        }
