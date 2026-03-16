"""
Node 4: execute_sql_node
────────────────────────
Executes the generated SQL against DuckDB and returns results as a list of dicts.
Handles query errors gracefully and caps result size.
"""

from app.agent.state import AgentState
from app.core.logging import get_logger
from app.db.connection import get_db_connection

log = get_logger(__name__)

MAX_RESULT_ROWS = 500  # Hard cap — prevents memory issues with runaway queries


async def execute_sql_node(state: AgentState) -> AgentState:
    log.info("execute_sql_node", session=state.get("session_id"))

    sql = state.get("generated_sql")
    if not sql:
        return {
            **state,
            "query_results": [],
            "nodes_visited": state.get("nodes_visited", []) + ["execute_sql"],
            "error": "No SQL generated to execute",
        }

    try:
        conn = get_db_connection()
        relation = conn.execute(sql)
        columns = [desc[0] for desc in relation.description]
        rows = relation.fetchmany(MAX_RESULT_ROWS)

        results = [dict(zip(columns, row)) for row in rows]

        # Coerce non-serializable types (dates, decimals → str/float)
        results = _coerce_results(results)

        log.info("SQL executed", row_count=len(results), sql_preview=sql[:100])

        return {
            **state,
            "query_results": results,
            "nodes_visited": state.get("nodes_visited", []) + ["execute_sql"],
        }

    except Exception as e:
        log.error("execute_sql_node failed", error=str(e), sql=sql)
        return {
            **state,
            "query_results": [],
            "nodes_visited": state.get("nodes_visited", []) + ["execute_sql"],
            "error": f"SQL execution error: {e}",
        }


def _coerce_results(results: list[dict]) -> list[dict]:
    """Convert DuckDB-specific types to JSON-safe Python types."""
    import datetime
    import decimal

    coerced = []
    for row in results:
        clean = {}
        for k, v in row.items():
            if isinstance(v, (datetime.date, datetime.datetime)):
                clean[k] = v.isoformat()
            elif isinstance(v, decimal.Decimal):
                clean[k] = float(v)
            elif isinstance(v, bytes):
                clean[k] = v.decode("utf-8", errors="replace")
            else:
                clean[k] = v
        coerced.append(clean)
    return coerced
