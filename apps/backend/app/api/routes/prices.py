"""
Direct price data endpoints (non-agent, structured queries).
"""

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from app.db.connection import get_db_connection
from app.core.logging import get_logger

router = APIRouter()
log = get_logger(__name__)


class PriceRecord(BaseModel):
    id: str
    network_name: str
    procedure_name: str
    normalized_name: str | None
    billing_code: str
    billing_code_type: str
    billing_class: str
    negotiated_type: str
    negotiated_rate: float
    plan_name: str
    city: str | None
    state_code: str
    zip_code: str | None
    last_updated: str


class PriceStats(BaseModel):
    min: float
    max: float
    median: float
    mean: float
    count: int


class PriceComparison(BaseModel):
    procedure: str
    normalized_name: str | None
    billing_code: str | None
    records: list[PriceRecord]
    stats: PriceStats


@router.get("/compare", response_model=PriceComparison)
async def compare_procedure(
    procedure: str = Query(..., description="Procedure name or CPT code"),
    state_code: str = Query(default="VA", description="State code filter"),
    limit: int = Query(default=50, le=200),
):
    """
    Compare negotiated rates for a procedure across all networks in a state.
    Uses fuzzy matching on normalized_name if an exact billing_code is not provided.
    """
    conn = get_db_connection()

    # Determine if query looks like a billing code
    is_code = procedure.replace("-", "").isalnum() and len(procedure) <= 8

    try:
        if is_code:
            query = """
                SELECT
                    gen_random_uuid()::VARCHAR AS id,
                    reporting_entity_name        AS network_name,
                    name                         AS procedure_name,
                    normalized_name,
                    billing_code,
                    billing_code_type,
                    billing_class,
                    negotiated_type,
                    negotiated_rate,
                    plan_name,
                    city,
                    state_code,
                    zip_code,
                    last_updated_on::VARCHAR     AS last_updated
                FROM master_price_graph
                WHERE state_code = ?
                  AND billing_code = ?
                ORDER BY negotiated_rate
                LIMIT ?
            """
            rows = conn.execute(query, [state_code.upper(), procedure.upper(), limit]).fetchall()
            cols = [d[0] for d in conn.description]
        else:
            # Text search on normalized_name
            query = """
                SELECT
                    gen_random_uuid()::VARCHAR AS id,
                    reporting_entity_name        AS network_name,
                    name                         AS procedure_name,
                    normalized_name,
                    billing_code,
                    billing_code_type,
                    billing_class,
                    negotiated_type,
                    negotiated_rate,
                    plan_name,
                    city,
                    state_code,
                    zip_code,
                    last_updated_on::VARCHAR     AS last_updated
                FROM master_price_graph
                WHERE state_code = ?
                  AND (
                      lower(normalized_name) LIKE lower(?)
                      OR lower(name) LIKE lower(?)
                  )
                ORDER BY negotiated_rate
                LIMIT ?
            """
            pattern = f"%{procedure}%"
            rows = conn.execute(query, [state_code.upper(), pattern, pattern, limit]).fetchall()
            cols = [d[0] for d in conn.description]

        if not rows:
            raise HTTPException(status_code=404, detail=f"No records found for '{procedure}' in {state_code}")

        records = [PriceRecord(**dict(zip(cols, row))) for row in rows]
        rates = [r.negotiated_rate for r in records]
        sorted_rates = sorted(rates)
        n = len(sorted_rates)

        stats = PriceStats(
            min=sorted_rates[0],
            max=sorted_rates[-1],
            median=sorted_rates[n // 2],
            mean=sum(sorted_rates) / n,
            count=n,
        )

        return PriceComparison(
            procedure=procedure,
            normalized_name=records[0].normalized_name,
            billing_code=records[0].billing_code if is_code else None,
            records=records,
            stats=stats,
        )

    except HTTPException:
        raise
    except Exception as e:
        log.error("Price compare error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
