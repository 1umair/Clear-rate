"""
DuckDB Loader
─────────────
Loads parsed MRF DataFrames into the master_price_graph table.
Uses DuckDB's native Polars/Arrow integration for high-throughput inserts.
"""

from pathlib import Path

import duckdb
import polars as pl
import structlog

log = structlog.get_logger(__name__)


def load_dataframe(
    df: pl.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    ingest_id: str,
) -> int:
    """
    Insert a Polars DataFrame batch into master_price_graph.
    Returns the number of rows inserted.
    """
    if df.is_empty():
        return 0

    try:
        # DuckDB can natively query Polars DataFrames registered as a view
        conn.register("_batch", df.to_arrow())
        conn.execute("""
            INSERT INTO master_price_graph
            SELECT
                ingest_id,
                file_id,
                file_url,
                TRY_CAST(last_updated_on AS DATE)   AS last_updated_on,
                now()                               AS ingested_at,
                reporting_entity_name,
                reporting_entity_type,
                ein,
                plan_name,
                NULL                                AS plan_id,
                NULL                                AS plan_id_type,
                plan_market_type,
                billing_code_type,
                billing_code,
                billing_code_type_version,
                name,
                normalized_name,
                description,
                negotiated_type,
                negotiated_rate,
                TRY_CAST(expiration_date AS DATE)   AS expiration_date,
                TRY_CAST(service_code AS VARCHAR[]) AS service_code,
                billing_class,
                additional_generic_notes,
                TRY_CAST(npi AS VARCHAR[])          AS npi,
                provider_group_id,
                tin_type,
                tin_value,
                state_code,
                NULL                                AS zip_code,
                NULL                                AS city,
                NULL                                AS county,
                true                                AS is_current,
                NULL                                AS data_quality_score,
                schema_version
            FROM _batch
        """)
        conn.unregister("_batch")
        return len(df)
    except Exception as e:
        log.error("Batch insert failed", error=str(e), rows=len(df))
        raise


def mark_superseded(
    conn: duckdb.DuckDBPyConnection,
    network_name: str,
    new_ingest_id: str,
) -> int:
    """
    Mark all previous records from a network as non-current
    after a successful new ingestion run.
    """
    result = conn.execute("""
        UPDATE master_price_graph
        SET is_current = false
        WHERE reporting_entity_name = ?
          AND ingest_id != ?
          AND is_current = true
    """, [network_name, new_ingest_id]).fetchone()

    rows_updated = result[0] if result else 0
    log.info("Superseded old records", network=network_name, count=rows_updated)
    return rows_updated


def export_to_parquet(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    state_code: str = "VA",
) -> Path:
    """
    Export current records to partitioned Parquet files for backup/sharing.
    Files are gitignored and never committed.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"master_price_graph_{state_code}.parquet"

    conn.execute(f"""
        COPY (
            SELECT * FROM master_price_graph
            WHERE state_code = '{state_code}'
              AND is_current = true
        )
        TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)

    size_mb = output_path.stat().st_size / (1024 ** 2)
    log.info("Exported to Parquet", path=str(output_path), size_mb=round(size_mb, 1))
    return output_path
