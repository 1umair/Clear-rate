"""
DuckDB schema initialization for the Master Price Graph.

Design principles:
- National-scale schema (state_code, zip_code, npi_number as primary geo identifiers)
- April 2026 CMS MRF schema compliant (in-network-rates v3.x)
- Partitioned for efficient state-level filtering in the MVP
- Run directly: `uv run python -m app.db.schema`
"""

from app.db.connection import get_db_connection
from app.core.logging import get_logger

log = get_logger(__name__)

# ── DDL ───────────────────────────────────────────────────

CREATE_MASTER_PRICE_GRAPH = """
CREATE TABLE IF NOT EXISTS master_price_graph (
    -- ── Ingestion metadata ─────────────────────────────
    ingest_id           VARCHAR     NOT NULL,         -- UUID per ingestion run
    file_id             VARCHAR     NOT NULL,         -- Source MRF file identifier
    file_url            VARCHAR,                      -- Source URL (for re-fetching)
    last_updated_on     DATE,                         -- MRF publication date
    ingested_at         TIMESTAMPTZ DEFAULT now(),    -- When we loaded it

    -- ── Reporting entity (hospital / network) ──────────
    reporting_entity_name   VARCHAR NOT NULL,
    reporting_entity_type   VARCHAR,                  -- hospital, health system, tpa, issuer
    ein                     VARCHAR,                  -- Employer Identification Number

    -- ── Insurance plan ─────────────────────────────────
    plan_name           VARCHAR,
    plan_id             VARCHAR,                      -- CMS plan ID
    plan_id_type        VARCHAR,                      -- EIN | HIOS
    plan_market_type    VARCHAR,                      -- individual | small_group | large_group | self_insured

    -- ── Procedure / billing code ───────────────────────
    billing_code_type   VARCHAR     NOT NULL,         -- CPT | HCPCS | MS-DRG | APC | RC | ICD | NDC
    billing_code        VARCHAR     NOT NULL,
    billing_code_type_version   VARCHAR,              -- ICD version, DRG year, etc.
    name                VARCHAR,                      -- Raw procedure name from MRF
    normalized_name     VARCHAR,                      -- Standardized by The Normalizer (proprietary)
    description         VARCHAR,                      -- Extended description

    -- ── Rate ───────────────────────────────────────────
    negotiated_type     VARCHAR     NOT NULL,         -- negotiated | derived | fee schedule | percent of billed charges | per diem | case rate
    negotiated_rate     DOUBLE      NOT NULL,
    expiration_date     DATE,
    service_code        VARCHAR[],                    -- Place of service codes (array)
    billing_class       VARCHAR,                      -- professional | institutional
    additional_generic_notes    VARCHAR,

    -- ── Provider (NPI-level granularity) ───────────────
    npi                 VARCHAR[],                    -- Array of NPI numbers
    provider_group_id   BIGINT,
    tin_type            VARCHAR,                      -- EIN | NPI
    tin_value           VARCHAR,                      -- Tax ID value

    -- ── Geography ──────────────────────────────────────
    state_code          VARCHAR(2)  NOT NULL,         -- Primary geo filter (e.g. 'VA')
    zip_code            VARCHAR(10),
    city                VARCHAR,
    county              VARCHAR,

    -- ── Quality flags ──────────────────────────────────
    is_current          BOOLEAN     DEFAULT true,     -- False when superseded by newer MRF
    data_quality_score  SMALLINT,                     -- 0-100 (future use)
    schema_version      VARCHAR,                      -- CMS schema version of source file
);
"""

CREATE_PROVIDER_INDEX = """
CREATE TABLE IF NOT EXISTS provider_index (
    npi             VARCHAR     PRIMARY KEY,
    name            VARCHAR,
    taxonomy_code   VARCHAR,
    taxonomy_desc   VARCHAR,
    address_line1   VARCHAR,
    address_line2   VARCHAR,
    city            VARCHAR,
    state_code      VARCHAR(2),
    zip_code        VARCHAR(10),
    phone           VARCHAR,
    -- NOTE: No PII stored — NPI registry is public data
    last_synced     TIMESTAMPTZ DEFAULT now(),
);
"""

CREATE_MRF_INDEX = """
CREATE TABLE IF NOT EXISTS mrf_index (
    id              VARCHAR PRIMARY KEY,
    network_id      VARCHAR     NOT NULL,
    network_name    VARCHAR     NOT NULL,
    state_code      VARCHAR(2)  NOT NULL,
    index_url       VARCHAR     NOT NULL,
    file_url        VARCHAR     NOT NULL,
    plan_name       VARCHAR,
    file_type       VARCHAR,                          -- in-network-rates | allowed-amounts | provider-reference
    file_size_bytes BIGINT,
    last_checked_at TIMESTAMPTZ,
    last_ingested_at TIMESTAMPTZ,
    checksum_sha256 VARCHAR,
    is_active       BOOLEAN DEFAULT true,
);
"""

# ── Indexes for common query patterns ─────────────────────

INDEXES = [
    # State-level filtering (MVP — VA only, but national-scale design)
    "CREATE INDEX IF NOT EXISTS idx_mpg_state ON master_price_graph (state_code)",

    # Billing code lookup (direct code queries)
    "CREATE INDEX IF NOT EXISTS idx_mpg_billing_code ON master_price_graph (billing_code, billing_code_type)",

    # Network filtering
    "CREATE INDEX IF NOT EXISTS idx_mpg_network ON master_price_graph (reporting_entity_name)",

    # Normalized name search (text queries via The Normalizer)
    "CREATE INDEX IF NOT EXISTS idx_mpg_normalized_name ON master_price_graph (normalized_name)",

    # Rate filtering / range queries
    "CREATE INDEX IF NOT EXISTS idx_mpg_rate ON master_price_graph (negotiated_rate)",

    # Composite: state + billing code (most common agent query)
    "CREATE INDEX IF NOT EXISTS idx_mpg_state_code ON master_price_graph (state_code, billing_code)",

    # Provider lookup
    "CREATE INDEX IF NOT EXISTS idx_mpg_npi ON master_price_graph (state_code)",
]


# ── Views ─────────────────────────────────────────────────

CREATE_VA_SUMMARY_VIEW = """
CREATE OR REPLACE VIEW va_price_summary AS
SELECT
    reporting_entity_name                               AS network,
    normalized_name,
    billing_code_type || ' ' || billing_code            AS code,
    billing_class,
    COUNT(*)                                            AS rate_count,
    ROUND(MIN(negotiated_rate), 2)                      AS min_rate,
    ROUND(MAX(negotiated_rate), 2)                      AS max_rate,
    ROUND(MEDIAN(negotiated_rate), 2)                   AS median_rate,
    ROUND(AVG(negotiated_rate), 2)                      AS avg_rate,
    ROUND(STDDEV(negotiated_rate), 2)                   AS stddev_rate
FROM master_price_graph
WHERE state_code = 'VA'
  AND is_current = true
  AND negotiated_type = 'negotiated'
GROUP BY 1, 2, 3, 4
ORDER BY code, network;
"""


# ── Entrypoint ────────────────────────────────────────────

def initialize_schema(drop_existing: bool = False) -> None:
    conn = get_db_connection()

    if drop_existing:
        log.warning("Dropping existing tables — data will be lost")
        conn.execute("DROP TABLE IF EXISTS master_price_graph CASCADE")
        conn.execute("DROP TABLE IF EXISTS provider_index CASCADE")
        conn.execute("DROP TABLE IF EXISTS mrf_index CASCADE")

    log.info("Creating tables...")
    conn.execute(CREATE_MASTER_PRICE_GRAPH)
    conn.execute(CREATE_PROVIDER_INDEX)
    conn.execute(CREATE_MRF_INDEX)

    log.info("Creating indexes...")
    for idx_sql in INDEXES:
        conn.execute(idx_sql)

    log.info("Creating views...")
    conn.execute(CREATE_VA_SUMMARY_VIEW)

    log.info("Schema initialization complete")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Initialize DuckDB schema")
    parser.add_argument("--drop", action="store_true", help="Drop existing tables first")
    args = parser.parse_args()
    initialize_schema(drop_existing=args.drop)
    print("Schema initialized successfully.")
