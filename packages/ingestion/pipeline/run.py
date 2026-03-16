"""
Main ingestion runner.
─────────────────────
Orchestrates: TOC fetch → download → parse → load for a given market.

Usage:
    uv run python -m pipeline.run --market va
    uv run python -m pipeline.run --market va --network inova --force-refresh
"""

import argparse
import asyncio
import uuid
from pathlib import Path

import duckdb
import structlog
import yaml

from pipeline.downloader import fetch_toc, download_all, MRFFile
from pipeline.parser import parse_mrf_file
from pipeline.loader import load_dataframe, mark_superseded

log = structlog.get_logger(__name__)

REPO_ROOT = Path(__file__).parent.parent.parent.parent
DATA_RAW_DIR = REPO_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = REPO_ROOT / "data" / "processed"
CONFIG_DIR = Path(__file__).parent.parent / "config"


def load_market_config(market_id: str) -> dict:
    config_path = CONFIG_DIR / f"sources_{market_id}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"No config found for market '{market_id}' at {config_path}")
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_duckdb_connection(duckdb_path: str | None = None) -> duckdb.DuckDBPyConnection:
    import os
    path = duckdb_path or os.environ.get("DUCKDB_PATH", str(REPO_ROOT / "data" / "price_graph.duckdb"))
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(path)
    conn.execute("PRAGMA threads=4")
    conn.execute("PRAGMA memory_limit='4GB'")
    return conn


async def run_market_ingestion(
    market_id: str,
    network_ids: list[str] | None = None,
    force_refresh: bool = False,
    duckdb_path: str | None = None,
) -> dict:
    """
    Full ingestion pipeline for a market.
    Returns stats dict: {files_processed, records_loaded, errors}.
    """
    ingest_id = str(uuid.uuid4())
    log.info("Starting ingestion", market=market_id, ingest_id=ingest_id)

    config = load_market_config(market_id)
    ingestion_cfg = config.get("ingestion", {})
    networks = config.get("networks", [])
    state_code = config["market"]["state_code"]

    # Filter to specified networks if provided
    if network_ids:
        networks = [n for n in networks if n["id"] in network_ids]

    conn = get_duckdb_connection(duckdb_path)

    total_files = 0
    total_records = 0
    errors: list[str] = []

    for network in networks:
        network_id = network["id"]
        network_name = network["name"]
        index_url = network.get("index_url", "")

        if not index_url:
            log.warning("No index URL configured", network=network_id)
            continue

        # ── Step 1: Fetch TOC ──────────────────────────────
        mrf_files: list[MRFFile] = await fetch_toc(
            network_id=network_id,
            network_name=network_name,
            state_code=state_code,
            index_url=index_url,
            skip_plan_market_types=ingestion_cfg.get("skip_plan_market_types", []),
        )

        if not mrf_files:
            log.warning("No MRF files found in TOC", network=network_id)
            continue

        # ── Step 2: Download MRF files ────────────────────
        raw_dir = DATA_RAW_DIR / state_code.lower() / network_id
        download_results = await download_all(
            mrf_files=mrf_files,
            output_dir=raw_dir,
            max_concurrent=int(ingestion_cfg.get("max_concurrent_downloads", 3)),
            max_size_bytes=int(ingestion_cfg.get("max_file_size_bytes", 5 * 1024 ** 3)),
            force_refresh=force_refresh,
        )

        # ── Step 3: Parse + Load ──────────────────────────
        for mrf_file, local_path in download_results:
            if local_path is None:
                errors.append(f"Download failed: {mrf_file.url}")
                continue

            try:
                file_records = 0
                for batch_df in parse_mrf_file(
                    file_path=local_path,
                    mrf_meta=mrf_file,
                    ingest_id=ingest_id,
                    batch_size=50_000,
                ):
                    rows = load_dataframe(batch_df, conn, ingest_id)
                    file_records += rows
                    total_records += rows
                    log.info("Batch loaded", network=network_id, batch_rows=rows,
                             total=total_records)

                total_files += 1
                log.info("File ingested", network=network_id,
                         file=local_path.name, records=file_records)

            except Exception as e:
                error_msg = f"Parse/load error for {local_path.name}: {e}"
                log.error(error_msg)
                errors.append(error_msg)

        # ── Step 4: Mark old records as superseded ────────
        if total_records > 0:
            mark_superseded(conn, network_name, ingest_id)

    conn.close()

    stats = {
        "ingest_id": ingest_id,
        "market_id": market_id,
        "files_processed": total_files,
        "records_loaded": total_records,
        "error_count": len(errors),
        "errors": errors,
    }
    log.info("Ingestion complete", **{k: v for k, v in stats.items() if k != "errors"})
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run CMS MRF ingestion pipeline")
    parser.add_argument("--market", required=True, help="Market ID (e.g. va)")
    parser.add_argument("--network", nargs="*", help="Specific network IDs (default: all)")
    parser.add_argument("--force-refresh", action="store_true", help="Re-download existing files")
    parser.add_argument("--duckdb-path", help="Path to DuckDB database file")
    args = parser.parse_args()

    asyncio.run(
        run_market_ingestion(
            market_id=args.market,
            network_ids=args.network,
            force_refresh=args.force_refresh,
            duckdb_path=args.duckdb_path,
        )
    )
