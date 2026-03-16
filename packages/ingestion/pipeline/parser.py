"""
CMS MRF Parser — April 2026 Schema Compliant
─────────────────────────────────────────────
Parses CMS in-network-rates JSON files (which can be 1-50GB+) using
streaming JSON parsing (ijson) to avoid loading the entire file into memory.

April 2026 CMS in-network-rates schema structure:
{
  "reporting_entity_name": "...",
  "reporting_entity_type": "...",
  "last_updated_on": "YYYY-MM-DD",
  "version": "3.x.x",
  "in_network": [
    {
      "negotiation_arrangement": "ffs" | "bundle" | "capitation",
      "name": "...",
      "billing_code_type": "CPT" | "HCPCS" | "MS-DRG" | ...,
      "billing_code_type_version": "...",
      "billing_code": "...",
      "description": "...",
      "negotiated_rates": [
        {
          "provider_references": [0, 1, ...],
          "negotiated_prices": [
            {
              "negotiated_type": "negotiated" | "derived" | ...,
              "negotiated_rate": 123.45,
              "expiration_date": "YYYY-MM-DD",
              "service_code": ["11", "21"],
              "billing_class": "professional" | "institutional",
              "additional_generic_notes": "...",
              "billing_code_modifier": [...]
            }
          ]
        }
      ]
    }
  ],
  "provider_references": [
    {
      "provider_group_id": 0,
      "npi": [1234567890, ...],
      "tin": {"type": "ein", "value": "..."}
    }
  ]
}
"""

import gzip
import json
from pathlib import Path
from typing import Iterator

import ijson
import polars as pl
import structlog

from pipeline.downloader import MRFFile

log = structlog.get_logger(__name__)

# ── Output schema (aligns with master_price_graph table) ──

RECORD_SCHEMA = {
    "ingest_id":                pl.Utf8,
    "file_id":                  pl.Utf8,
    "file_url":                 pl.Utf8,
    "last_updated_on":          pl.Utf8,      # Date string → cast in loader
    "reporting_entity_name":    pl.Utf8,
    "reporting_entity_type":    pl.Utf8,
    "ein":                      pl.Utf8,
    "plan_name":                pl.Utf8,
    "plan_market_type":         pl.Utf8,
    "billing_code_type":        pl.Utf8,
    "billing_code":             pl.Utf8,
    "billing_code_type_version":pl.Utf8,
    "name":                     pl.Utf8,
    "normalized_name":          pl.Utf8,
    "description":              pl.Utf8,
    "negotiated_type":          pl.Utf8,
    "negotiated_rate":          pl.Float64,
    "expiration_date":          pl.Utf8,
    "service_code":             pl.Utf8,      # Serialized as JSON array string
    "billing_class":            pl.Utf8,
    "additional_generic_notes": pl.Utf8,
    "npi":                      pl.Utf8,      # Serialized as JSON array string
    "provider_group_id":        pl.Int64,
    "tin_type":                 pl.Utf8,
    "tin_value":                pl.Utf8,
    "state_code":               pl.Utf8,
    "schema_version":           pl.Utf8,
}


def parse_mrf_file(
    file_path: Path,
    mrf_meta: MRFFile,
    ingest_id: str,
    batch_size: int = 50_000,
) -> Iterator[pl.DataFrame]:
    """
    Stream-parse a CMS MRF JSON (or .json.gz) file.
    Yields Polars DataFrames in batches of `batch_size` rows.

    Uses ijson for memory-efficient streaming — critical for 10GB+ files.
    """
    log.info("Parsing MRF file", path=str(file_path), network=mrf_meta.network_id)

    open_fn = gzip.open if str(file_path).endswith(".gz") else open
    records: list[dict] = []
    provider_ref_map: dict[int, dict] = {}
    header: dict = {}

    file_id = f"{mrf_meta.network_id}_{file_path.stem}"

    with open_fn(str(file_path), "rb") as f:  # type: ignore
        # ── Pass 1: Extract header fields and provider references ──
        # We need the provider_references map before processing in_network items
        # ijson prefix "provider_references.item" streams each provider group
        log.debug("Building provider reference map...")
        try:
            for group in ijson.items(f, "provider_references.item"):
                group_id = group.get("provider_group_id", -1)
                npi_list = group.get("npi", [])
                tin = group.get("tin", {})
                provider_ref_map[group_id] = {
                    "npi": [str(n) for n in npi_list],
                    "tin_type": tin.get("type", ""),
                    "tin_value": tin.get("value", ""),
                }
        except Exception:
            # Some files don't have provider_references at top-level
            pass

    with open_fn(str(file_path), "rb") as f:  # type: ignore
        # ── Pass 2: Extract header ──
        parser = ijson.parse(f)
        for prefix, event, value in parser:
            if prefix in ("reporting_entity_name", "reporting_entity_type",
                          "last_updated_on", "version"):
                header[prefix] = value
            if prefix == "in_network" and event == "start_array":
                break  # Stop at in_network array start

    with open_fn(str(file_path), "rb") as f:  # type: ignore
        # ── Pass 3: Stream in_network items ──
        for item in ijson.items(f, "in_network.item"):
            billing_code_type = item.get("billing_code_type", "UNKNOWN")
            billing_code = str(item.get("billing_code", ""))
            billing_code_version = item.get("billing_code_type_version", "")
            procedure_name = item.get("name", "")
            description = item.get("description", "")

            for rate_entry in item.get("negotiated_rates", []):
                # Resolve provider references to NPI arrays
                provider_refs = rate_entry.get("provider_references", [])
                npis: list[str] = []
                tin_type = ""
                tin_value = ""
                provider_group_id = -1

                for ref_id in provider_refs:
                    ref = provider_ref_map.get(ref_id, {})
                    npis.extend(ref.get("npi", []))
                    if not tin_type:
                        tin_type = ref.get("tin_type", "")
                        tin_value = ref.get("tin_value", "")
                        provider_group_id = ref_id

                for price in rate_entry.get("negotiated_prices", []):
                    negotiated_rate = price.get("negotiated_rate")
                    if negotiated_rate is None:
                        continue  # Skip missing rates

                    record = {
                        "ingest_id":                ingest_id,
                        "file_id":                  file_id,
                        "file_url":                 mrf_meta.url,
                        "last_updated_on":          header.get("last_updated_on", ""),
                        "reporting_entity_name":    header.get("reporting_entity_name", mrf_meta.network_name),
                        "reporting_entity_type":    header.get("reporting_entity_type", ""),
                        "ein":                      tin_value if tin_type == "ein" else "",
                        "plan_name":                mrf_meta.plan_name,
                        "plan_market_type":         mrf_meta.plan_market_type,
                        "billing_code_type":        billing_code_type,
                        "billing_code":             billing_code,
                        "billing_code_type_version":billing_code_version,
                        "name":                     procedure_name,
                        "normalized_name":          None,  # Set by The Normalizer post-load
                        "description":              description,
                        "negotiated_type":          price.get("negotiated_type", ""),
                        "negotiated_rate":          float(negotiated_rate),
                        "expiration_date":          price.get("expiration_date", ""),
                        "service_code":             json.dumps(price.get("service_code", [])),
                        "billing_class":            price.get("billing_class", ""),
                        "additional_generic_notes": price.get("additional_generic_notes", ""),
                        "npi":                      json.dumps(npis[:50]),  # Cap NPI list
                        "provider_group_id":        provider_group_id,
                        "tin_type":                 tin_type,
                        "tin_value":                tin_value,
                        "state_code":               mrf_meta.state_code,
                        "schema_version":           header.get("version", "unknown"),
                    }
                    records.append(record)

                    if len(records) >= batch_size:
                        yield _to_dataframe(records)
                        records = []

    if records:
        yield _to_dataframe(records)

    log.info("Parsing complete", network=mrf_meta.network_id, file=file_path.name)


def _to_dataframe(records: list[dict]) -> pl.DataFrame:
    """Convert a batch of record dicts to a typed Polars DataFrame."""
    df = pl.from_dicts(records, schema=RECORD_SCHEMA, infer_schema_length=None)
    # Basic quality filter — exclude $0 rates and unreasonably high rates
    df = df.filter(
        (pl.col("negotiated_rate") > 0) &
        (pl.col("negotiated_rate") < 10_000_000)
    )
    return df
