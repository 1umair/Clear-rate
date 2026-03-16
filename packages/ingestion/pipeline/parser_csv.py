"""
CMS Standard Charges CSV Parser
────────────────────────────────
Parses the CMS-mandated hospital standard charges CSV file (45 CFR 180.50).

File layout:
  Row 1  — metadata field names  (hospital_name, last_updated_on, version, ...)
  Row 2  — metadata values
  Row 3  — data column headers
  Row 4+ — one procedure per row

Each procedure row has dynamic payer/plan columns:
  standard_charge|{payer}|{plan}|negotiated_dollar  → the rate
  standard_charge|{payer}|{plan}|methodology        → negotiation type
  standard_charge|{payer}|{plan}|negotiated_percentage → if percent-based

Outputs records matching the master_price_graph schema (same as parser.py).
"""

import csv
import io
import json
import uuid
from pathlib import Path
from typing import Iterator

import polars as pl
import structlog

log = structlog.get_logger(__name__)

# Columns always present in the data header row (row 3)
_STANDARD_COLS = {
    "description", "code|1", "code|1|type", "code|2", "code|2|type",
    "setting", "drug_unit_of_measurement", "drug_type_of_measurement",
    "modifiers", "standard_charge|gross", "standard_charge|discounted_cash",
    "standard_charge|min", "standard_charge|max",
}

RECORD_SCHEMA = {
    "ingest_id":                pl.Utf8,
    "file_id":                  pl.Utf8,
    "file_url":                 pl.Utf8,
    "last_updated_on":          pl.Utf8,
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
    "service_code":             pl.Utf8,
    "billing_class":            pl.Utf8,
    "additional_generic_notes": pl.Utf8,
    "npi":                      pl.Utf8,
    "provider_group_id":        pl.Int64,
    "tin_type":                 pl.Utf8,
    "tin_value":                pl.Utf8,
    "state_code":               pl.Utf8,
    "schema_version":           pl.Utf8,
}

_METHODOLOGY_MAP = {
    "fee schedule":                     "negotiated",
    "case rate":                        "bundle",
    "percent of total billed charges":  "percentage",
    "per diem":                         "per_diem",
    "other":                            "other",
}


def parse_standard_charges_csv(
    source: Path | str | bytes,
    hospital_name: str,
    ein: str,
    state_code: str,
    ingest_id: str,
    plan_market_type: str = "large_group",
    batch_size: int = 10_000,
) -> Iterator[pl.DataFrame]:
    """
    Stream-parse a CMS standard charges CSV file.
    `source` can be a file path, URL string (fetched), or raw bytes.
    Yields Polars DataFrames in batches.
    """
    raw: bytes
    if isinstance(source, bytes):
        raw = source
        file_url = f"memory://{hospital_name}"
        file_id = hospital_name.lower().replace(" ", "_")
    elif isinstance(source, Path):
        raw = source.read_bytes()
        file_url = str(source)
        file_id = source.stem
    else:
        import urllib.request
        log.info("Downloading CSV", url=source)
        req = urllib.request.Request(source, headers={"User-Agent": "ClearRate-Ingestion/1.0"})
        with urllib.request.urlopen(req, timeout=120) as r:
            raw = r.read()
        file_url = source
        file_id = source.split("/")[-1].replace(".csv", "")

    text = raw.decode("utf-8-sig", errors="replace")
    lines = text.splitlines()

    if len(lines) < 4:
        log.warning("CSV too short — skipping", lines=len(lines))
        return

    # ── Row 1-2: hospital metadata ──────────────────────────────────────────
    meta_reader = csv.reader([lines[0], lines[1]])
    meta_keys = next(meta_reader)
    meta_vals = next(meta_reader)
    meta = {k.strip(): v.strip() for k, v in zip(meta_keys, meta_vals)}

    last_updated_on = meta.get("last_updated_on", "").strip()
    schema_version = meta.get("version", "2.0.0").strip()
    reporting_entity = meta.get("hospital_name", hospital_name).strip() or hospital_name

    log.info(
        "Parsing standard charges CSV",
        hospital=reporting_entity,
        updated=last_updated_on,
        version=schema_version,
    )

    # ── Row 3: data column headers ──────────────────────────────────────────
    data_reader = csv.DictReader(lines[2:])   # lines[2] becomes the fieldnames row
    headers = data_reader.fieldnames or []

    # Detect format:
    # WIDE  — Inova style: one column per payer/plan  e.g. standard_charge|aetna|hmo|negotiated_dollar
    # TALL  — UVA style:   one row per payer/plan     with payer_name + plan_name columns
    payer_plan_pairs: list[tuple[str, str]] = []
    for h in headers:
        parts = h.split("|")
        if len(parts) == 4 and parts[0] == "standard_charge" and parts[3] == "negotiated_dollar":
            payer_plan_pairs.append((parts[1], parts[2]))

    is_tall_format = "payer_name" in headers and not payer_plan_pairs
    log.info(
        "Discovered format",
        fmt="tall" if is_tall_format else "wide",
        payer_plan_pairs=len(payer_plan_pairs),
    )

    records: list[dict] = []
    row_count = 0

    for row in data_reader:
        description = (row.get("description") or "").strip()
        code1 = (row.get("code|1") or "").strip()
        code1_type = (row.get("code|1|type") or "").strip().upper()
        code2 = (row.get("code|2") or "").strip()
        code2_type = (row.get("code|2|type") or "").strip().upper()
        code3 = (row.get("code|3") or "").strip()
        code3_type = (row.get("code|3|type") or "").strip().upper()
        setting = (row.get("setting") or "").strip()

        # Prefer CPT/HCPCS code; fall back to first code
        billing_code = ""
        billing_code_type = "OTHER"
        for code, ctype in [(code1, code1_type), (code2, code2_type), (code3, code3_type)]:
            if ctype in ("CPT", "HCPCS", "MS-DRG", "APR-DRG", "ICD-10-CM", "NDC") and code:
                billing_code = code
                billing_code_type = ctype
                break
        if not billing_code:
            billing_code = code1
            billing_code_type = code1_type or "OTHER"

        if not billing_code:
            continue

        billing_class = (
            "institutional" if setting in ("inpatient", "both")
            else "professional"
        )

        if is_tall_format:
            # TALL format: payer_name + plan_name are direct columns
            payer = (row.get("payer_name") or "").strip()
            plan = (row.get("plan_name") or "").strip()
            if not payer:
                continue
            dollar_str = (row.get("standard_charge|negotiated_dollar") or "").strip()
            methodology_raw = (row.get("standard_charge|methodology") or "").strip().lower()

            if not dollar_str:
                continue
            try:
                rate = float(dollar_str)
            except ValueError:
                continue
            if rate <= 0 or rate >= 10_000_000:
                continue

            negotiated_type = _METHODOLOGY_MAP.get(methodology_raw, "negotiated")
            plan_label = f"{payer} — {plan}" if plan else payer

            records.append(_make_record(
                ingest_id, file_id, file_url, last_updated_on, reporting_entity, ein,
                plan_label, plan_market_type, billing_code_type, billing_code,
                description, negotiated_type, rate, setting, billing_class,
                state_code, schema_version,
            ))

            if len(records) >= batch_size:
                yield _to_dataframe(records)
                records = []
                row_count += batch_size
                log.info("Batch yielded", rows_so_far=row_count, hospital=reporting_entity)

        else:
            # WIDE format: iterate payer/plan columns
            for payer, plan in payer_plan_pairs:
                dollar_key = f"standard_charge|{payer}|{plan}|negotiated_dollar"
                method_key = f"standard_charge|{payer}|{plan}|methodology"

                dollar_str = (row.get(dollar_key) or "").strip()
                if not dollar_str:
                    continue
                try:
                    rate = float(dollar_str)
                except ValueError:
                    continue
                if rate <= 0 or rate >= 10_000_000:
                    continue

                methodology_raw = (row.get(method_key) or "").strip().lower()
                negotiated_type = _METHODOLOGY_MAP.get(methodology_raw, "negotiated")
                plan_label = f"{payer.title()} — {plan.title()}"

                records.append(_make_record(
                    ingest_id, file_id, file_url, last_updated_on, reporting_entity, ein,
                    plan_label, plan_market_type, billing_code_type, billing_code,
                    description, negotiated_type, rate, setting, billing_class,
                    state_code, schema_version,
                ))

                if len(records) >= batch_size:
                    yield _to_dataframe(records)
                    records = []
                    row_count += batch_size
                    log.info("Batch yielded", rows_so_far=row_count, hospital=reporting_entity)

    if records:
        yield _to_dataframe(records)

    log.info("CSV parsing complete", hospital=reporting_entity, total_rows=row_count + len(records))


def _make_record(
    ingest_id, file_id, file_url, last_updated_on, reporting_entity, ein,
    plan_label, plan_market_type, billing_code_type, billing_code,
    description, negotiated_type, rate, setting, billing_class,
    state_code, schema_version,
) -> dict:
    return {
        "ingest_id":                ingest_id,
        "file_id":                  file_id,
        "file_url":                 file_url,
        "last_updated_on":          last_updated_on,
        "reporting_entity_name":    reporting_entity,
        "reporting_entity_type":    "hospital",
        "ein":                      ein,
        "plan_name":                plan_label,
        "plan_market_type":         plan_market_type,
        "billing_code_type":        billing_code_type,
        "billing_code":             billing_code,
        "billing_code_type_version":"",
        "name":                     description,
        "normalized_name":          None,
        "description":              description,
        "negotiated_type":          negotiated_type,
        "negotiated_rate":          rate,
        "expiration_date":          "",
        "service_code":             json.dumps([setting] if setting else []),
        "billing_class":            billing_class,
        "additional_generic_notes": "",
        "npi":                      json.dumps([]),
        "provider_group_id":        -1,
        "tin_type":                 "ein",
        "tin_value":                ein,
        "state_code":               state_code,
        "schema_version":           schema_version,
    }


def _to_dataframe(records: list[dict]) -> pl.DataFrame:
    return pl.from_dicts(records, schema=RECORD_SCHEMA, infer_schema_length=None)
