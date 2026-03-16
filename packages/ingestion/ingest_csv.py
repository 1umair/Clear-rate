"""
CMS Standard Charges Ingestion Script
──────────────────────────────────────
Downloads and ingests hospital standard charges files (CSV or JSON)
into the master_price_graph DuckDB table.

Usage:
  python ingest_csv.py                          # ingest all hospitals
  python ingest_csv.py --hospital inova_fairfax # ingest one hospital
  python ingest_csv.py --network inova          # ingest all Inova hospitals
  python ingest_csv.py --dry-run                # parse only, don't write to DB

Run from: packages/ingestion/
"""

import argparse
import sys
import uuid
from pathlib import Path

import duckdb
import structlog

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from pipeline.loader import load_dataframe, mark_superseded
from pipeline.parser_csv import parse_standard_charges_csv

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(colors=False),
    ]
)
log = structlog.get_logger()

# ─────────────────────────────────────────────────────────────────────────────
# Hospital definitions
#
# HOW TO POPULATE URLS:
# Each U.S. hospital is required under 45 CFR Part 180 to publish a
# machine-readable file (MRF) on their price transparency page.
# To find a hospital's current MRF URL:
#   1. Visit the hospital's website and search "price transparency"
#   2. Right-click the download link and copy the URL
#   3. The EIN (Employer Identification Number) typically appears in the filename
#      in the format: {EIN}_{hospital-slug}_standardcharges.{csv|json}
#   4. CMS also maintains a compliance tracker at:
#      https://www.cms.gov/hospital-price-transparency/hospitals
#
# URL format examples:
#   Wide CSV:  https://{hospital-domain}/price-transparency/{ein}_{slug}_standardcharges.csv
#   JSON:      https://{blob-storage}/{ein}_{SLUG}_standardcharges.json
#
# NOTE: URLs rotate quarterly when hospitals publish new MRF files.
# Re-check each hospital's price transparency page each quarter.
# ─────────────────────────────────────────────────────────────────────────────
HOSPITALS: dict[str, dict] = {

    # ── Inova Health System (5 hospitals, Northern Virginia) ─────────────────
    # Format: Wide CSV | Source: inova.org/price-transparency
    "inova_fairfax": {
        "name": "Inova Fairfax Hospital", "network": "inova", "ein": "",
        "state_code": "VA", "fmt": "csv",
        "url": "",  # Find at: https://www.inova.org/patients-visitors/billing/price-transparency
    },
    "inova_alexandria": {
        "name": "Inova Alexandria Hospital", "network": "inova", "ein": "",
        "state_code": "VA", "fmt": "csv",
        "url": "",
    },
    "inova_fair_oaks": {
        "name": "Inova Fair Oaks Hospital", "network": "inova", "ein": "",
        "state_code": "VA", "fmt": "csv",
        "url": "",
    },
    "inova_loudoun": {
        "name": "Inova Loudoun Hospital", "network": "inova", "ein": "",
        "state_code": "VA", "fmt": "csv",
        "url": "",
    },
    "inova_mount_vernon": {
        "name": "Inova Mount Vernon Hospital", "network": "inova", "ein": "",
        "state_code": "VA", "fmt": "csv",
        "url": "",
    },

    # ── UVA Health (4 hospitals, Virginia) ────────────────────────────────────
    # Format: Tall CSV | Source: uvahealth.com/price-transparency
    "uva_medical_center": {
        "name": "UVA Health Medical Center", "network": "uva", "ein": "",
        "state_code": "VA", "fmt": "csv",
        "url": "",  # Find at: https://uvahealth.com/patient-financial-services/standard-charges
    },
    "uva_culpeper": {
        "name": "UVA Culpeper Medical Center", "network": "uva", "ein": "",
        "state_code": "VA", "fmt": "csv",
        "url": "",
    },
    "uva_haymarket": {
        "name": "UVA Haymarket Medical Center", "network": "uva", "ein": "",
        "state_code": "VA", "fmt": "csv",
        "url": "",
    },
    "uva_prince_william": {
        "name": "UVA Prince William Medical Center", "network": "uva", "ein": "",
        "state_code": "VA", "fmt": "csv",
        "url": "",
    },

    # ── HCA Virginia (11 hospitals, Virginia) ─────────────────────────────────
    # Format: JSON via Azure Blob Storage | Source: hcavirginia.com/price-transparency
    "hca_chippenham": {
        "name": "Chippenham Hospital", "network": "hca_va", "ein": "",
        "state_code": "VA", "fmt": "json",
        "url": "",  # Find at: https://www.hcavirginia.com/patients-families/patient-financial-info/billing-insurance/pricing
    },
    "hca_dominion": {
        "name": "Dominion Hospital", "network": "hca_va", "ein": "",
        "state_code": "VA", "fmt": "json",
        "url": "",
    },
    "hca_henrico": {
        "name": "Henrico Doctors Hospital", "network": "hca_va", "ein": "",
        "state_code": "VA", "fmt": "json",
        "url": "",
    },
    "hca_johnston_willis": {
        "name": "Johnston-Willis Hospital", "network": "hca_va", "ein": "",
        "state_code": "VA", "fmt": "json",
        "url": "",
    },
    "hca_reston": {
        "name": "Reston Hospital Center", "network": "hca_va", "ein": "",
        "state_code": "VA", "fmt": "json",
        "url": "",
    },
    "hca_spotsylvania": {
        "name": "Spotsylvania Regional Medical Center", "network": "hca_va", "ein": "",
        "state_code": "VA", "fmt": "json",
        "url": "",
    },
    "hca_stonesprings": {
        "name": "StoneSprings Hospital Center", "network": "hca_va", "ein": "",
        "state_code": "VA", "fmt": "json",
        "url": "",
    },
    "hca_lewisgale_mc": {
        "name": "LewisGale Medical Center", "network": "hca_va", "ein": "",
        "state_code": "VA", "fmt": "json",
        "url": "",
    },
    "hca_lewisgale_montgomery": {
        "name": "LewisGale Hospital Montgomery", "network": "hca_va", "ein": "",
        "state_code": "VA", "fmt": "json",
        "url": "",
    },
    "hca_lewisgale_alleghany": {
        "name": "LewisGale Hospital Alleghany", "network": "hca_va", "ein": "",
        "state_code": "VA", "fmt": "json",
        "url": "",
    },
    "hca_lewisgale_pulaski": {
        "name": "LewisGale Hospital Pulaski", "network": "hca_va", "ein": "",
        "state_code": "VA", "fmt": "json",
        "url": "",
    },

    # ── MedStar Health (10 hospitals, DC + Maryland) ──────────────────────────
    # Format: Wide CSV | Source: medstarhealth.org/price-transparency-disclosures
    "medstar_washington": {
        "name": "MedStar Washington Hospital Center", "network": "medstar", "ein": "",
        "state_code": "DC", "fmt": "csv",
        "url": "",  # Find at: https://www.medstarhealth.org/patients-and-visitors/billing-and-insurance/hospital-price-transparency-disclosures
    },
    "medstar_georgetown": {
        "name": "MedStar Georgetown University Hospital", "network": "medstar", "ein": "",
        "state_code": "DC", "fmt": "csv",
        "url": "",
    },
    "medstar_national_rehab": {
        "name": "MedStar National Rehabilitation Hospital", "network": "medstar", "ein": "",
        "state_code": "DC", "fmt": "csv",
        "url": "",
    },
    "medstar_franklin_square": {
        "name": "MedStar Franklin Square Medical Center", "network": "medstar", "ein": "",
        "state_code": "MD", "fmt": "csv",
        "url": "",
    },
    "medstar_good_samaritan": {
        "name": "MedStar Good Samaritan Hospital", "network": "medstar", "ein": "",
        "state_code": "MD", "fmt": "csv",
        "url": "",
    },
    "medstar_harbor": {
        "name": "MedStar Harbor Hospital", "network": "medstar", "ein": "",
        "state_code": "MD", "fmt": "csv",
        "url": "",
    },
    "medstar_montgomery": {
        "name": "MedStar Montgomery Medical Center", "network": "medstar", "ein": "",
        "state_code": "MD", "fmt": "csv",
        "url": "",
    },
    "medstar_southern_maryland": {
        "name": "MedStar Southern Maryland Hospital Center", "network": "medstar", "ein": "",
        "state_code": "MD", "fmt": "csv",
        "url": "",
    },
    "medstar_st_marys": {
        "name": "MedStar St. Mary's Hospital", "network": "medstar", "ein": "",
        "state_code": "MD", "fmt": "csv",
        "url": "",
    },
    "medstar_union_memorial": {
        "name": "MedStar Union Memorial Hospital", "network": "medstar", "ein": "",
        "state_code": "MD", "fmt": "csv",
        "url": "",
    },
}

DB_PATH = _HERE.parent.parent / "apps" / "backend" / "data" / "price_graph.duckdb"


def ingest_hospital(hospital_id: str, cfg: dict, conn: duckdb.DuckDBPyConnection | None, dry_run: bool) -> int:
    ingest_id = str(uuid.uuid4())
    total_rows = 0

    log.info("Starting ingestion", hospital=cfg["name"], ingest_id=ingest_id[:8], fmt=cfg["fmt"])

    if cfg["fmt"] == "json":
        batches = _parse_json_hospital(cfg, ingest_id)
    else:
        batches = parse_standard_charges_csv(
            source=cfg["url"],
            hospital_name=cfg["name"],
            ein=cfg["ein"],
            state_code=cfg["state_code"],
            ingest_id=ingest_id,
            plan_market_type="large_group",
            batch_size=10_000,
        )

    for batch in batches:
        if dry_run:
            log.info("[DRY RUN] Parsed batch", rows=len(batch), hospital=cfg["name"])
            total_rows += len(batch)
        else:
            inserted = load_dataframe(batch, conn, ingest_id)
            total_rows += inserted
            # Force checkpoint every 100K rows to prevent WAL OOM on large CSV files
            if total_rows % 100_000 == 0:
                conn.checkpoint()

    if not dry_run and total_rows > 0 and conn:
        mark_superseded(conn, cfg["name"], ingest_id)
        conn.checkpoint()

    log.info("Done", hospital=cfg["name"], total_rows=total_rows, dry_run=dry_run)
    return total_rows


def _parse_json_hospital(cfg: dict, ingest_id: str):
    """
    Handle CMS standard charges JSON format (HCA publishes JSON instead of CSV).
    The JSON standard charges format mirrors the CSV columns but in nested structure.
    Falls back to CSV parser if the JSON turns out to be CSV-compatible.
    """
    import json
    import urllib.request

    log.info("Downloading JSON standard charges", url=cfg["url"])
    req = urllib.request.Request(cfg["url"], headers={"User-Agent": "ClearRate-Ingestion/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            raw = r.read()
    except Exception as e:
        log.error("Download failed", hospital=cfg["name"], error=str(e))
        return

    # Detect if it's actually a CSV
    if raw[:3] in (b"hos", b"des", b"\xef\xbb\xbf"):
        log.info("File is actually CSV — redirecting to CSV parser")
        yield from parse_standard_charges_csv(
            source=raw,
            hospital_name=cfg["name"],
            ein=cfg["ein"],
            state_code=cfg["state_code"],
            ingest_id=ingest_id,
        )
        return

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        log.error("JSON decode failed", hospital=cfg["name"], error=str(e))
        return

    yield from _parse_cms_standard_charges_json(data, cfg, ingest_id)


def _parse_cms_standard_charges_json(data: dict, cfg: dict, ingest_id: str):
    """
    Parse CMS standard charges JSON format into master_price_graph batches.
    JSON format has standard_charge_information[] with nested payers_information[].
    """
    import json
    import polars as pl
    from pipeline.parser_csv import RECORD_SCHEMA, _METHODOLOGY_MAP

    hospital_name = data.get("hospital_name", cfg["name"])
    last_updated = data.get("last_updated_on", "")
    schema_version = data.get("version", "2.0.0")
    file_id = cfg["url"].split("/")[-1].replace(".json", "")

    records = []
    batch_size = 10_000

    for item in data.get("standard_charge_information", []):
        description = (item.get("description") or "").strip()
        codes = item.get("code_information", [])

        # Pick best code
        billing_code = ""
        billing_code_type = "OTHER"
        for c in codes:
            ctype = (c.get("type") or "").upper()
            cval = (c.get("code") or "").strip()
            if ctype in ("CPT", "HCPCS", "MS-DRG") and cval:
                billing_code = cval
                billing_code_type = ctype
                break
        if not billing_code and codes:
            billing_code = str(codes[0].get("code", ""))
            billing_code_type = str(codes[0].get("type", "OTHER")).upper()

        if not billing_code:
            continue

        for charge in item.get("standard_charges", []):
            setting = (charge.get("setting") or "").lower()
            billing_class = "institutional" if setting in ("inpatient", "both") else "professional"

            for payer_info in charge.get("payers_information", []):
                payer = (payer_info.get("payer_name") or "").strip()
                plan = (payer_info.get("plan_name") or "").strip()
                rate = payer_info.get("standard_charge_dollar")
                methodology = (payer_info.get("methodology") or "").lower()

                if rate is None:
                    continue
                try:
                    rate = float(rate)
                except (TypeError, ValueError):
                    continue
                if rate <= 0 or rate >= 10_000_000:
                    continue

                negotiated_type = _METHODOLOGY_MAP.get(methodology, "negotiated")
                plan_label = f"{payer.title()} — {plan.title()}" if plan else payer.title()

                records.append({
                    "ingest_id":                ingest_id,
                    "file_id":                  file_id,
                    "file_url":                 cfg["url"],
                    "last_updated_on":          last_updated,
                    "reporting_entity_name":    hospital_name,
                    "reporting_entity_type":    "hospital",
                    "ein":                      cfg["ein"],
                    "plan_name":                plan_label,
                    "plan_market_type":         "large_group",
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
                    "tin_value":                cfg["ein"],
                    "state_code":               cfg["state_code"],
                    "schema_version":           schema_version,
                })

                if len(records) >= batch_size:
                    yield pl.from_dicts(records, schema=RECORD_SCHEMA, infer_schema_length=None)
                    records = []

    if records:
        import polars as pl
        yield pl.from_dicts(records, schema=RECORD_SCHEMA, infer_schema_length=None)


def main():
    all_hospital_ids = list(HOSPITALS.keys())
    all_networks = sorted({cfg["network"] for cfg in HOSPITALS.values()})

    parser = argparse.ArgumentParser(description="Ingest CMS standard charges files into DuckDB")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--hospital", choices=all_hospital_ids, help="Ingest a single hospital")
    group.add_argument("--network", choices=all_networks, help="Ingest all hospitals in a network")
    parser.add_argument("--dry-run", action="store_true", help="Parse only, don't write to DB")
    args = parser.parse_args()

    if args.hospital:
        targets = {args.hospital: HOSPITALS[args.hospital]}
    elif args.network:
        targets = {k: v for k, v in HOSPITALS.items() if v["network"] == args.network}
    else:
        targets = HOSPITALS

    conn = None
    if not args.dry_run:
        if not DB_PATH.exists():
            log.error("DuckDB not found — run schema init first", path=str(DB_PATH))
            sys.exit(1)
        conn = duckdb.connect(str(DB_PATH))
        # Keep WAL small — checkpoint every 50 MB to prevent OOM on large MRF files
        conn.execute("SET checkpoint_threshold = '50MB'")
        log.info("Connected to DuckDB", path=str(DB_PATH))

    log.info("Ingestion starting", hospitals=list(targets.keys()), dry_run=args.dry_run)

    grand_total = 0
    for hospital_id, cfg in targets.items():
        try:
            rows = ingest_hospital(hospital_id, cfg, conn, args.dry_run)
            grand_total += rows
        except Exception as e:
            log.error("Ingestion failed", hospital=cfg["name"], error=str(e))

    if conn:
        count = conn.execute("SELECT COUNT(*) FROM master_price_graph").fetchone()[0]
        conn.close()
        log.info("All done", total_inserted=grand_total, db_total_rows=count)
    else:
        log.info("[DRY RUN] Complete", total_parsed=grand_total)


if __name__ == "__main__":
    main()
