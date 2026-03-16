# ClearRate

A text-to-SQL agent for querying CMS hospital price transparency data (Machine-Readable Files) using natural language.

Built on: FastAPI + LangGraph + DuckDB + Next.js 14.

Described in: *ClearRate: A Text-to-SQL Agent for Healthcare Price Intelligence over CMS Machine-Readable Files* (see `paper/clearrate-paper.md`).

---

## What It Does

Hospitals in the U.S. are required under 45 CFR Part 180 to publish machine-readable files (MRFs) containing negotiated rates for every payer and procedure. These files are publicly available but practically unusable -- a single health system's MRF can be 10+ GB, span three incompatible schemas, and require domain expertise to interpret.

ClearRate ingests these files into a normalized DuckDB price graph and exposes a natural language query interface. Example queries:

- "Which hospital has the lowest median rate for total knee replacement?"
- "Compare Cigna rates across all hospitals by service category."
- "What is the price spread for a colonoscopy across all 30 hospitals?"

---

## Architecture

```
[ CMS MRF Sources ]          [ Ingestion Pipeline ]
  Wide CSV (Inova, MedStar)  ---> Format detection
  Tall CSV (UVA Health)      ---> Streaming parser (ijson / csv)
  JSON     (HCA Virginia)    ---> DuckDB loader + checkpoint mgmt
                                       |
                              [ master_price_graph ]
                               DuckDB columnar store
                                       |
                          [ LangGraph 5-Node Agent ]
                           1. parse_intent
                           2. map_procedure
                           3. generate_sql
                           4. execute_sql
                           5. synthesize_response
                                       |
                          [ FastAPI + in-memory TTL cache ]
                                       |
                          [ Next.js 14 frontend ]
```

---

## Stack

| Layer | Technology |
|---|---|
| Frontend | Next.js 14 App Router, shadcn/ui, Tailwind CSS |
| Backend | FastAPI, LangGraph, Python 3.12 |
| Database | DuckDB (columnar, single-node) |
| LLM | Claude via Anthropic API |
| Ingestion | Polars, ijson (streaming), csv |
| Package management | uv (Python), pnpm (Node) |

---

## Repository Structure

```
healthcare-price-platform/
  apps/
    backend/          FastAPI + LangGraph agent
    frontend/         Next.js chat interface
  packages/
    ingestion/        CMS MRF download, parse, and load pipeline
  benchmark/          ClearRate-Bench NL-to-SQL evaluation dataset
  paper/              Research paper (Markdown)
  data/               Gitignored (DuckDB files, raw MRF downloads)
```

---

## Setup

### Prerequisites

- Python 3.12+
- Node.js 20+
- pnpm
- uv (`pip install uv`)
- Anthropic API key

### Backend

```bash
cd apps/backend
uv venv .venv
uv pip install -e .
cp ../../.env.example .env
# Add your ANTHROPIC_API_KEY to .env

# Initialize DuckDB schema
.venv/Scripts/python -m app.db.schema   # Windows
.venv/bin/python -m app.db.schema       # macOS/Linux

# Start backend
.venv/Scripts/uvicorn app.main:app --reload --port 8000
```

### Frontend

```bash
cd apps/frontend
pnpm install
pnpm dev
```

Open http://localhost:3000. Backend must be running at http://localhost:8000.

---

## Ingesting Data

**Step 1: Stop the backend** (DuckDB single-writer constraint).

**Step 2: Populate hospital MRF URLs** in `packages/ingestion/ingest_csv.py`.

Each U.S. hospital is required to publish their MRF on their price transparency page (45 CFR Part 180). The file comments include the price transparency page URL for each hospital system. EINs typically appear in the MRF filename in the format `{EIN}_{hospital-slug}_standardcharges.{csv|json}`.

CMS also maintains a compliance tracker: https://www.cms.gov/hospital-price-transparency/hospitals

**Step 3: Run ingestion**

```bash
cd packages/ingestion

python ingest_csv.py                          # all hospitals
python ingest_csv.py --hospital inova_fairfax # one hospital
python ingest_csv.py --network inova          # one network
python ingest_csv.py --dry-run                # parse only, no DB writes
```

**Step 4: Restart the backend.**

### DuckDB OOM Note

Large MRF files (>1M rows) can cause OOM during DuckDB WAL checkpointing. The ingestion script handles this with `SET checkpoint_threshold = '50MB'` and explicit checkpoints every 100K rows. On crash recovery, DuckDB replays the WAL automatically -- re-run ingestion for the affected hospital to clean up partial rows.

---

## Benchmark: ClearRate-Bench

`benchmark/` contains 30 annotated (natural language question, gold SQL) pairs for evaluating NL-to-SQL systems over CMS pricing data. Three difficulty tiers, five categories.

```bash
cd benchmark
python run_benchmark.py --difficulty easy
python run_benchmark.py --difficulty medium
python run_benchmark.py --difficulty hard
```

---

## Key Design Decisions

**Vectorless RAG.** Text-to-SQL over a relational schema rather than embedding-based retrieval. Pricing queries require exact aggregations (MIN, MEDIAN, GROUP BY) that vector similarity cannot compute.

**DuckDB.** Single-node columnar storage handles tens of millions of rows with sub-second GROUP BY queries. No Spark or distributed compute needed at this scale.

**Single pipeline, not multi-agent.** Five fixed nodes in order for every query. Complexity is in the data and schema, not in task decomposition.

**In-memory TTL cache.** SHA-256 keyed, 1-hour TTL. Reduces repeated query latency from 8-12s to under 150ms. No Redis dependency for single-instance deployment.

---

## Procedure Normalization

`map_procedure` (Node 2) maps natural language procedure terms to billing codes via Claude API fallback and DB validation. A production procedure normalization module can be plugged in via the `ProcedureNormalizer` interface -- see `packages/ingestion/pipeline/`. That module is not included in this repository.

---

## License

Apache 2.0

---

## Citation

```bibtex
@misc{clearrate2025,
  title={ClearRate: A Text-to-SQL Agent for Healthcare Price Intelligence over CMS Machine-Readable Files},
  year={2025},
  note={Preprint. See paper/clearrate-paper.md}
}
```
