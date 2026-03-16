"""
ClearRate-Bench Runner
─────────────────────
Submits benchmark questions to the live ClearRate API, captures gold SQL
and results, and writes an annotated benchmark file.

Usage:
  python run_benchmark.py                     # run all unannotated questions
  python run_benchmark.py --ids cb-001 cb-002 # run specific questions
  python run_benchmark.py --difficulty easy   # run by difficulty tier
  python run_benchmark.py --evaluate          # score against existing gold SQL

The runner saves gold_sql and gold_result_preview back into the seed JSON
after each successful API call. Re-running skips already-annotated questions
unless --force is passed.
"""

import argparse
import json
import time
from pathlib import Path

import httpx

API_URL = "http://localhost:8000/api/v1/query"
MARKET_ID = "dc_metro"
SEED_FILE = Path(__file__).parent / "clearrate_bench_seed.json"
OUTPUT_FILE = Path(__file__).parent / "clearrate_bench_annotated.json"
TIMEOUT_S = 120


def run_question(question: dict) -> dict:
    """Submit a single question and return the API response."""
    try:
        resp = httpx.post(
            API_URL,
            json={"query": question["question"], "market_id": MARKET_ID},
            timeout=TIMEOUT_S,
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "success": True,
            "gold_sql": data.get("sql"),
            "answer": data.get("answer", ""),
            "execution_ms": data.get("metadata", {}).get("execution_ms"),
            "row_count": data.get("metadata", {}).get("row_count"),
            "agent_nodes": data.get("metadata", {}).get("agent_nodes", []),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def evaluate_question(question: dict, gold_sql: str) -> dict:
    """
    Run the gold SQL against the DB and compare to a freshly generated response.
    Returns accuracy signal: exact_match, execution_match, key_column_match.
    """
    # Placeholder: full evaluation requires direct DuckDB access
    # which is blocked while the backend is running (single-writer).
    # In CI, stop the backend, run evaluation, restart backend.
    return {"note": "Evaluation requires backend to be stopped for direct DB access."}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ids", nargs="+", help="Run specific question IDs")
    parser.add_argument("--difficulty", choices=["easy", "medium", "hard"])
    parser.add_argument("--force", action="store_true", help="Re-annotate already-annotated questions")
    parser.add_argument("--evaluate", action="store_true", help="Score against existing gold SQL")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between API calls")
    args = parser.parse_args()

    with open(SEED_FILE) as f:
        bench = json.load(f)

    questions = bench["questions"]

    # Filter
    if args.ids:
        questions = [q for q in questions if q["id"] in args.ids]
    if args.difficulty:
        questions = [q for q in questions if q["difficulty"] == args.difficulty]
    if not args.force:
        questions = [q for q in questions if not q.get("gold_sql")]

    print(f"Running {len(questions)} questions...")
    print()

    results = []
    for i, q in enumerate(questions):
        print(f"[{i+1}/{len(questions)}] {q['id']} ({q['difficulty']}) -- {q['question'][:70]}...")

        result = run_question(q)

        if result["success"]:
            q["gold_sql"] = result["gold_sql"]
            q["gold_result_preview"] = None  # Populated in evaluation phase
            q["execution_ms"] = result["execution_ms"]
            q["row_count"] = result["row_count"]
            print(f"  OK | {result['execution_ms']}ms | {result['row_count']} rows")
            print(f"  SQL: {(result['gold_sql'] or '')[:120]}...")
        else:
            print(f"  FAIL | {result.get('error', 'unknown')}")

        results.append({"id": q["id"], **result})

        # Save progress after each question (crash-safe)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(bench, f, indent=2)

        if i < len(questions) - 1:
            time.sleep(args.delay)

    print()
    print(f"Done. Annotated file saved to: {OUTPUT_FILE}")

    # Summary
    success = sum(1 for r in results if r["success"])
    print(f"Success: {success}/{len(results)}")
    if results:
        times = [r["execution_ms"] for r in results if r.get("execution_ms")]
        if times:
            print(f"Median execution time: {sorted(times)[len(times)//2]}ms")


if __name__ == "__main__":
    main()
