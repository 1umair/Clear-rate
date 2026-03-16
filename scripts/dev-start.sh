#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────
# Dev startup script — starts backend and frontend together
# Usage: ./scripts/dev-start.sh
# ─────────────────────────────────────────────────────────

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Check .env exists
if [ ! -f .env ]; then
  echo "ERROR: .env file not found. Copy .env.example → .env and fill in values."
  echo "  cp .env.example .env"
  exit 1
fi

# Check ANTHROPIC_API_KEY is set
source .env
if [ -z "$ANTHROPIC_API_KEY" ] || [ "$ANTHROPIC_API_KEY" = "sk-ant-..." ]; then
  echo "ERROR: ANTHROPIC_API_KEY not set in .env"
  exit 1
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Healthcare Price Platform — Dev Mode"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Initialize DB schema if needed
echo ""
echo "▶ Initializing DuckDB schema..."
cd apps/backend && uv run python -m app.db.schema && cd "$REPO_ROOT"

# Start backend in background
echo ""
echo "▶ Starting FastAPI backend on :8000..."
cd apps/backend && uv run uvicorn app.main:app --reload --port 8000 &
BACKEND_PID=$!
cd "$REPO_ROOT"

# Wait for backend
sleep 2
echo "  Backend PID: $BACKEND_PID"

# Start frontend
echo ""
echo "▶ Starting Next.js frontend on :3000..."
cd apps/frontend && pnpm dev &
FRONTEND_PID=$!
cd "$REPO_ROOT"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Frontend: http://localhost:3000"
echo "  Backend:  http://localhost:8000"
echo "  API Docs: http://localhost:8000/docs"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Press Ctrl+C to stop all services."

# Cleanup on exit
trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" INT TERM

wait
