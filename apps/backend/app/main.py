"""
Healthcare Price Orchestration Platform — FastAPI entrypoint
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.core.logging import get_logger, setup_logging
from app.db.connection import get_db_connection, close_db_connection
from app.api.routes import health, query, prices, ingestion

setup_logging()
log = get_logger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    log.info("Starting Healthcare Price Platform", env=settings.app_env)
    get_db_connection()   # Initialize DuckDB connection pool
    yield
    close_db_connection()
    log.info("Shutdown complete")


app = FastAPI(
    title="Healthcare Price Orchestration Platform",
    description=(
        "Agentic platform for querying CMS machine-readable files "
        "with natural language. MVP: Virginia market."
    ),
    version="0.1.0",
    docs_url="/docs" if not settings.is_production else None,
    redoc_url="/redoc" if not settings.is_production else None,
    lifespan=lifespan,
)

# ── Middleware ────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# ── Routes ────────────────────────────────────────────────

app.include_router(health.router, tags=["health"])
app.include_router(query.router,  prefix="/api/v1/query",  tags=["query"])
app.include_router(prices.router, prefix="/api/v1/prices", tags=["prices"])
app.include_router(ingestion.router, prefix="/api/v1/ingestion", tags=["ingestion"])


# ── Global error handler ──────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request, exc: Exception):
    log.error("Unhandled exception", error=str(exc), path=str(request.url))
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc) if not settings.is_production else None},
    )
