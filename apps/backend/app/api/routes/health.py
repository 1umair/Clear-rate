from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.db.connection import get_db_connection
from app.core.config import get_settings

router = APIRouter()
settings = get_settings()


@router.get("/health")
async def health_check():
    """Health check endpoint for load balancers and Docker healthchecks."""
    try:
        conn = get_db_connection()
        conn.execute("SELECT 1").fetchone()
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {e}"

    status = "ok" if db_status == "ok" else "degraded"
    return JSONResponse(
        status_code=200 if status == "ok" else 503,
        content={
            "status": status,
            "db": db_status,
            "env": settings.app_env,
            "version": "0.1.0",
        },
    )
