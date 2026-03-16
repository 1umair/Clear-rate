"""
Application configuration — loaded from environment variables.
Never hardcode secrets. Copy .env.example → .env with real values.
"""

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Walk up from apps/backend to find the project root .env
_HERE = Path(__file__).resolve()
_PROJECT_ROOT = _HERE.parent.parent.parent.parent.parent  # config → core → app → backend → apps → root
_ENV_FILE = _PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Core ──────────────────────────────────────────────
    app_env: str = Field(default="development", description="development | staging | production")
    log_level: str = Field(default="INFO")
    secret_key: str = Field(default="change-me")

    # ── Server ────────────────────────────────────────────
    backend_host: str = Field(default="0.0.0.0")
    backend_port: int = Field(default=8000)
    cors_origins: str = Field(default="http://localhost:3000")

    # ── LLM (Anthropic) ───────────────────────────────────
    anthropic_api_key: str = Field(default="", description="Anthropic API key — required")
    anthropic_model: str = Field(default="claude-sonnet-4-6", description="Claude model ID")
    anthropic_max_tokens: int = Field(default=4096)

    # ── Database ──────────────────────────────────────────
    duckdb_path: str = Field(default="./data/price_graph.duckdb")

    # ── Ingestion ─────────────────────────────────────────
    cms_request_timeout_seconds: int = Field(default=300)
    cms_max_concurrent_downloads: int = Field(default=3)
    cms_retry_attempts: int = Field(default=3)

    # ── Rate limiting ─────────────────────────────────────
    api_rate_limit_per_minute: int = Field(default=60)

    # ── Observability (optional) ──────────────────────────
    langfuse_public_key: str = Field(default="")
    langfuse_secret_key: str = Field(default="")
    langfuse_host: str = Field(default="https://cloud.langfuse.com")

    @field_validator("duckdb_path")
    @classmethod
    def ensure_data_dir(cls, v: str) -> str:
        path = Path(v)
        path.parent.mkdir(parents=True, exist_ok=True)
        return v

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",")]

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
