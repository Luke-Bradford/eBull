from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Minimum operator API key length. 32 chars of base64/hex is ~192 bits of
# entropy, well above brute-force range. We refuse to start with anything
# shorter rather than silently accepting a weak credential.
_MIN_API_KEY_LEN = 32


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "dev"
    database_url: str = "postgresql://postgres:postgres@localhost:5432/trader_os"

    etoro_read_api_key: str | None = None
    etoro_write_api_key: str | None = None
    etoro_env: str = "demo"

    fmp_api_key: str | None = None
    companies_house_api_key: str | None = None
    # SEC EDGAR requires no API key (public API, 10 req/s fair-use limit)
    sec_user_agent: str = "eBull dev@example.com"

    anthropic_api_key: str | None = None

    default_portfolio_mode: str = "balanced"
    max_active_positions: int = 20
    max_initial_position_pct: int = 5
    max_full_position_pct: int = 10
    max_sector_exposure_pct: int = 25

    # NOTE: enable_auto_trading and enable_live_trading used to live here.
    # As of issue #56 they are DB-backed (runtime_config singleton) and
    # toggled at runtime via PATCH /config — env values are no longer the
    # source of truth.

    # Operator API key for authenticating requests to protected endpoints.
    # Sourced from EBULL_API_KEY (or API_KEY via env_file). When unset, all
    # protected endpoints fail closed — we never silently allow access.
    api_key: str | None = None

    @field_validator("api_key")
    @classmethod
    def _api_key_min_length(cls, v: str | None) -> str | None:
        # An empty string is treated as "unset" by require_auth (fail-closed),
        # so we allow it through validation. Any non-empty value must meet
        # the minimum length so a single-character key cannot be accepted.
        if v is None or v == "":
            return v
        if len(v) < _MIN_API_KEY_LEN:
            raise ValueError(f"api_key must be at least {_MIN_API_KEY_LEN} characters")
        return v


settings = Settings()
