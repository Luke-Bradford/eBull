from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Minimum service-token length. 32 chars of base64/hex is ~192 bits of
# entropy, well above brute-force range. We refuse to start with anything
# shorter rather than silently accepting a weak credential. The same floor
# applies to the operator browser session credential and to the service
# token used by tests / scripts / cron jobs.
_MIN_SERVICE_TOKEN_LEN = 32


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "dev"
    database_url: str = "postgresql://postgres:postgres@localhost:5432/ebull"

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

    # Service token for tests / scripts / cron jobs that authenticate by
    # bearer header instead of an interactive browser session. Sourced from
    # EBULL_SERVICE_TOKEN. When unset, the bearer-token auth path fails
    # closed — we never silently allow access. The browser session path
    # (issue #98) is independent and is the normal operator path.
    #
    # Renamed from ``api_key`` in issue #98. The single environment field
    # ``EBULL_API_KEY`` previously served this role; deployments must rename
    # to ``EBULL_SERVICE_TOKEN`` (documented in .env.example).
    service_token: str | None = None

    # --- Browser session settings (issue #98) ---------------------------
    # Both timeouts are enforced server-side in get_active_session. The
    # cookie expiry is set from session_absolute_timeout_hours so the
    # browser drops the cookie at the same point the server stops accepting
    # it -- avoids "cookie still present, server says 401" UX confusion.
    session_absolute_timeout_hours: int = 12
    session_idle_timeout_minutes: int = 60
    session_cookie_name: str = "ebull_session"
    # In dev (HTTP localhost) Secure cookies will not be set by browsers.
    # Production deploys must set this to True. We default to False so the
    # local dev stack works without TLS; the .env.example documents the
    # required production override.
    session_cookie_secure: bool = False

    # --- First-run setup + bind address (issue #106 / ADR 0002) ---------
    # Bind address used by uvicorn AND consulted by the bootstrap-mode
    # check. Default is loopback, so a fresh clone of the repo runs in
    # Mode A (zero-config setup, no token required) without the user
    # having to do anything. Setting this to 0.0.0.0 (or any non-loopback
    # address) flips the application into Mode B, requiring a bootstrap
    # token for /auth/setup. The uvicorn launch command MUST read the
    # same setting -- if uvicorn binds 0.0.0.0 but settings.host is
    # 127.0.0.1, the loopback check will incorrectly accept setup over
    # the LAN.
    host: str = "127.0.0.1"
    # Optional one-time bootstrap token for first-run setup. When set,
    # /auth/setup requires the caller to present this exact value
    # regardless of bind address. When unset and the server is bound to
    # a non-loopback address, the application generates a fresh token at
    # startup and prints it to the application log exactly once. Never
    # logged again, never written to disk. See ADR 0002 §3.
    bootstrap_token: str | None = None

    @field_validator("service_token")
    @classmethod
    def _service_token_min_length(cls, v: str | None) -> str | None:
        # An empty string is treated as "unset" by the bearer dep
        # (fail-closed), so we allow it through validation. Any non-empty
        # value must meet the minimum length so a single-character token
        # cannot be accepted.
        if v is None or v == "":
            return v
        if len(v) < _MIN_SERVICE_TOKEN_LEN:
            raise ValueError(f"service_token must be at least {_MIN_SERVICE_TOKEN_LEN} characters")
        return v


settings = Settings()
