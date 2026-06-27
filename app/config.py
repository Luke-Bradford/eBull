import os

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Minimum service-token length. 32 chars of base64/hex is ~192 bits of
# entropy, well above brute-force range. We refuse to start with anything
# shorter rather than silently accepting a weak credential. The same floor
# applies to the operator browser session credential and to the service
# token used by tests / scripts / cron jobs.
_MIN_SERVICE_TOKEN_LEN = 32

# --- libpq connect timeout (single source) — #1472 PR0 ---------------------
# Bound the connect phase of EVERY psycopg connection — raw or pooled —
# across every process (API / jobs / one-off scripts / tests). libpq has no
# default connect timeout, so a connect that stalls mid-SCRAM-auth under a
# cadence-boundary connection herd hangs *forever*. When that happens inside
# the scheduled-fire wrapper's gate/prereq connect (app/jobs/runtime.py:1511,
# :1541) — which runs BEFORE record_job_start — the job's APScheduler
# instance counter never decrements and max_instances=1 then silently
# suppresses every later fire of that job (the 2026-06-04 SEC discovery-layer
# freeze, #1474). libpq reads PGCONNECT_TIMEOUT from the environment for all
# connections, so setting it here — at config import, before any connect —
# bounds all 115 raw psycopg.connect sites + the pools with no DSN or
# call-site change. setdefault means an explicit value in the PROCESS
# environment (shell / systemd / Docker) wins — PGCONNECT_TIMEOUT is a
# libpq env var, not a Settings/.env field, so the process env is its
# knob (a repo `.env` entry would NOT override it). See
# docs/proposals/infra/2026-06-04-db-connection-discipline.md (GAP-A).
DB_CONNECT_TIMEOUT_S = 10
os.environ.setdefault("PGCONNECT_TIMEOUT", str(DB_CONNECT_TIMEOUT_S))


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "dev"
    database_url: str = "postgresql://postgres:postgres@localhost:5432/ebull"
    # Redis (IPC layer for live pricing — follow-up plan)
    redis_url: str = "redis://localhost:6379/0"

    # eToro API credentials are now stored in the encrypted broker_credentials
    # table (issue #99) and loaded via load_credential_for_provider_use().
    # The migration script scripts/migrate_etoro_credential.py handles the
    # one-time move from env vars to the encrypted store.
    etoro_env: str = "demo"
    etoro_base_url: str = "https://public-api.etoro.com"

    companies_house_api_key: str | None = None
    # SEC EDGAR requires no API key (public API, 10 req/s fair-use limit)
    sec_user_agent: str = "eBull dev@example.com"

    # Yahoo Finance per-ticker RSS news feed (#1750) rejects the default
    # httpx User-Agent; a browser-like UA is required (verified). No API key.
    news_rss_user_agent: str = "Mozilla/5.0 (compatible; eBull/1.0; +research)"

    # Soft deadline (seconds) for the universe-wide 13F-HR quarterly
    # sweep (#913). The sweep walks ~11k filers in ``institutional_filers``;
    # at ~1-3s per filer the cold first sweep can run several hours.
    # Already-ingested accessions are tombstoned in
    # ``institutional_holdings_ingest_log`` so a deadline-interrupted
    # run resumes the tail on the next weekly fire. Default 6h.
    sec_13f_sweep_deadline_seconds: float = 6 * 60 * 60

    # Soft deadline (seconds) for the monthly NPORT-P fund-holdings
    # sweep (#917). Same shape as the 13F sweep — already-attempted
    # accessions are tombstoned in ``n_port_ingest_log`` so a
    # deadline-interrupted run resumes on the next monthly fire.
    # Default 6h. The MVP universe walks the
    # ``institutional_filers`` rows where ``filer_type IN ('INV', 'INS', 'ETF')``;
    # not every RIC is in that list, so coverage is partial until a
    # dedicated fund-filer directory walk lands as a follow-up.
    sec_n_port_sweep_deadline_seconds: float = 6 * 60 * 60

    # Number of concurrent per-filer ingest pipelines for the universe-wide
    # SEC sweeps (13F #913 + N-PORT #917), all sharing one process-global SEC
    # rate gate (#1274). The serial loop used ~5-10% of the 10 req/s budget;
    # N pipelines saturate it — the gate, not the worker count, bounds the
    # request rate (app/providers/rate_gate.py::InProcessFloorGate). Each
    # worker opens its own DB connection, so this also caps concurrent
    # job-pool conns. Clamped to >=1 by the concurrency driver.
    sec_filer_ingest_concurrency: int = 8

    anthropic_api_key: str | None = None

    # OpenFIGI free fallback CUSIP→ticker resolver (#1233 PR-1b + SD-1).
    # Optional. Without a key the resolver runs unkeyed (25 req/min,
    # max 10 jobs/POST = 250 mappings/min — ~48 min on a 12k unresolved
    # CUSIP backlog). With a key (free tier, register at
    # https://www.openfigi.com/api): 25 req/6s, max 100 jobs/POST =
    # 25,000 mappings/min — sweep completes in ~5 min.
    openfigi_api_key: str | None = None

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

    # --- Sync orchestrator (issue #260) ---------------------------------
    # Phase 4 (this flip): activates the orchestrator. POST /sync now
    # returns 202 + plan; the 12 legacy cron triggers mapping to
    # non-empty JOB_TO_LAYERS entries have been removed and replaced
    # with two orchestrator triggers (FULL @ 03:00 UTC and
    # HIGH_FREQUENCY @ */5min).
    # The 13 underlying job functions stay in _INVOKERS so
    # POST /jobs/{name}/run continues to work via the adapter.
    orchestrator_enabled: bool = True

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

    # --- Broker secret encryption (issue #99 / ADR 0001) ----------------
    # Base64-encoded 32-byte key used for AES-256-GCM encryption of broker
    # credentials (broker_credentials.ciphertext). Sourced from
    # EBULL_SECRETS_KEY. The server refuses to start if this is missing or
    # does not decode to exactly 32 bytes -- we never silently generate a
    # key, because that would lock the existing ciphertext rows out after
    # the next restart. Rotation is manual and documented in ADR 0001.
    #
    # Generate with:
    #     python -c "import os, base64; print(base64.b64encode(os.urandom(32)).decode())"
    # #1406 — the docs, .env.example, and master_key/secrets_crypto
    # docstrings all name this **EBULL_SECRETS_KEY**, but ``Settings`` has no
    # ``env_prefix`` so without an alias the field would only read the bare
    # ``SECRETS_KEY`` and silently drop a documented ``EBULL_SECRETS_KEY``.
    # AliasChoices honours the documented name AND keeps the legacy bare name
    # working (back-compat). Currently the value is empty everywhere, so this
    # changes nothing at runtime; it only un-breaks the documented variable.
    # NOTE (ADR-0003 §9): if an operator later sets a REAL key while
    # ciphertext already exists under the file-derived key, env-key mode
    # activates and the server fail-loud refuses to start until the documented
    # key-rotation flow is followed.
    secrets_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("EBULL_SECRETS_KEY", "SECRETS_KEY"),
    )

    # --- Local secret bootstrap data dir (issue #114 / ADR-0003) --------
    # Directory holding the persisted root secret file. When unset we
    # fall back to platformdirs user_data_dir("eBull"). EBULL_DATA_DIR
    # env override takes precedence over this setting. Configurable so
    # operators can park the file on an encrypted volume.
    data_dir: str | None = None

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

    # Raw-data retention sweep job (#268 follow-up Plan A).
    # When True, ``raw_data_retention_sweep`` logs counts per source
    # but does NOT delete any files. Default flipped to False under
    # #325 after dry-run cycles confirmed the reclaim volume and
    # nothing downstream reads from ``data/raw/`` post-ingest. Env
    # override ``EBULL_RAW_RETENTION_DRY_RUN=true`` still available
    # for operators who need a one-run pause (e.g. before a manual
    # audit). See ``_RETENTION_POLICY`` in
    # ``app/services/raw_persistence.py`` for per-source max_age.
    raw_retention_dry_run: bool = False

    # Chunk L: dedupe SEC filings fetch. When True,
    # ``daily_research_refresh`` skips its SEC EDGAR filings fetch
    # entirely and relies on ``daily_financial_facts``'s master-index
    # path (``_upsert_filing_from_master_index``) to populate
    # ``filing_events``. Static audit confirms the master-index path
    # is strictly broader in coverage (every form type, including
    # amendments) — the only difference is ``primary_document_url``
    # may be the generic index page rather than the specific document.
    # Companies House filings path is unaffected.
    # Ship as False (default) → operator flips True → observe ~1 week
    # → follow-up PR deletes the guarded SEC block.
    enable_filings_fetch_dedupe: bool = False

    # When True, ``daily_research_refresh`` skips its SEC XBRL
    # ``refresh_fundamentals`` call and ``fundamentals_sync`` phase 1b
    # owns ``fundamentals_snapshot`` refresh for CIK-mapped tradable
    # instruments. Collapses the dual SEC ``companyfacts`` fetch path
    # identified in issue #414 so only one scheduled job hits
    # ``data.sec.gov/api/xbrl/companyfacts/…`` each day. Companies
    # House filings continue to run in ``daily_research_refresh``
    # regardless of this flag.
    # Ship as False (default) → operator flips True → observe ~1 day
    # → follow-up PR deletes the guarded SEC-fundamentals block in
    # ``daily_research_refresh``.
    enable_sec_fundamentals_dedupe: bool = False


settings = Settings()
