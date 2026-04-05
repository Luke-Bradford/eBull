from pydantic_settings import BaseSettings, SettingsConfigDict


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

    enable_auto_trading: bool = False
    enable_live_trading: bool = False


settings = Settings()
