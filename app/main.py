from fastapi import FastAPI

from app.config import settings

app = FastAPI(title="trader-os", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "env": settings.app_env,
        "etoro_env": settings.etoro_env,
        "auto_trading_enabled": settings.enable_auto_trading,
        "live_trading_enabled": settings.enable_live_trading,
    }
