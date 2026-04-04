# dev.ps1 — start the trader-os dev environment on Windows (PowerShell)
# Usage: .\dev.ps1
# Starts postgres (docker), waits for it to be ready, applies pending migrations, then starts uvicorn.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "Starting postgres..." -ForegroundColor Cyan
docker compose up -d

Write-Host "Waiting for postgres to be ready..." -ForegroundColor Cyan
$timeout = 30
$elapsed = 0
while ($elapsed -lt $timeout) {
    $result = docker exec trader-os-postgres pg_isready -U postgres -d trader_os 2>&1
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 1
    $elapsed++
}
if ($elapsed -ge $timeout) {
    Write-Host "Postgres did not become ready in ${timeout}s — check docker logs" -ForegroundColor Red
    exit 1
}
Write-Host "Postgres is ready." -ForegroundColor Green

Write-Host "Applying pending migrations..." -ForegroundColor Cyan
uv run python scripts/migrate.py

Write-Host "Starting uvicorn on http://localhost:8000 ..." -ForegroundColor Cyan
Write-Host "  /health    — liveness check" -ForegroundColor Gray
Write-Host "  /health/db — DB + migration status" -ForegroundColor Gray
Write-Host "  /docs      — interactive API docs" -ForegroundColor Gray
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
