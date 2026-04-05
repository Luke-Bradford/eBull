#!/usr/bin/env pwsh
# Start the full trader-os dev stack: postgres -> migrations -> server

Set-Location $PSScriptRoot

Write-Host "Starting postgres..." -ForegroundColor Cyan
docker compose up -d
if ($LASTEXITCODE -ne 0) { Write-Error "docker compose failed"; exit 1 }

Write-Host "Waiting for postgres to be ready..." -ForegroundColor Cyan
$timeout = 60
$elapsed = 0
while ($elapsed -lt $timeout) {
    $ready = docker exec trader-os-postgres pg_isready -U postgres -d trader_os 2>$null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 1
    $elapsed++
}
if ($elapsed -ge $timeout) {
    Write-Error "Postgres did not become ready in ${timeout}s. Check: docker logs trader-os-postgres"
    exit 1
}
Write-Host "Postgres is ready." -ForegroundColor Green

Write-Host "Applying migrations..." -ForegroundColor Cyan
$env:PYTHONPATH = $PSScriptRoot
uv run python scripts/migrate.py
if ($LASTEXITCODE -ne 0) { Write-Error "Migrations failed"; exit 1 }

Write-Host "Starting server on http://localhost:8000" -ForegroundColor Green
# Binds to localhost only. To expose on the network, override: --host 0.0.0.0
uv run uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
exit 0
