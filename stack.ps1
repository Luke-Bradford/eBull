#!/usr/bin/env pwsh
# stack.ps1 — prepare the eBull dev stack (postgres + migrations).
#
# What it does:
#   1. docker compose up -d   (postgres)
#   2. waits for pg_isready
#   3. applies pending migrations
#
# The backend (uvicorn) and frontend (vite) are launched as separate
# VS Code tasks ("stack: backend" / "stack: frontend") so they live in
# integrated terminal tabs. Run them via the "dev: start stack" task,
# which depends on this script.
#
# To stop postgres: .\stack-stop.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Write-Host "[1/3] Starting postgres..." -ForegroundColor Cyan
docker compose up -d
if ($LASTEXITCODE -ne 0) { Write-Error "docker compose failed"; exit 1 }

Write-Host "[2/3] Waiting for postgres to be ready..." -ForegroundColor Cyan
$timeout = 60
$elapsed = 0
while ($elapsed -lt $timeout) {
    docker exec ebull-postgres pg_isready -U postgres -d ebull 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 1
    $elapsed++
}
if ($elapsed -ge $timeout) {
    Write-Error "Postgres did not become ready in ${timeout}s. Check: docker logs ebull-postgres"
    exit 1
}
Write-Host "      Postgres ready." -ForegroundColor Green

Write-Host "[3/3] Applying migrations..." -ForegroundColor Cyan
$env:PYTHONPATH = $PSScriptRoot
uv run python scripts/migrate.py
if ($LASTEXITCODE -ne 0) { Write-Error "Migrations failed"; exit 1 }

Write-Host ""
Write-Host "Postgres is up and migrations are applied." -ForegroundColor Green
Write-Host "Backend and frontend are launched by the VS Code task 'dev: start stack'." -ForegroundColor DarkGray
Write-Host "To stop postgres: .\stack-stop.ps1" -ForegroundColor DarkGray
