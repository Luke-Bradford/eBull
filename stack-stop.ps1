#!/usr/bin/env pwsh
# stack-stop.ps1 — stop the full eBull dev stack.
#
# Closes the backend and frontend windows launched by stack.ps1 (matched by
# window title) and stops the postgres container. Postgres data is
# preserved in the `pgdata` docker volume.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot

Write-Host "Stopping backend and frontend windows..." -ForegroundColor Cyan
$titles = @("eBull backend :8000", "eBull frontend :5173")
Get-Process pwsh, powershell -ErrorAction SilentlyContinue | Where-Object {
    $titles -contains $_.MainWindowTitle
} | ForEach-Object {
    Write-Host "  killing $($_.MainWindowTitle) (pid $($_.Id))" -ForegroundColor Gray
    Stop-Process -Id $_.Id -Force
}

Write-Host "Stopping postgres..." -ForegroundColor Cyan
docker compose stop

Write-Host "Stack stopped." -ForegroundColor Green
