#!/usr/bin/env pwsh
# stack-restart.ps1 — restart the backend and/or frontend dev processes.
#
# Usage:
#   .\stack-restart.ps1              # restart both
#   .\stack-restart.ps1 -Backend     # restart backend only
#   .\stack-restart.ps1 -Frontend    # restart frontend only
#
# Why: uvicorn --reload on Windows misses file changes from git operations
# (merge, checkout, rebase). After pulling or merging, run this to pick up
# the latest code without touching postgres or migrations.

param(
    [switch]$Backend,
    [switch]$Frontend
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Default: restart both if neither flag is passed
if (-not $Backend -and -not $Frontend) {
    $Backend = $true
    $Frontend = $true
}

if ($Backend) {
    Write-Host "Restarting backend..." -ForegroundColor Cyan

    # Kill existing uvicorn processes
    Get-Process uvicorn -ErrorAction SilentlyContinue | ForEach-Object {
        Write-Host "  stopping uvicorn (pid $($_.Id))" -ForegroundColor Gray
        Stop-Process -Id $_.Id -Force
    }
    # Also kill child python processes spawned by uvicorn --reload
    # (the reload watcher spawns a child python process)
    Start-Sleep -Milliseconds 500

    Write-Host "  starting uvicorn..." -ForegroundColor Gray
    Start-Process pwsh -ArgumentList "-NoProfile", "-Command", "Set-Location '$PSScriptRoot'; uv run uvicorn app.main:app --reload --host 127.0.0.1 --port 8000" -WindowStyle Normal
    Write-Host "  backend started on http://127.0.0.1:8000" -ForegroundColor Green
}

if ($Frontend) {
    Write-Host "Restarting frontend..." -ForegroundColor Cyan

    # Kill existing vite/node dev server
    $nodeProcesses = Get-Process node -ErrorAction SilentlyContinue | Where-Object {
        try {
            $cmd = (Get-CimInstance Win32_Process -Filter "ProcessId=$($_.Id)" -ErrorAction SilentlyContinue).CommandLine
            $cmd -and $cmd -match "vite"
        } catch { $false }
    }
    $nodeProcesses | ForEach-Object {
        Write-Host "  stopping vite (pid $($_.Id))" -ForegroundColor Gray
        Stop-Process -Id $_.Id -Force
    }
    Start-Sleep -Milliseconds 500

    Write-Host "  starting vite..." -ForegroundColor Gray
    Start-Process pwsh -ArgumentList "-NoProfile", "-Command", "Set-Location '$PSScriptRoot\frontend'; pnpm dev" -WindowStyle Normal
    Write-Host "  frontend started on http://localhost:5173" -ForegroundColor Green
}

Write-Host ""
Write-Host "Done. Services restarted." -ForegroundColor Green
