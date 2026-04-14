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

    # Kill existing uvicorn parent processes and their child python workers
    # (uvicorn --reload spawns a child python process that holds the port)
    $uvicornProcs = Get-Process uvicorn -ErrorAction SilentlyContinue
    if ($uvicornProcs) {
        $uvicornPids = $uvicornProcs | ForEach-Object { $_.Id }
        # Kill child python processes spawned by uvicorn
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object { $_.ParentProcessId -in $uvicornPids -and $_.Name -eq "python.exe" } |
            ForEach-Object {
                Write-Host "  stopping python worker (pid $($_.ProcessId))" -ForegroundColor Gray
                Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
            }
        # Kill uvicorn parents
        $uvicornProcs | ForEach-Object {
            Write-Host "  stopping uvicorn (pid $($_.Id))" -ForegroundColor Gray
            Stop-Process -Id $_.Id -Force
        }
    }
    Start-Sleep -Milliseconds 500

    # Also kill any remaining python processes bound to port 8000
    $portHolders = netstat -ano | Select-String ":8000.*LISTENING" | ForEach-Object {
        ($_ -split '\s+')[-1]
    } | Sort-Object -Unique
    foreach ($pid in $portHolders) {
        if ($pid -and $pid -ne "0") {
            Write-Host "  killing orphan on :8000 (pid $pid)" -ForegroundColor Gray
            Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
        }
    }

    Write-Host "  starting uvicorn..." -ForegroundColor Gray
    $proc = Start-Process pwsh -ArgumentList "-NoProfile", "-Command", "Set-Location '$PSScriptRoot'; uv run uvicorn app.main:app --reload --host 127.0.0.1 --port 8000" -WindowStyle Normal -PassThru
    Start-Sleep -Seconds 2
    if ($proc.HasExited) {
        Write-Error "  backend failed to start (exited with code $($proc.ExitCode))"
    } else {
        Write-Host "  backend started on http://127.0.0.1:8000 (pid $($proc.Id))" -ForegroundColor Green
    }
}

if ($Frontend) {
    Write-Host "Restarting frontend..." -ForegroundColor Cyan

    # Kill existing vite/node dev server on port 5173
    $portHolders = netstat -ano | Select-String ":5173.*LISTENING" | ForEach-Object {
        ($_ -split '\s+')[-1]
    } | Sort-Object -Unique
    foreach ($pid in $portHolders) {
        if ($pid -and $pid -ne "0") {
            Write-Host "  stopping process on :5173 (pid $pid)" -ForegroundColor Gray
            Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
        }
    }
    Start-Sleep -Milliseconds 500

    Write-Host "  starting vite..." -ForegroundColor Gray
    $proc = Start-Process pwsh -ArgumentList "-NoProfile", "-Command", "Set-Location '$PSScriptRoot\frontend'; pnpm dev" -WindowStyle Normal -PassThru
    Start-Sleep -Seconds 2
    if ($proc.HasExited) {
        Write-Error "  frontend failed to start (exited with code $($proc.ExitCode))"
    } else {
        Write-Host "  frontend started on http://localhost:5173 (pid $($proc.Id))" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "Done. Services restarted." -ForegroundColor Green
