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

# ---------------------------------------------------------------------------
# Clear-Port — kill real processes and wait out ghost sockets on a port.
#
# Windows can leave TCP sockets in LISTENING state after a process dies
# (zombie/ghost sockets). These PIDs don't exist in the process table so
# Stop-Process fails silently. The only fix is to wait for the kernel to
# reclaim the handle (usually 10-30 seconds after the last connection closes).
# ---------------------------------------------------------------------------
function Clear-Port {
    param([int]$Port, [int]$MaxWaitSeconds = 30)

    $elapsed = 0
    while ($elapsed -lt $MaxWaitSeconds) {
        $holders = netstat -ano |
            Select-String ":$Port\s.*LISTENING" |
            ForEach-Object { ($_ -split '\s+')[-1] } |
            Sort-Object -Unique |
            Where-Object { $_ -and $_ -ne "0" }

        if (-not $holders) { return $true }

        $anyGhost = $false
        foreach ($holderPid in $holders) {
            $proc = Get-Process -Id $holderPid -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Host "  killing $($proc.ProcessName) on :$Port (pid $holderPid)" -ForegroundColor Gray
                Stop-Process -Id $holderPid -Force -ErrorAction SilentlyContinue
            } else {
                $anyGhost = $true
            }
        }

        if ($anyGhost) {
            Write-Host "  ghost socket on :$Port — waiting for kernel cleanup ($elapsed/$MaxWaitSeconds s)..." -ForegroundColor Yellow
            Start-Sleep -Seconds 2
            $elapsed += 2
        } else {
            # Killed real processes — brief pause for socket release
            Start-Sleep -Milliseconds 500
            $elapsed += 1
        }
    }

    # Final check
    $remaining = netstat -ano |
        Select-String ":$Port\s.*LISTENING" |
        Measure-Object | Select-Object -ExpandProperty Count
    if ($remaining -gt 0) {
        Write-Warning "  port $Port still held after ${MaxWaitSeconds}s — ghost sockets may clear on their own"
        return $false
    }
    return $true
}

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

    # Clear port 8000 — handles both real processes and ghost sockets
    Clear-Port -Port 8000

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

    # Clear port 5173 — handles both real processes and ghost sockets
    Clear-Port -Port 5173

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
