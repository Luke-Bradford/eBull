#!/usr/bin/env pwsh
# scripts/clear-port.ps1 — kill any process LISTENING on a TCP port.
#
# Used by .vscode/tasks.json `stack: backend` + `stack: frontend` to
# reap orphaned vite / uvicorn processes before launching new ones.
# Without this, closing the VS Code window without "Terminate Task"
# leaves the background process holding the port; the next session
# either silently port-hops (vite default) or fails to bind.
#
# Mirrors the kill_port helper in scripts/clear-port.sh. The full
# ghost-socket-aware Clear-Port function in stack-restart.ps1 stays
# for the explicit restart path; this lightweight wrapper covers the
# common case (real process, no kernel-zombie socket).
#
# Usage:
#   pwsh ./scripts/clear-port.ps1 5173
#   pwsh ./scripts/clear-port.ps1 8000

param(
    [Parameter(Mandatory = $true, Position = 0)]
    [int]$Port
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

$holders = netstat -ano |
    Select-String ":$Port\s.*LISTENING" |
    ForEach-Object { ($_ -split '\s+')[-1] } |
    Sort-Object -Unique |
    Where-Object { $_ -and $_ -ne "0" }

if (-not $holders) {
    exit 0
}

foreach ($holderPid in $holders) {
    $proc = Get-Process -Id $holderPid -ErrorAction SilentlyContinue
    if ($proc) {
        Write-Host "clear-port: killing $($proc.ProcessName) on :$Port (pid $holderPid)"
        Stop-Process -Id $holderPid -Force -ErrorAction SilentlyContinue
    }
}

Start-Sleep -Milliseconds 500
