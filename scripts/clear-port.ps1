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

# Parse netstat columns explicitly rather than substring-matching the
# port number. A bare `:$Port\s` regex would match `:58000 ` when the
# target port is 8000 only if the digits aligned (they don't, in
# practice — `\s` anchors the right side), but the parsed form
# anchors the port at the end of the local-address column and is
# immune to future netstat layout changes / IPv6 brackets.
$holders = netstat -ano |
    ForEach-Object {
        $cols = ($_.Trim() -split '\s+')
        # Expected layout: <proto> <local-addr:port> <foreign-addr:port> <state> <pid>
        if ($cols.Count -ge 5 -and $cols[3] -eq "LISTENING") {
            $localPort = ($cols[1] -split ':')[-1]
            if ($localPort -eq "$Port") {
                $cols[-1]
            }
        }
    } |
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
