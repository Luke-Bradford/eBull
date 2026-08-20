#!/usr/bin/env bash
# Superseded by loop_status.sh, which reports every registered loop and scopes
# each probe to that loop's worktree. Kept as a shim because this path is in
# the plist headers, in the issue handoffs and in operator muscle memory — and
# a status command that silently disappears is worse than one that redirects.
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/loop_status.sh" ta "$@"
