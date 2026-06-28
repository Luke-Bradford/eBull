#!/usr/bin/env bash
# scripts/autonomy/board.sh — the autonomy loop maintains the GitHub Projects v2
# board ("eBull engineering board") inline, using the loop's EXISTING `gh` auth
# (the keyring token already carries the `project` scope). No PAT, no GitHub
# Action, no repo secret.
#
# Usage:
#   board.sh status <issue#> "<Status>"   # ensure the card exists, set its Status column
#   board.sh add    <issue#>              # ensure the card exists (lands in the default column)
#
# <Status> is matched case-insensitively against the board's Status options, so
# any column that exists works (Todo / Blocked / In Progress / In Review / Done —
# and a future "QA" column the moment it is added, with no change here).
#
# BEST-EFFORT BY DESIGN: board upkeep must NEVER block engineering work. Every
# failure path warns to stderr and exits 0. A board hiccup is not a merge blocker.
set -uo pipefail

OWNER="Luke-Bradford"
PROJECT_TITLE="eBull engineering board"

warn() { echo "board.sh: $*" >&2; }

cmd="${1:-}"; issue="${2:-}"; status="${3:-}"
if [ -z "$cmd" ] || [ -z "$issue" ]; then
  warn 'usage: board.sh status <issue#> "<Status>" | add <issue#>'
  exit 0
fi

# One query: project id + Status field id + option(name->id) map.
meta="$(gh api graphql -f query='
  query($o:String!){ user(login:$o){ projectsV2(first:30){ nodes{
    id title
    field(name:"Status"){ ... on ProjectV2SingleSelectField{ id options{ id name } } }
  }}}}' -f o="$OWNER" 2>/dev/null)" || { warn "board metadata query failed (skip)"; exit 0; }

# Resolve PID / FID / OPT_ID via python (robust to spaces/casing).
ids="$(PROJECT_TITLE="$PROJECT_TITLE" STATUS="$status" python3 - "$meta" <<'PY' 2>/dev/null
import sys, json, os
t = os.environ["PROJECT_TITLE"]; want = os.environ.get("STATUS", "")
d = json.loads(sys.argv[1])
proj = next((n for n in d["data"]["user"]["projectsV2"]["nodes"] if n["title"] == t), None)
if not proj:
    print(); sys.exit(0)
f = proj.get("field") or {}
oid = ""
for o in (f.get("options") or []):
    if o["name"].lower() == want.lower():
        oid = o["id"]
print(proj["id"], f.get("id", ""), oid)
PY
)"
read -r PID FID OPT_ID <<<"$ids"
if [ -z "${PID:-}" ]; then warn "project '$PROJECT_TITLE' not found (skip)"; exit 0; fi

# Issue node id.
NID="$(gh issue view "$issue" --json id --jq .id 2>/dev/null)" || { warn "issue #$issue not found (skip)"; exit 0; }
if [ -z "$NID" ]; then warn "issue #$issue not found (skip)"; exit 0; fi

# Existing card for this issue on THIS project?
ITEM="$(gh api graphql -f query='query($n:ID!){node(id:$n){... on Issue{projectItems(first:20){nodes{id project{id}}}}}}' -f n="$NID" 2>/dev/null \
  | PID="$PID" python3 -c 'import sys,json,os; d=json.load(sys.stdin); p=os.environ["PID"]; ns=d["data"]["node"]["projectItems"]["nodes"]; print(next((i["id"] for i in ns if i["project"]["id"]==p), ""))' 2>/dev/null)"

if [ -z "${ITEM:-}" ]; then
  ITEM="$(gh api graphql -f query='mutation($p:ID!,$c:ID!){addProjectV2ItemById(input:{projectId:$p,contentId:$c}){item{id}}}' -f p="$PID" -f c="$NID" --jq '.data.addProjectV2ItemById.item.id' 2>/dev/null)"
  if [ -z "${ITEM:-}" ]; then warn "could not add #$issue to board (skip)"; exit 0; fi
  warn "added #$issue to board"
fi

if [ "$cmd" = "add" ]; then exit 0; fi

if [ "$cmd" = "status" ]; then
  if [ -z "${FID:-}" ]; then warn "Status field not found (skip)"; exit 0; fi
  if [ -z "${OPT_ID:-}" ]; then warn "status '$status' is not a board column (skip)"; exit 0; fi
  if gh api graphql -f query='mutation($p:ID!,$i:ID!,$f:ID!,$o:String!){updateProjectV2ItemFieldValue(input:{projectId:$p,itemId:$i,fieldId:$f,value:{singleSelectOptionId:$o}}){projectV2Item{id}}}' -f p="$PID" -f i="$ITEM" -f f="$FID" -f o="$OPT_ID" >/dev/null 2>&1; then
    warn "#$issue -> $status"
  else
    warn "failed to set #$issue status (skip)"
  fi
  exit 0
fi

warn "unknown command '$cmd' (skip)"
exit 0
