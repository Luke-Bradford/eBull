"""REFUTED: a high-identity escape hatch for degraded Item 403 headers (#2160).

Arm 1 round 1 found genuine Item 403 tables emptied because their ONLY surviving
caption is an amount column -- 0000805928-25-000059 renders
``| | | Number of Shares | | | | | | Number of Shares | |``, scores 4, and
extracts 20 holders at identity fraction 1.00 (BlackRock, First Light Asset
Management LLC, Soleus Capital Master Fund LP). D4 rejects it on headers alone.

The natural fix is to let overwhelming ROW evidence carry a table whose HEADERS
have degraded: eligible if ``frac >= HIGH`` and some amount indicator is present.

It does not survive measurement, which is why this probe is committed rather
than the idea. Full population, 42,577 bodies:

    baseline (no escape)            emptied=199  dropped=423  extra-admits=0
    escape hi=0.90                  emptied=119  dropped=263  extra-admits=516
    escape hi=0.90 + entity-gated   emptied=170  dropped=362  extra-admits=143

The admits are dominated by Item 402 compensation tables -- 'Named Executives |
Number of Shares of Restricted Stock | Number of Performance Units',
'Beneficial Owner | Number of RSUs', 'Named Executive Officer | Shares at Target
| Final PSU Payout %', 'Name and Position | Number of Shares Subject to Stock
Options'. That violates this spec's BLOCKING acceptance (zero comp/plan/metric
tables admitted), so the escape hatch is rejected in both forms.

Gating on 403(a) institutional evidence (a row carrying an entity designator, or
Instruction 5's as-a-group row) narrows it but does not close it: a deferred-comp
table for a bank lists an affiliated entity among its rows.

Counts here come from the census REPLAY and are approximate -- the replay
undercounts real ``main`` (85,554 vs 105,400 rows) because it does not dedup
across sibling tables. Use it to RANK variants, never to state an outcome; the
two-checkout A/B is the authority.

    PYTHONPATH=. uv run python scripts/probe_def14a_high_identity_escape.py DUMP.jsonl
"""

import collections
import json
import sys

sys.path.insert(0, ".")
import app.providers.implementations.sec_def14a as P
import scripts.analyse_def14a_window_tables as A

recs = []
for line in open(sys.argv[1]):
    line = line.strip()
    if not line.startswith("{"):
        continue
    r = json.loads(line)
    if "error" not in r:
        recs.append(r)


def has_403a_evidence(t):
    """>=1 row is an institutional entity (403(a) >5% holder) or Instruction 5's group row."""
    for nm in t["nm"]:
        s = (nm or "").strip()
        if "as a group" in s.lower():
            return True
        if P._OWNER_ENTITY_CASED_RE.search(s):
            return True
    return False


def sel(windows, hi, need_entity):
    for tables in windows:
        q = [t for t in tables if t["s"] >= 3]
        el = []
        for t in q:
            j = A.joined_headers(t, "both")
            f = A.identity_fraction(t)
            ok = P._item403_value_signature((j,)) and f >= 0.5
            if not ok and hi is not None and f >= hi and P._ITEM403_AMOUNT_IND_RE.search(j):
                if (not need_entity) or has_403a_evidence(t):
                    ok = True
            if ok:
                el.append(t)
        if el:
            return el
    return []


base_cache = {}
for label, hi, ne in (
    ("baseline", None, False),
    ("escape+entity hi=0.9", 0.90, True),
    ("escape+entity hi=0.8", 0.80, True),
):
    zero = 0
    dropped = 0
    gained = collections.Counter()
    gex = {}
    for r in recs:
        _, m = A.main_selection(r["windows"])
        b = sel(r["windows"], hi, ne)
        if any(t["n"] > 0 for t in m) and not any(t["n"] > 0 for t in b):
            zero += 1
        mk = {A.table_key(t) for t in m}
        bk = {A.table_key(t) for t in b}
        dropped += sum(1 for t in m if A.table_key(t) not in bk and t["n"] > 0)
        if hi is not None:
            base = set(A.table_key(t) for t in sel(r["windows"], None, False))
            for t in b:
                if A.table_key(t) not in base and t["n"] > 0:
                    h = A.joined_headers(t, "both")[:100]
                    gained[h] += 1
                    gex.setdefault(h, f"{r['accession']} n={t['n']} {t['nm'][:2]}")
    print(f"\n=== {label}: emptied={zero} dropped={dropped} extra-admits={sum(gained.values())} shapes={len(gained)}")
    for h, c in gained.most_common(14):
        print(f"  {c:>4}  {h}")
        print(f"        {gex[h]}")
