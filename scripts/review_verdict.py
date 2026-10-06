"""The review bot's verdict, read the way the merge gate reads it (#3613 item 1).

A copy of ``review_verdict`` in autonomy-engine's ``engine/bin/safe_merge.sh``, so the required ``review`` check and
``safe_merge`` judge the same text the same way. The engine lives in another repository and cannot be imported here.
If the copies drift, one gate refuses what the other passes; neither can let a non-APPROVE merge.

Usage: ``python3 scripts/review_verdict.py < review.txt`` prints ``approve``, ``block`` or ``none`` and exits 0 only
for ``approve``. Standard library only: the review job runs it without installing dependencies.
"""

from __future__ import annotations

import re
import sys

_BLOCKING = re.compile(r"^\s*#{1,6}\s*\[BLOCKING\]", re.I)
#: A heading (``### Verdict``) or an inline label with a colon (``Verdict:`` / ``**Verdict:**``); the last one wins.
_MARKER = r"^\s*(#{1,6}\s*\**\s*Verdict\b\**\s*:?|\**\s*Verdict\s*\**\s*:\s*\**)"
#: The verdict token must lead the Verdict text; "Cannot **APPROVE** until X" is not a verdict.
_LEAD = re.compile(r"\s*\**\s*(APPROVE|REQUEST CHANGES|NEEDS DISCUSSION)\b", re.I)


def review_verdict(body: str) -> str:
    lines = body.splitlines()
    if any(_BLOCKING.match(line) for line in lines):
        return "block"
    marks = [i for i, line in enumerate(lines) if re.match(_MARKER, line, re.I)]
    if not marks:
        return "none"
    section = [re.sub(_MARKER, "", lines[marks[-1]], flags=re.I)]
    for line in lines[marks[-1] + 1 :]:
        if re.match(r"^\s*#{1,6}\s", line):
            break
        section.append(line)
    lead = _LEAD.match("\n".join(section))
    token = lead.group(1).upper() if lead else None
    return {"APPROVE": "approve", "REQUEST CHANGES": "block"}.get(token or "", "none")


def main() -> int:
    verdict = review_verdict(sys.stdin.read())
    print(verdict)
    return 0 if verdict == "approve" else 1


if __name__ == "__main__":
    sys.exit(main())
