#!/usr/bin/env python3
"""Run #2520's bounded delayed-SIP qualification probe; write no market data.

PYTHONPATH=. uv run python scripts/probe_2520_alpaca_delayed_sip.py
"""

from __future__ import annotations

import json
import os

import httpx

from app.services.alpaca_delayed_sip_probe import ProbeRefusal, credential_headers, run_probe


def main() -> int:
    try:
        headers = credential_headers(
            key_id=os.environ.get("APCA_API_KEY_ID", ""),
            secret_key=os.environ.get("APCA_API_SECRET_KEY", ""),
        )
        with httpx.Client(headers=headers, follow_redirects=False) as client:
            result = run_probe(client)
    except ProbeRefusal as exc:
        print(json.dumps({"status": "refused", "reason": str(exc)}, sort_keys=True))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
