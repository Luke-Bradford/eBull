"""Mint a dev operator session cookie for headless browser navigation.

The frontend authenticates with an HttpOnly ``ebull_session`` cookie that JS
cannot set — so an isolated browser (chrome-devtools / Playwright) can't reach
the app without a real session. This mints one for the sole operator (reusing
the real ``create_session`` path, so it validates server-side like a true
login) and prints the cookie + base URL as JSON. The caller injects it via
Playwright ``context.addCookies`` (which CAN set HttpOnly cookies) then
navigates.

Dev only — never wire into product. Run::

    uv run python scripts/dev_browser_session.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta

import psycopg

from app.config import settings
from app.security.sessions import create_session
from app.services.operators import sole_operator_id

_DEV_BASE_URL = "http://localhost:5173"
_SESSION_TTL = timedelta(hours=12)


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        operator_id = sole_operator_id(conn)
        session_id, expires_at = create_session(
            conn,
            operator_id=operator_id,
            user_agent="dev_browser_session.py",
            ip="127.0.0.1",
            absolute_timeout=_SESSION_TTL,
        )
        conn.commit()

    out = {
        "cookie_name": settings.session_cookie_name,
        "session_id": session_id,
        "base_url": _DEV_BASE_URL,
        "operator_id": str(operator_id),
        "expires_at": expires_at.astimezone().isoformat(),
        "minted_at": datetime.now().astimezone().isoformat(),
        "playwright_cookie": {
            "name": settings.session_cookie_name,
            "value": session_id,
            "url": _DEV_BASE_URL,
            "httpOnly": True,
            "sameSite": "Lax",
        },
    }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
