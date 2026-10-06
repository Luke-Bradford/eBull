"""The operator push channel: one ntfy topic (#3614 item 3).

Alerts written as rows are rows nobody reads — the jobs child was dark for
14 h on 2026-09-27 and nothing told anyone. This module is the one place that
turns an alert into a phone notification.

Channel: ntfy (https://docs.ntfy.sh/publish/). A message is an HTTP POST whose
body is the text, with ``Title``, ``Priority`` (1-5) and ``Tags`` headers and
an optional ``Authorization: Bearer <token>`` for a protected topic. It needs
no account, no SDK and no new dependency (stdlib ``urllib``), and the operator
subscribes from the ntfy phone app.

Opt-in by configuration: nothing is sent unless ``EBULL_NTFY_TOPIC`` is set.
On the public server the topic name is the only read capability, so it must be
an unguessable string and must never be committed. ``EBULL_NTFY_SERVER``
overrides the server (default ``https://ntfy.sh``); ``EBULL_NTFY_TOKEN`` adds
bearer auth.

Read from ``os.environ`` rather than ``app.config.Settings`` on purpose: the
dead-man (``scripts/jobs_dead_man.py``) must still be able to page when the
app's own configuration is what broke.

Nothing here raises. A push is best-effort; the caller's own record (a row,
a status file) stays the source of truth.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Literal

DEFAULT_SERVER = "https://ntfy.sh"
_TIMEOUT_S = 10.0

# ntfy priority scale: 1 min, 3 default, 5 max ("urgent"; bypasses Do Not
# Disturb on the phone apps).
Priority = Literal[1, 2, 3, 4, 5]


@dataclass(frozen=True)
class PushConfig:
    server: str
    topic: str
    token: str | None


def config_from_env(env: dict[str, str] | None = None) -> PushConfig | None:
    """The configured channel, or ``None`` when no topic is set (push off)."""
    source = os.environ if env is None else env
    topic = source.get("EBULL_NTFY_TOPIC", "").strip()
    if not topic:
        return None
    server = source.get("EBULL_NTFY_SERVER", "").strip() or DEFAULT_SERVER
    token = source.get("EBULL_NTFY_TOKEN", "").strip() or None
    return PushConfig(server=server.rstrip("/"), topic=topic, token=token)


def build_request(
    config: PushConfig,
    *,
    title: str,
    message: str,
    priority: Priority = 3,
    tags: tuple[str, ...] = (),
) -> urllib.request.Request:
    """The ntfy publish request. Pure, so the wire shape is testable.

    Header values go out as latin-1 under ``http.client``; non-ASCII in the
    title would be mangled or rejected, so it is replaced. The body is UTF-8.
    """
    headers = {
        "Title": title.encode("ascii", "replace").decode("ascii"),
        "Priority": str(priority),
    }
    if tags:
        headers["Tags"] = ",".join(tags)
    if config.token:
        headers["Authorization"] = f"Bearer {config.token}"
    return urllib.request.Request(
        f"{config.server}/{config.topic}",
        data=message.encode("utf-8"),
        headers=headers,
        method="POST",
    )


def send_push(
    *,
    title: str,
    message: str,
    priority: Priority = 3,
    tags: tuple[str, ...] = (),
    config: PushConfig | None = None,
) -> bool:
    """Publish one notification. ``True`` only on a 2xx from the server.

    ``config`` defaults to the environment; with no topic configured this is a
    no-op that returns ``False``.
    """
    cfg = config if config is not None else config_from_env()
    if cfg is None:
        return False
    req = build_request(cfg, title=title, message=message, priority=priority, tags=tags)
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:  # noqa: S310 — https URL from operator config
            return 200 <= resp.status < 300
    except urllib.error.URLError, OSError, ValueError:
        return False
