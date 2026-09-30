"""#3515 — what a trial version's pack builder returns or raises (fund-v1 spec §5, §7).

Its own module so a version's builder and the descriptor module can both import it without a
cycle. Not one of ``ai_trial_policy.POLICY_MODULES``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from app.services.ai_trial_pack_reader import Pack


@dataclass(frozen=True)
class BuiltPack:
    pack: Pack
    #: Extra ``ai_trial_runs`` columns the version records on the run, whatever its outcome.
    run_record: Mapping[str, Any] = field(default_factory=dict)


class PackRefusal(Exception):
    """A version's pack builder refuses the RUN (fund-v1 §5: a read error, a coverage or budget
    refusal refuses the run, never drops a name). ``reason`` is the recorded refusal code and
    ``run_record`` the extra columns recorded with it."""

    def __init__(self, reason: str, run_record: Mapping[str, Any] | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.run_record: Mapping[str, Any] = run_record or {}


#: §3 step 2 for one version: ``(conn, *, step1, account, fetch_intraday, declaration) -> BuiltPack``.
PackBuilder = Callable[..., BuiltPack]

__all__ = ["BuiltPack", "PackBuilder", "PackRefusal"]
