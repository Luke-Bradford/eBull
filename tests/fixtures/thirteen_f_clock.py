"""Pin the 13F-HR 8-quarter retention floor for fixture-dated tests.

The 13F parser and ingester fixtures are locked at period 2024-Q3 / 2024-Q4, and the floor
(``thirteen_f_retention_cutoff``) follows the wall clock, so the fixtures aged out on
2026-10-01 when 2026-Q3 completed and every test that expected a write got ``retention floor``.
This caps the floor's default clock at the date these modules already use as their fixed clock
(2026-05-20, cutoff 2024-06-30). A test that passes ``now``, or patches the module's
``datetime`` to an earlier instant (the VALUE-cutover test anchors 2023-01-15), is unaffected.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from app.services import institutional_holdings

THIRTEEN_F_FIXED_NOW = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def pin_thirteen_f_retention_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    real = institutional_holdings.thirteen_f_retention_cutoff

    def cutoff(now: datetime | None = None) -> date:
        if now is None:
            # The module's own clock, so a test's `patch(...institutional_holdings.datetime)` holds.
            now = min(institutional_holdings.datetime.now(tz=UTC), THIRTEEN_F_FIXED_NOW)
        return real(now)

    monkeypatch.setattr(institutional_holdings, "thirteen_f_retention_cutoff", cutoff)
