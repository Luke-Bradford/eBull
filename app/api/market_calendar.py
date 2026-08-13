"""US market-calendar endpoint for intraday chart session shading (#609 Phase A).

Serves the NYSE full-closure + early-close ("half") day sets for a year so
the frontend can shade closed days / half-day afternoons on the intraday
chart. Instrument-independent + deterministic, so it is heavily cacheable;
the frontend fetches at most the year(s) the visible bar range spans and
client-caches.

Authoritative calendar logic lives in ``app.services.market_calendar`` (pure,
pytest-tested); this is a thin serialisation layer.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response

from app.api.auth import require_session_or_service_token
from app.services.market_calendar import us_market_specials

router = APIRouter(
    prefix="/market-calendar",
    tags=["market-calendar"],
    dependencies=[Depends(require_session_or_service_token)],
)

# Holiday primitives are valid far beyond our data range; the bound just
# rejects garbage input rather than computing for year 9999.
_MIN_YEAR = 2000
_MAX_YEAR = 2100

# The set is deterministic and slow-changing (NYSE publishes years ahead),
# so a day of CDN/browser staleness can never matter.
_CACHE_CONTROL = "public, max-age=86400"


@router.get("/us/{year}")
def get_us_market_calendar(year: int, response: Response) -> dict[str, object]:
    """NYSE full closures + 13:00 ET half days for ``year``.

    Dates are ``America/New_York`` civil dates (``YYYY-MM-DD``). The frontend
    must derive the year(s) to fetch from NY-local bar dates, not UTC/browser
    local, so bars near a Jan-1 / Dec-31 boundary resolve their specials.
    """
    if year < _MIN_YEAR or year > _MAX_YEAR:
        raise HTTPException(
            status_code=400,
            detail=f"year must be in [{_MIN_YEAR}, {_MAX_YEAR}]",
        )
    specials = us_market_specials(year)
    response.headers["Cache-Control"] = _CACHE_CONTROL
    return {
        "year": year,
        "full_closures": [d.isoformat() for d in sorted(specials.full_closures)],
        "half_days": [d.isoformat() for d in sorted(specials.half_days)],
    }
