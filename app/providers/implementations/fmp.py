"""
Financial Modelling Prep (FMP) fundamentals provider.

Implements FundamentalsProvider against the FMP API.
Full implementation is built in issue #4 (filings and fundamentals).
"""

from datetime import date

from app.providers.fundamentals import FundamentalsProvider, FundamentalsSnapshot


class FmpFundamentalsProvider(FundamentalsProvider):
    """
    Fetches normalised fundamentals from FMP.

    Requires FMP_API_KEY in environment settings.
    If official filing data disagrees with FMP data, prefer the filing.
    """

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    def get_latest_snapshot(self, symbol: str) -> FundamentalsSnapshot | None:
        raise NotImplementedError("Implemented in issue #4")

    def get_snapshot_history(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
    ) -> list[FundamentalsSnapshot]:
        raise NotImplementedError("Implemented in issue #4")
