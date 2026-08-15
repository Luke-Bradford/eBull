"""One Nasdaq halt-feed identity for every strategy submission path (#2709).

Nasdaq's halt feed publishes the issue symbol from its Symbol Directory; it does
not know eToro's session/listing suffixes. eBull's measured eToro US-equity
catalog contains four suffixes that are venue metadata rather than part of that
issue symbol: ``.RTH``, ``.24-7``, ``.US`` and ``.CH``. They are stripped here,
narrowly.

Dots are otherwise load-bearing. ``BRK.B`` and ``BF.A`` are distinct share
classes and Nasdaq itself publishes those dotted identities, while catalog rows
such as ``.CVR`` and ``.WS`` can denote distinct securities. A generic
``split('.')`` or regexp would therefore turn a safety check into a different
fail-open.

Primary-source contract (verified 2026-08-15):

* https://classic.nasdaqtrader.com/Trader.aspx?id=tradehaltcodes defines the
  feed's field as "Issue Symbol — Symbol of the Issue".
* https://classic.nasdaqtrader.com/Trader.aspx?id=TradingHaltSearch requires a
  symbol exactly as it appears in Nasdaq's Symbol Directory.
* https://www.etoro.com/markets/tcom.ch identifies eToro's ``TCOM.CH`` as the
  Nasdaq-delayed Trip.com ADR, while https://www.nasdaq.com/market-activity/stocks/tcom
  identifies that Nasdaq-listed issue as ``TCOM``.

The eToro suffix census is repository evidence: ``research_corpus_ingest.py``
records ``.RTH``/``.24-7`` as venue variants and ``.US`` as a primary-listing
suffix. Dev census on 2026-08-15 found 595/9/239/7 tradable US-equity rows for
``.RTH``/``.24-7``/``.US``/``.CH``, plus 14 class-like dotted symbols which
must remain unchanged.

The SQL fragment deliberately names the canonical query alias ``i``. Both the
paper executor and core preflight interpolate this exact LiteralString, so the
normalisation cannot drift between the two order paths.
"""

from typing import Final, LiteralString

HALT_IDENTITY_RULE_VERSION: Final = "nasdaq-etoro-halt-identity-v1"

INSTRUMENT_HALT_SYMBOL_SQL: Final[LiteralString] = """
CASE
    WHEN right(upper(i.symbol), 5) = '.24-7' THEN left(upper(i.symbol), -5)
    WHEN right(upper(i.symbol), 4) = '.RTH' THEN left(upper(i.symbol), -4)
    WHEN right(upper(i.symbol), 3) = '.US' THEN left(upper(i.symbol), -3)
    WHEN right(upper(i.symbol), 3) = '.CH' THEN left(upper(i.symbol), -3)
    ELSE upper(i.symbol)
END
"""
