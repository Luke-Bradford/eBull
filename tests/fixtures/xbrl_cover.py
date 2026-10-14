"""Synthetic SEC cover instances for the tax character tests (#3616 slice 3d-3a): contexts, ``dei`` facts and a
registrant cover with its (title, symbol) classes."""

from __future__ import annotations

AAPL_CIK = "0000320193"
XBRLI = "http://www.xbrl.org/2003/instance"
DEI = "http://xbrl.sec.gov/dei/2025"
SEC = "http://www.sec.gov/CIK"


def ctx(cid: str, cik: str = AAPL_CIK, *, scheme: str = SEC, where: str = "entity") -> str:
    """A context whose single SEC identifier sits where ``where`` says: ``entity`` (direct), ``context`` (directly
    under the context), ``scenario`` (under ``xbrli:scenario``) or ``none``."""
    ident = f'<xbrli:identifier scheme="{scheme}">{cik}</xbrli:identifier>'
    body = {
        "entity": f"<xbrli:entity>{ident}</xbrli:entity>",
        "context": ident,
        "scenario": f"<xbrli:entity/><xbrli:scenario>{ident}</xbrli:scenario>",
        "none": "<xbrli:entity/>",
    }[where]
    period = "<xbrli:period><xbrli:instant>2026-06-27</xbrli:instant></xbrli:period>"
    return f'<xbrli:context id="{cid}">{body}{period}</xbrli:context>'


def fact(name: str, ref: str | None, text: str) -> str:
    attr = "" if ref is None else f' contextRef="{ref}"'
    return f"<dei:{name}{attr}>{text}</dei:{name}>"


def instance(contexts: list[str], facts: list[str], *, dei: str = DEI, root: str = XBRLI) -> bytes:
    return (
        f'<?xml version="1.0" encoding="utf-8"?><xbrli:xbrl xmlns:xbrli="{root}" xmlns:dei="{dei}">'
        + "".join(contexts)
        + "".join(facts)
        + "</xbrli:xbrl>"
    ).encode()


def cover(cik: str, classes: list[tuple[str, str | None]], *, ecik: str | None = None) -> bytes:
    """A registrant context carrying ``EntityCentralIndexKey`` and one context per (title, symbol) class; a class
    with no symbol is an untraded note."""
    contexts, facts = [ctx("c0", cik)], [fact("EntityCentralIndexKey", "c0", ecik or cik)]
    for n, (title, symbol) in enumerate(classes, start=1):
        contexts.append(ctx(f"c{n}", cik))
        facts.append(fact("Security12bTitle", f"c{n}", title))
        if symbol is not None:
            facts += [fact("TradingSymbol", f"c{n}", symbol), fact("SecurityExchangeName", f"c{n}", "NASDAQ")]
    return instance(contexts, facts)
