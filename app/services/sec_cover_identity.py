"""Read the Section 12(b) security table from a 10-K / 10-Q / 20-F cover XBRL instance.

Source rule: the cover's 12(b) table is tagged ``dei:Security12bTitle`` +
``dei:TradingSymbol`` + ``dei:SecurityExchangeName`` in ONE shared context per
registered class (Reg S-K Item 601(b)(104), Reg S-T Rule 406). A context carrying all
three identifies one listed class.

Lifted from ``scripts/census_2900_sec_cover_identity.py`` (#2351) so the DEF 14A
recipient job and the census share one reader. ``require_dei`` is the job's stricter
mode: facts outside the ``dei`` taxonomy namespace are ignored.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Final

import lxml.etree as ET

from app.services.xbrl_instance import context_dimensions, context_period

SECURITY_FACTS: Final = frozenset({"Security12bTitle", "TradingSymbol", "SecurityExchangeName"})
COVER_FACTS: Final = SECURITY_FACTS | {"DocumentPeriodEndDate"}


def _is_dei(tag: str) -> bool:
    # dei namespaces are ``http://xbrl.sec.gov/dei/<year>``.
    return "xbrl.sec.gov/dei/" in (ET.QName(tag).namespace or "")


def parse_cover_contexts_root(root: ET._Element, *, require_dei: bool = False) -> list[dict[str, Any]]:
    contexts: dict[str, ET._Element] = {}
    for element in root.iter():
        if not isinstance(element.tag, str) or ET.QName(element.tag).localname != "context":
            continue
        context_id = element.get("id")
        if context_id:
            contexts[context_id] = element

    values: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for element in root.iter():
        if not isinstance(element.tag, str):
            continue
        local = ET.QName(element.tag).localname
        if require_dei and local in COVER_FACTS and not _is_dei(element.tag):
            continue
        context_ref = element.get("contextRef")
        text = " ".join("".join(element.itertext()).split())
        if local in COVER_FACTS and context_ref and text:
            values[context_ref][local].append(text)

    document_periods = sorted({value for facts in values.values() for value in facts.get("DocumentPeriodEndDate", [])})
    if len(document_periods) != 1:
        return []

    out: list[dict[str, Any]] = []
    for context_ref, facts in sorted(values.items()):
        # The three security facts identify one listed class through their
        # shared context. DocumentPeriodEndDate is document-level and may use
        # the default context, so require one unique value across the instance.
        if not SECURITY_FACTS.issubset(facts):
            continue
        context = contexts.get(context_ref)
        if context is None:
            continue
        out.append(
            {
                "context_ref": context_ref,
                "dimensions": context_dimensions(context),
                "period": context_period(context),
                "facts": {
                    "DocumentPeriodEndDate": document_periods,
                    **{key: sorted(set(facts[key])) for key in sorted(SECURITY_FACTS)},
                },
            }
        )
    return out


def entity_ciks(root: ET._Element) -> frozenset[str]:
    """Distinct ``dei:EntityCentralIndexKey`` values, zero-padded to 10 digits."""
    out: set[str] = set()
    for element in root.iter():
        if not isinstance(element.tag, str) or not _is_dei(element.tag):
            continue
        if ET.QName(element.tag).localname != "EntityCentralIndexKey":
            continue
        text = "".join(element.itertext()).strip()
        if text.isdigit():
            out.add(text.zfill(10))
    return frozenset(out)


def cover_pairs(contexts: list[dict[str, Any]]) -> frozenset[tuple[str, str]]:
    """Unambiguous (title, symbol) pairs.

    A context with exactly one title and one symbol yields a pair. A symbol that also
    appears in a context carrying several titles or symbols is ambiguous and is
    dropped entirely. Titles are whitespace-collapsed, symbols trimmed and upper-cased.
    """
    pairs: set[tuple[str, str]] = set()
    ambiguous: set[str] = set()
    for ctx in contexts:
        facts = ctx["facts"]
        titles = [" ".join(t.split()) for t in facts["Security12bTitle"] if t.strip()]
        symbols = [s.strip().upper() for s in facts["TradingSymbol"] if s.strip()]
        if len(titles) == 1 and len(symbols) == 1:
            pairs.add((titles[0], symbols[0]))
        else:
            ambiguous.update(symbols)
    return frozenset((t, s) for t, s in pairs if s not in ambiguous)
