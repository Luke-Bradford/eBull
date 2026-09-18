"""#2215 — the folded 13D/G channel becomes a legible memo overlay.

Pure tests, no DB. ``_blockholder_restatement_holders`` is a function of the
already-reconciled pie slices, so the whole decision is table-testable and the
DB-backed rollup tests stay untouched.

What each test pins is the rule it names in its own docstring; every one was
revert-probed against the implementation (see the PR).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    DroppedSource,
    Holder,
    OwnershipSlice,
    SliceCategory,
    SourceTag,
    _argmax_source,
    _blockholder_restatement_holders,
    _build_slice,
    count_additive_institutional_holders,
)

OUTSTANDING = Decimal(1000)


def _dropped(
    source: SourceTag,
    shares: str,
    *,
    accession: str,
    as_of: date | None = None,
    filer_cik: str | None = "0000001",
    filer_name: str = "",
) -> DroppedSource:
    """``filer_cik`` defaults to ``_holder``'s default CIK, i.e. the entry belongs to
    the holder it hangs on — the ordinary same-owner fold. Pass a different
    ``filer_cik`` for a FOREIGN entry: a co-filer's row appended to a representative
    by one of the group collapses (#3189 finding 15). Identity is CIK-first
    (:func:`_identity_key`), so ``filer_name`` only has to be set where a test asserts
    the rendered name."""
    return DroppedSource(
        source=source,
        shares=Decimal(shares),
        accession_number=accession,
        edgar_url=f"https://example.invalid/{accession}",
        as_of_date=as_of,
        filer_cik=filer_cik,
        filer_name=filer_name if filer_name is not None else "",
    )


def _holder(
    name: str,
    shares: str,
    *,
    source: SourceTag = "13f",
    accession: str = "survivor-accession",
    as_of: date | None = None,
    cik: str | None = "0000001",
    dropped: tuple[DroppedSource, ...] = (),
) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession,
        winning_edgar_url=f"https://example.invalid/{accession}",
        as_of_date=as_of,
        filer_type=None,
        dropped_sources=dropped,
    )


def _slice(category: SliceCategory, holders: list[Holder]) -> OwnershipSlice:
    return _build_slice(category, holders, OUTSTANDING)


def test_a_13g_folded_into_institutions_is_restated_from_the_DROPPED_entry() -> None:
    """The overlay row's provenance is the losing filing, never the survivor's.

    Stamping the survivor's source/accession would label a 13G figure with a 13F
    accession, and the survivor's ``as_of_date`` would corrupt the slice as-of
    coherence envelope (#1647 part 1).
    """
    survivor = _holder(
        "Vanguard",
        "500",
        source="13f",
        accession="13f-acc",
        as_of=date(2026, 6, 30),
        dropped=(_dropped("13g", "300", accession="13g-acc", as_of=date(2026, 2, 14), filer_name="Vanguard"),),
    )

    [restated] = _blockholder_restatement_holders([_slice("institutions", [survivor])])

    assert restated.shares == Decimal(300)
    assert restated.winning_source == "13g"
    assert restated.winning_accession == "13g-acc"
    assert restated.winning_edgar_url == "https://example.invalid/13g-acc"
    assert restated.as_of_date == date(2026, 2, 14)
    assert restated.filer_name == "Vanguard"
    assert restated.filer_cik == "0000001"
    assert restated.dropped_sources == ()


def test_the_blockholders_slice_itself_is_excluded() -> None:
    """A dropped 13G behind the same owner's surviving 13D is a WITHIN-channel
    supersession, already represented by the blockholder row. Re-listing it
    would show one stake twice in one panel."""
    survivor = _holder(
        "Activist LP",
        "400",
        source="13d",
        accession="13d-acc",
        dropped=(_dropped("13g", "380", accession="13g-acc"),),
    )

    assert _blockholder_restatement_holders([_slice("blockholders", [survivor])]) == []


def test_an_owner_with_both_a_dropped_13d_and_13g_is_restated_at_the_MAX() -> None:
    """A 13D and a 13G from one filer are overlapping restatements of the same
    block, not additive holdings (#1640/#1889 — MAX overlapping, SUM additive).
    Summing would double one invisible position."""
    survivor = _holder(
        "Dual Filer",
        "900",
        dropped=(
            _dropped("13d", "250", accession="13d-acc"),
            _dropped("13g", "310", accession="13g-acc"),
        ),
    )

    [restated] = _blockholder_restatement_holders([_slice("institutions", [survivor])])

    assert restated.shares == Decimal(310)
    assert restated.winning_accession == "13g-acc"


def test_non_blockholder_dropped_channels_are_ignored() -> None:
    """Only 13D/G restatements belong in this overlay — a dropped DEF 14A or
    Form 3 line is a different channel with its own surface."""
    survivor = _holder(
        "Officer",
        "120",
        source="form4",
        dropped=(
            _dropped("def14a", "119", accession="proxy-acc"),
            _dropped("form3", "100", accession="f3-acc"),
        ),
    )

    assert _blockholder_restatement_holders([_slice("insiders", [survivor])]) == []


def test_a_holder_with_no_dropped_sources_contributes_nothing() -> None:
    assert _blockholder_restatement_holders([_slice("institutions", [_holder("Plain", "700")])]) == []


def test_the_overlay_never_enters_the_additive_institutional_count() -> None:
    """The basis, not the category list, is what keeps the overlay out of the
    additive paths — ``count_additive_institutional_holders`` filters
    ``denominator_basis == "pie_wedge"``, so a restated 13F-adjacent owner
    cannot be counted a second time (#2232 arm 3's pigeonhole)."""
    institutions = _slice(
        "institutions",
        [_holder("Vanguard", "500", dropped=(_dropped("13g", "300", accession="13g-acc"),))],
    )
    overlay = _build_slice(
        "blockholders_restated",
        _blockholder_restatement_holders([institutions]),
        OUTSTANDING,
        denominator_basis="cross_channel_restatement",
    )

    assert overlay.denominator_basis == "cross_channel_restatement"
    assert overlay.filer_count == 1
    assert count_additive_institutional_holders([institutions]) == 1
    assert count_additive_institutional_holders([institutions, overlay]) == 1


def test_every_pie_category_except_blockholders_is_scanned() -> None:
    """``_reconcile_owner_once`` can route a 13D/G filer into any of the other
    three pie categories, so scanning must not be institutions-only."""
    slices = [
        _slice(
            "insiders",
            [
                _holder(
                    "Insider",
                    "100",
                    cik="A",
                    source="form4",
                    dropped=(_dropped("13d", "90", accession="a", filer_cik="A", filer_name="Insider"),),
                )
            ],
        ),
        _slice(
            "institutions",
            [
                _holder(
                    "Manager",
                    "200",
                    cik="B",
                    dropped=(_dropped("13g", "150", accession="b", filer_cik="B", filer_name="Manager"),),
                )
            ],
        ),
        _slice(
            "etfs",
            [
                _holder(
                    "ETF Sponsor",
                    "300",
                    cik="C",
                    dropped=(_dropped("13g", "250", accession="c", filer_cik="C", filer_name="ETF Sponsor"),),
                )
            ],
        ),
    ]

    assert sorted(h.filer_name for h in _blockholder_restatement_holders(slices)) == [
        "ETF Sponsor",
        "Insider",
        "Manager",
    ]


# ---------------------------------------------------------------------------
# #3189 findings 14 / 14b / 15 / 16 — corrections to the above.
# See docs/proposals/etl/2026-09-18-restatement-overlay-corrections.md
# ---------------------------------------------------------------------------


def test_a_13d_that_BEAT_the_13f_is_restated_from_the_surviving_row() -> None:
    """#3189 finding 14. ``_reconcile_owner_once`` branches on ``"13f" in present``,
    not on "13f won": when the 13D/G subtotal is the LARGER one it becomes
    ``figure_src`` while the category stays ``institutions``. The survivor is then the
    13D/G filing itself and carries no dropped blockholder entry, so a
    ``dropped_sources``-only scan emits nothing for the very case #2215 exists to
    cover. Measured at 3,862 holders / 2,295 instruments on the dev corpus, 1,007 of
    them with no blockholders wedge at all."""
    survivor = _holder(
        "Activist LP",
        "600",
        source="13d",
        accession="13d-acc",
        as_of=date(2026, 3, 31),
        dropped=(_dropped("13f", "400", accession="13f-acc"),),
    )

    [restated] = _blockholder_restatement_holders([_slice("institutions", [survivor])])

    assert restated.shares == Decimal(600)
    assert restated.winning_source == "13d"
    assert restated.winning_accession == "13d-acc"
    assert restated.as_of_date == date(2026, 3, 31)
    assert restated.filer_name == "Activist LP"


def test_an_insider_whose_13d_beat_their_form4_is_also_restated() -> None:
    """The same branch reaches ``insiders``: ``present & _INSIDER_SOURCES`` sets the
    category while ``figure_src`` is still the beneficial MAX, which can be the 13D."""
    survivor = _holder("Director", "500", source="13d", accession="d-acc", cik="D")

    [restated] = _blockholder_restatement_holders([_slice("insiders", [survivor])])

    assert restated.filer_name == "Director"
    assert restated.winning_source == "13d"


def test_a_superseded_amendment_at_a_LARGER_figure_does_not_win() -> None:
    """#3189 finding 14b. ``_dedup_within_source`` ships the superseded originals of an
    amendment chain as ``dropped_sources``, still tagged ``13d``. A filer who reported
    100 on a 13D and then 60 on the 13D/A that replaced it has both entries here; a
    shares-only MAX publishes 100, a figure Rule 13d-2 retired. Within one source tag
    the LATEST supersedes, whatever the direction of the revision."""
    survivor = _holder(
        "Reducer",
        "900",
        source="13f",
        dropped=(
            _dropped("13d", "100", accession="13d-original", as_of=date(2025, 1, 15)),
            _dropped("13d", "60", accession="13d-amendment", as_of=date(2026, 5, 1)),
        ),
    )

    [restated] = _blockholder_restatement_holders([_slice("institutions", [survivor])])

    assert restated.shares == Decimal(60)
    assert restated.winning_accession == "13d-amendment"


def test_supersession_crosses_the_13d_13g_tags_because_upstream_does() -> None:
    """Checkpoint 2 killed the version that partitioned supersession BY SOURCE TAG.
    ``_dedup_within_source`` receives the 13D and 13G rows in one pool and groups them
    on identity + ``ownership_nature``, NOT on source, so a filer who moved from a 13D
    to a 13G has ONE amendment chain upstream and the pie carries the 13G's figure.
    Treating the tags as independent chains republishes the retired 13D."""
    survivor = _holder(
        "Switcher",
        "900",
        dropped=(
            _dropped("13d", "310", accession="13d-original", as_of=date(2026, 1, 15)),
            _dropped("13g", "200", accession="13g-later", as_of=date(2026, 6, 30)),
        ),
    )

    [restated] = _blockholder_restatement_holders([_slice("institutions", [survivor])])

    assert restated.shares == Decimal(200)
    assert restated.winning_accession == "13g-later"


def test_two_filings_of_the_SAME_vintage_take_the_MAX() -> None:
    """Supersession leads, but where two filings share an as-of date neither retires the
    other — they are one block through two lenses, so MAX, never SUM (#1640/#1889). This
    is the regime the shipped #2215 docstring named, kept intact under the new ordering."""
    survivor = _holder(
        "Dual Filer",
        "900",
        dropped=(
            _dropped("13d", "250", accession="13d-acc", as_of=date(2026, 3, 31)),
            _dropped("13g", "310", accession="13g-acc", as_of=date(2026, 3, 31)),
        ),
    )

    [restated] = _blockholder_restatement_holders([_slice("institutions", [survivor])])

    assert restated.shares == Decimal(310)
    assert restated.winning_accession == "13g-acc"


def test_a_foreign_entry_is_restated_under_the_filer_not_the_representative() -> None:
    """#3189 finding 15. A group collapse (#1764 / #1652 / #1645) appends a co-filer's
    row to a representative, and ``DroppedSource`` carried no identity — so the overlay
    stamped the rep's name and CIK on someone else's accession, with a clickable EDGAR
    link naming a third party. 1,861 of 6,917 overlay rows on the dev corpus."""
    rep = _holder(
        "Rep Fund",
        "700",
        cik="REP",
        source="form4",
        dropped=(_dropped("13d", "500", accession="member-acc", filer_cik="MEMBER", filer_name="Co-Filer LLC"),),
    )

    [restated] = _blockholder_restatement_holders([_slice("insiders", [rep])])

    assert restated.filer_cik == "MEMBER"
    assert restated.filer_name == "Co-Filer LLC"
    assert restated.winning_accession == "member-acc"


def test_a_foreign_entry_is_NOT_dropped_when_it_is_the_only_13dg_evidence() -> None:
    """Filtering foreign entries out was the first design and checkpoint 1 killed it:
    where a same-accession collapse consumed co-filer B's 13D into representative A and
    A wins on Form 4, B's filing is the ONLY 13D/G evidence on the instrument, so
    filtering re-opens #2215 for that case. This is the same shape as the test above
    and is kept separate because a fail-closed filter that drops everything would pass
    an attribution assertion by vacuity."""
    rep = _holder(
        "Rep Fund",
        "700",
        cik="REP",
        source="form4",
        dropped=(_dropped("13d", "500", accession="member-acc", filer_cik="MEMBER", filer_name="Co-Filer LLC"),),
    )

    assert len(_blockholder_restatement_holders([_slice("insiders", [rep])])) == 1


def test_one_surviving_holder_emits_ONE_row_however_many_filings_it_carries() -> None:
    """Checkpoint 2 killed the version that emitted one row per IDENTITY found. Every
    13D/G filing hanging off one surviving holder describes ONE block — that is why the
    collapses folded them onto that holder, each member being deemed to own the whole
    group's securities (Rule 13d-5(b)(1) / 16a-1(a)(2)). Per-identity emission renders a
    two-member 10M group as two 10M rows and a 20M slice total against a pie carrying one
    10M block."""
    rep = _holder(
        "Rep Fund",
        "700",
        cik="REP",
        source="form4",
        dropped=(
            _dropped(
                "13d", "1000", accession="rep-acc", as_of=date(2026, 1, 1), filer_cik="REP", filer_name="Rep Fund"
            ),
            _dropped(
                "13d",
                "1000",
                accession="member-acc",
                as_of=date(2026, 6, 30),
                filer_cik="MEMBER",
                filer_name="Co-Filer LLC",
            ),
        ),
    )

    restated = _blockholder_restatement_holders([_slice("insiders", [rep])])

    assert [h.filer_name for h in restated] == ["Co-Filer LLC"]
    assert restated[0].shares == Decimal(1000)


def test_one_block_copied_onto_two_representatives_emits_ONE_row() -> None:
    """Identity is the unit of account (Rule 13d-3 / I14), so the result is de-duplicated
    GLOBALLY across slices. Without it, a member's filing appended to two reps by two
    passes would render twice at the same figure and the overlay slice's own
    ``total_shares`` would read 2x one block."""
    rep_a = _holder(
        "Rep A",
        "700",
        cik="A",
        source="form4",
        dropped=(_dropped("13d", "500", accession="member-acc", filer_cik="MEMBER", filer_name="Co-Filer LLC"),),
    )
    rep_b = _holder(
        "Rep B",
        "800",
        cik="B",
        source="13f",
        dropped=(_dropped("13d", "500", accession="member-acc", filer_cik="MEMBER", filer_name="Co-Filer LLC"),),
    )

    restated = _blockholder_restatement_holders([_slice("insiders", [rep_a]), _slice("institutions", [rep_b])])

    assert [h.filer_name for h in restated] == ["Co-Filer LLC"]


def test_an_equal_share_13d_13g_tie_is_broken_the_same_way_in_any_order() -> None:
    """#3189 finding 16. ``max(dropped, key=shares)`` returns the FIRST maximal entry and
    the upstream order derives from ``set(by_source)`` iteration, which ``str`` hash
    salting makes process-dependent. Both permutations must publish the same filing."""
    entries = (
        _dropped("13d", "250", accession="acc-13d", as_of=date(2026, 1, 1)),
        _dropped("13g", "250", accession="acc-13g", as_of=date(2026, 1, 1)),
    )
    forward = _blockholder_restatement_holders([_slice("institutions", [_holder("Tied", "900", dropped=entries)])])
    reverse = _blockholder_restatement_holders(
        [_slice("institutions", [_holder("Tied", "900", dropped=tuple(reversed(entries)))])]
    )

    assert forward[0].winning_source == reverse[0].winning_source
    assert forward[0].winning_accession == reverse[0].winning_accession


def test_a_null_as_of_date_sorts_LAST_not_first() -> None:
    """NULL ordering is the classic inversion. A dated amendment must supersede an
    undated entry of the same channel, not the other way round."""
    survivor = _holder(
        "Undated",
        "900",
        dropped=(
            _dropped("13d", "100", accession="no-date", as_of=None),
            _dropped("13d", "60", accession="dated", as_of=date(2026, 5, 1)),
        ),
    )

    [restated] = _blockholder_restatement_holders([_slice("institutions", [survivor])])

    assert restated.winning_accession == "dated"


def test_a_total_tie_on_shares_date_and_accession_is_broken_by_SOURCE() -> None:
    """The trailing ``source`` key is what makes the ordering TOTAL. ``accession_number``
    is ``str(row["source_accession"] or "")`` upstream, so empty accessions occur; two
    entries can then tie on shares, date and accession and differ only in tag, and
    ``13d`` / ``13g`` share ``_PRIORITY_RANK`` 3 deliberately, so rank cannot separate
    them. Without this key the winner would depend on iteration order again."""
    entries = (
        _dropped("13d", "250", accession="", as_of=None),
        _dropped("13g", "250", accession="", as_of=None),
    )
    forward = _blockholder_restatement_holders([_slice("institutions", [_holder("Blank", "900", dropped=entries)])])
    reverse = _blockholder_restatement_holders(
        [_slice("institutions", [_holder("Blank", "900", dropped=tuple(reversed(entries)))])]
    )

    assert forward[0].winning_source == reverse[0].winning_source


def test_argmax_source_is_deterministic_across_a_permuted_source_set() -> None:
    """#3189 finding 16, one level up. ``_argmax_source`` runs ``max`` over a list built
    from ``set(by_source)``; with ``13d`` and ``13g`` tied on both subtotal and
    ``_PRIORITY_RANK``, which one becomes ``figure_src`` — and hence which filings even
    become ``dropped_sources`` for the overlay to select from — was process-dependent.
    A deterministic final sort inside the overlay cannot repair a candidate set that
    changed upstream."""
    totals: dict[SourceTag, Decimal] = {"13d": Decimal(100), "13g": Decimal(100)}

    assert _argmax_source(["13d", "13g"], totals) == _argmax_source(["13g", "13d"], totals)
