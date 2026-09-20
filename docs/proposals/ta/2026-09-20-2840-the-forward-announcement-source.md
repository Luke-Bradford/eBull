# #2840 — the forward announcement source: REFUSED, and the prerequisite it rests on is not one

Status: **refused at checkpoint 1 (61 findings), and the refusal holds.** No parser, no probe and
no measurement shipped — deliberately, see §5. What ships is the premise correction, the source
rules that were verified on the way to it, and one measurement that makes the corrected route
concrete.

## 0. What I set out to do, and the single finding that ended it

`bar_capture_certificate.py:416` downgrades every certifying verdict while
`PROVIDER_REWRITE_TIMING_VERIFIED` is `False` (`:287`), so **no bar can be certified until that
flag moves** — that part I re-derived and it stands. What moves it, per the constant's own
comment, is *"a bar CAPTURED between an effective instant and the following open"*.

Two prior close-outs (`9246f77c`, `b9860cf2`) then recorded the blocker as: **no forward
corporate-action calendar exists**, so the capture cannot be scheduled. I took that at face
value and spent the session building the calendar's first leg — an Item 5.03 effective-date
extractor over the 13,575 stored Item 5.03 8-Ks.

⛔⛔ **It is not a prerequisite, and nobody had ever derived that it was.** Checkpoint 1,
finding 58:

> Advance knowledge of the effective instant is not logically necessary. Continuous capture, or
> discovery after effectiveness but before open, can still obtain the required observation.

That is correct and it inverts the ticket. The flag needs **one bar that happens to sit in the
window**, not a bar captured *because* we knew the window was coming. A capture that runs
pre-open across a panel produces that bar for whatever re-denominates; the split is then
identified **retrospectively**, which is exactly what `308d1e38`'s register already does from
filings alone. The forward calendar only makes the capture *cheaper* (targeted rather than
broad). It was never the gate.

### 0.1 And the corrected route is already live here, not hypothetical

Measured read-only on the dev DB, 2026-09-20:

```sql
SELECT count(*), count(DISTINCT instrument_id)
  FROM strategy_intraday_bars
 WHERE (captured_at AT TIME ZONE 'America/New_York')::time < time '09:30';
-- 10,127 bars, 8 instruments
```

Capture-hour histogram (America/New_York): hour 7 carries **3,045** bars, hour 9 **9,519**,
then 2,500–2,900 per hour to 15, and 442 at 16.

⇒ pre-open capture **already happens on this box**. The gap is not knowing *when* to capture; it
is **breadth** — 8 instruments is a panel, and the chance that one of those eight re-denominates
in any given window is small. That is a capture-breadth question with a cost, not a
source-discovery question with a blocker. ⚠ The 09:30 boundary is a filter, not a claim about
the flag: a bar *captured* pre-open is a candidate observation, and whether it satisfies the
certificate additionally depends on the bar's own timestamp and the event's effective instant,
which this query does not test.

## 1. What was verified about the source rules, and what that changed

Worth keeping even though the build is refused — each was fetched, not recalled.

| claim | verdict |
| --- | --- |
| **17 CFR 240.10b-17(b)(1)** requires notice *"no later than 10 days prior to the record date"* for *"a stock split or reverse split"* | **Confirmed** (law.cornell.edu, fetched 2026-09-20). ⚠ Notice goes to FINRA (the rule still says NASD) or by comparable exchange notice — **not to EDGAR** — and it is notice of the **record date**, one clock removed from the effective instant. ⚠ Checkpoint 1 finding 10: it also carries exemptions and fund carve-outs, and guarantees neither issuer compliance nor a public FINRA feed. |
| **Form 8-K Item 5.03(1)(i)** requires *"the effective date of the amendment"* | **Confirmed verbatim** (`sec.gov/files/form8-k.pdf`, fetched 2026-09-20). |
| Item 5.03's trigger clause (*"a proposal … was not disclosed in a proxy statement"*) **excludes** the proxy-approved population | ⛔ **FALSIFIED by a real filing** (finding 6). MetaVia reports a **proxy-approved** reverse split under Item 5.03, with advance effectiveness — `sec.gov/Archives/edgar/data/1638287/000110465925117540/mtva-20251202x8k.htm`. Absence of a mandatory trigger does not imply absence from the item. |
| Item 3.03 is past-tense and therefore never forward notice | ⛔ **FALSIFIED by a real filing** (finding 7). Workhorse announces **17 March** effectiveness in an Item 3.03 filed **12 March** — `sec.gov/Archives/edgar/data/1425287/000121390025022950/ea0234041-8k_work.htm`. |
| A reverse split requires a charter amendment, and a large one requires a vote | ⛔ **Uncited generalisation** (finding 8). Nevada provides a certificate-of-change route with its own voting conditions (NRS 78.207–78.209). |
| Mandated disclosure implies advance disclosure | ⛔ **No** (finding 11). General Instruction B.1 ordinarily allows **four business days after** the event. |

⇒ **The whole "which 8-K item carries forward notice" line cannot be settled from the form text,
and every structural claim I made from it was wrong in the permissive direction.** Two of the six
rows above were refuted by a single counterexample filing each — which is the tell that the
reasoning was deductive where it needed to be empirical.

## 2. The structured-field check, and why its zero would have been worthless

`filing_events.items` is a real array populated on **405,810 of 406,628** stored 8-Ks (99.8%), so
the ITEM is structured and needs no text handling: `items @> ARRAY['5.03']` gives **13,762 rows
over 13,575 accessions**, 3,785 instruments, 2016-06-03 → 2026-09-18.

Inside the document I claimed no structured split field and built a detector to measure it.
Checkpoint 1 broke the detector three ways (findings 15, 46): it matched **any** HTML `name=`
attribute (an anchor `name="effective-date"` counted as a hit), it missed `name = "…"` with
whitespace around the `=`, and its vocabulary omitted **consolidation** — the reverse split's own
other name, which the direction regex three lines away already knew. Finding 15 adds that SEC
staff guidance requires tagging **all** cover-page information, not only identifiers, so two
substrings could never have established the absence.

**A check that cannot see its target reports the same zero as a check that looked.** That is the
reusable half and it is now in the prevention log.

## 3. Why the parser could not have carried a population claim either

Not shipped, so this is a record of what the refusal caught rather than a defect list on live
code. The ones that would have produced confident wrong numbers:

- **The split and the date were never associated** (finding 17). Any split mention anywhere in a
  document combined with the first bound effective date anywhere — including one belonging to an
  employment agreement or a different security.
- **Forward splits, the archetypal case, fell out** (finding 23): *"announced a 2-for-1 stock
  split, effective October 3, 2026"* returned nothing, because the vocabulary excluded a bare
  "stock split" to avoid historical recitals. And the complement (finding 24): *"effected a
  1-for-10 stock split"* was labelled **forward** on a reverse ratio.
- **My ordering rationale was false** (finding 21). I wrote that a forward-first test would
  label the entire reverse population "forward" because every reverse document contains "split".
  It would not — the forward pattern never matched a bare `reverse stock split`, and
  `share consolidation` contains no "split" at all. The ordering may still be right; the stated
  reason was not, and a test's docstring asserting it would have taught the next reader a false
  rule.
- **Conditional effectiveness read as definite** (finding 27): *"not effective before"*,
  *"effective no later than"* and abandoned proposals were indistinguishable from a fixed date.
- **`effective\b` matches `ineffective`** (finding 26), reproduced: *"reverse stock split was
  ineffective October 3, 2026"* yielded that date.
- **My anti-circularity guard did not establish what I claimed** (finding 1). Withholding
  `filing_date` from the classifier's signature prevents one mechanism; it does not make
  admission independent of the lead, because *date-extractable language* can itself correlate
  with advance-versus-retrospective disclosure. The structural argument was weaker than its
  presentation.
- **`lead > 0` was the wrong criterion anyway** (finding 2). Same-day notice can precede both
  effectiveness and the session open, so a strictly-positive filter names a subset of forward
  notice, not the whole of it.

Two defects I introduced and Codex reproduced, kept here because both are transferable:

- **A borrowed regex bound carried its corpus assumption.** `[^.]{0,120}` came from
  `dividend_calendar`, where no clock time sits between the label and the date. Split text puts
  one there constantly, so *"effective at 12:01 a.m. Eastern Time on October 3, 2026"* — the
  delayed-effectiveness shape the rule existed for — read as having no date.
- **And the fix for that broke the thing the bound protected.** Stripping abbreviation periods
  also removes the period that ENDS a sentence, so *"became effective at 5 p.m. Results are due
  October 3, 2026"* confidently returned 3 October. The two guards were fighting, and the loose
  window let the loser through silently. ⚠ It is irreducible: in *"…5 p.m. On October 3, 2026,
  the Company will report"* the period is a sentence end and in *"…12:01 a.m. Eastern Time on
  October 3"* it is not, and no local rule separates them.

## 4. What was also wrong in the framing, briefly

- **"EDGAR is not it" does not follow from "Item 5.03 is not it"** (finding 5), and the same
  paragraph that said so also said it did not. Self-contradicting, and it would have selected
  FINRA as the next source on no evidence.
- **The eToro negative was overstated** (finding 16). `https://api-portal.etoro.com/llms.txt`
  (fetched 2026-09-20) lists no corporate-action, split or events-calendar slug in any of its
  sections. That establishes **no documented endpoint**, not that none exists — the skill's own
  protocol asks for the per-endpoint check before an existence claim, and an index is not that.
  Stated in the corrected form in `.claude/skills/data-sources/etoro-api.md`.
- **`filing_date` is not availability time** (finding 3). Regulation S-T Rule 13 governs the
  filing-date/acceptance relationship, and the probe treated a date column as the moment the
  notice became readable.

## 5. Why nothing was shipped rather than shipped-with-caveats

The tempting close was to land the probe with §3 as a limitations section. Refused for the
reason this repo keeps recording: **a number becomes the next session's premise.** A register
that misses forward splits, mislabels reverse ratios and binds unassociated dates would have
produced a lead distribution, and the distribution would have been quoted — the same way
`b9860cf2` had to withdraw a "5,791 rows detached" figure re-quoted from an earlier close-out of
my own. An instrument that cannot carry the claim is worse than no instrument, because it reads
as coverage.

⚠ The 4,750 documents fetched before the run was stopped are in `/tmp` and are not part of this
change. The fetch was killed on the finding, not left to finish — continuing to crawl SEC for a
measurement that will not be used is the opposite of the skill's *"download only what you need"*.

## 6. Next, with the blocker removed rather than restated

- 🎯 **The capture-breadth question, which is now the real one.** Pre-open capture already runs
  (§0.1). What is missing is breadth: 8 instruments. The next ticket sizes the panel needed for a
  reasonable chance of catching a re-denomination in-window, against the intraday rate budget —
  and joins whatever it catches to `308d1e38`'s retrospective register. **No forward calendar,
  no new source, no operator gate.**
- ⚠ **Do not re-open "which 8-K item carries forward notice" from the form text.** §1 shows two
  structural readings falsified by one counterexample filing each. If that question ever matters
  again it is an empirical one over filings, and it is not on the critical path.
- ⚠ **The two obligations `b9860cf2` recorded open (§8) are unchanged and remain open** — the
  carrier's source selection sits outside every identity hash, and a certified carrier is bound
  to its series only by length and rule version.

Refs #2840. Refs #2437.
