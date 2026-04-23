# test-quality

Engineering standard for writing tests that prove something. A test that doesn't assert observable behaviour is noise, not coverage.

## A test must assert on a specific value

```python
# Noise — proves nothing except no crash
def test_generate_thesis():
    result = generate_thesis(...)
    assert result is not None

# Coverage — proves the contract
def test_generate_thesis_returns_correct_version():
    result = generate_thesis(...)
    assert result.thesis_version == 1
    assert result.stance == "buy"
    assert result.confidence == pytest.approx(0.8)
```

## Mandatory boundary cases

For every function, identify and test:
- **First row / empty table** — does an INSERT using `MAX()` work when there are no prior rows?
- **Zero results** — does a query returning a list handle the empty case without raising?
- **None / null fields** — does optional data come back as `None`, not raise `AttributeError`?
- **Failure path** — does a best-effort operation (API call, critic scoring) fail gracefully without blocking the happy path?

These aren't edge cases. They're the first things a reviewer checks.

## Semantic boundary checks

For any rule about affordability, capacity, or limits, include tests for the actual business boundary:
- zero
- exact cap
- just below cap
- just above cap

Do not stop at proving branch execution.
Prove the rule matches its intended meaning.

## Mock discipline

**Match what the real library returns.** psycopg `fetchone()` returns `None` on exhaustion — not a `MagicMock`. A mock that returns `MagicMock` instead of `None` will never trigger the None-check branch.

**Use `spec=` on MagicMock** so accessing an unexpected attribute raises `AttributeError` rather than silently returning another mock:
```python
mock_conn = MagicMock(spec=psycopg.Connection)
```

**Patch at the point of use**, not the point of definition:
```python
# Wrong — patches the original, not where it's imported
patch("datetime.datetime.now")

# Correct — patches the name as used in the module under test
patch("app.services.thesis._utcnow")
```

**SQL text-dispatch mocks** that match on substrings must document branch priority. INSERT branches must come before SELECT branches because a scalar subquery inside VALUES contains `SELECT ... FROM table` as a substring. Add a comment noting what structural SQL defects this matching approach cannot catch.

## Time-dependent code

Any function calling `_utcnow()` — directly or transitively — must have it patched in tests. If unsure whether a function calls it transitively, read the call chain. An unpatched `_utcnow()` makes the test non-deterministic.

## Free-text comparisons

Any test comparing a rationale or explanation string must derive the expected value from the same helper used in production — never a hardcoded literal:

```python
# Wrong — breaks silently when production format changes
assert rec.rationale == "No action trigger met; score=0.600 rank=2"

# Correct — format change propagates automatically
assert rec.rationale == _hold_rationale(score_row, quote_is_fallback=False)
```

## DB write + return value consistency

If a function both writes to the DB and returns a result object, there must be a test verifying the returned object matches what was written. Silent divergence between in-memory and persisted state is a real bug class.

## Integration-marker discipline

Any test that uses the `clean_client` fixture (or any fixture that touches a real DB) MUST be decorated with `@pytest.mark.integration`. Unit-only CI passes deselect integration tests by marker; an unmarked integration test will either be silently skipped or error during fixture setup.

```python
# Wrong — silently runs against whichever DB mode CI picked
def test_post_ingest_enabled_unknown_key_404(clean_client: TestClient) -> None:
    ...

# Correct
@pytest.mark.integration
def test_post_ingest_enabled_unknown_key_404(clean_client: TestClient) -> None:
    ...
```

Self-check before pushing: `grep -n "def test_.*\(clean_client" tests/` and assert each match is preceded by `@pytest.mark.integration`.

## Test naming

Method names describe the scenario and expected outcome:
- `test_first_thesis_gets_version_1` ✓
- `test_insert` ✗

No test longer than ~20 lines. If it's longer, the function under test probably does too much, or the test is testing too many things at once.
