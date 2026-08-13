"""Does the spec's pinned D4 regex reject GENUINE Item 403 header shapes?"""

from app.providers.implementations.sec_def14a import _item403_value_signature

# Genuine Item 403 shapes taken from the repo's own regression fixtures
# (tests/test_sec_def14a_parser.py) and from the #2160 spec's own list of
# shapes it says D4 must ADMIT.
GENUINE = [
    ("bare promoted label row (CYH 0001193125-26-140269)", ("Name", "", "Number", "", "Percent")),
    ("bare 3-col", ("Name of Beneficial Owner", "Shares Beneficially Owned", "Percent")),
    ("spec says admit", ("Name", "Shares (1)", "Percent of Outstanding Shares of Common Stock")),
    ("spec says admit", ("Name", "Number of Common Shares", "Percent of Common Shares")),
    ("spec says admit", ("Name", "Number of Shares", "Approximate Percentage of Outstanding Common")),
    (
        "prescribed",
        ("Name and Address of Beneficial Owner", "Amount and Nature of Beneficial Ownership", "Percent of Class"),
    ),
    ("percent-of-class", ("Name of Beneficial Owner", "Shares", "Percent of Class")),
    ("pct sign col", ("Name of Beneficial Owner", "Shares Beneficially Owned", "%")),
]
JUNK = [
    ("comp payout", ("Named Executive Officer", "Shares at Target", "Final PSU Payout %")),
    ("comp salary", ("Name", "Threshold (Percentage of Base Salary)", "Target (Percentage of Base Salary)")),
    ("comp options", ("Name of Individual or Identity of Group and Position", "Shares Underlying Options")),
    ("comp rsu", ("Beneficial Owner", "Number of RSUs")),
    ("guidelines", ("Position", "Minimum Dollar Value", "Minimum Number of Shares")),
    ("capitalisation", ("", "Authorized for issuance", "Issued and outstanding")),
]

print("=== GENUINE Item 403 shapes — every one MUST pass ===")
for label, h in GENUINE:
    ok = _item403_value_signature(h)
    print(f"  {'PASS' if ok else 'REJECT  <-- OVER-REJECTION':28s} {label:42s} {h}")
print("\n=== junk — every one MUST be rejected ===")
for label, h in JUNK:
    ok = _item403_value_signature(h)
    print(f"  {'ADMIT  <-- LEAK' if ok else 'reject':28s} {label:42s} {h}")
