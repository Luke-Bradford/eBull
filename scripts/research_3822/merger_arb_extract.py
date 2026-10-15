# ruff: noqa: E501
"""Extract deal-related filings for ALL filers from the pinned SEC submissions.zip.

Output: JSON {cik: {"name":..., "tickers":[...], "filings":[[form, filingDate, accession], ...]}} for CIKs that filed any
of FORMS. Reads recent + every listed history page."""

import json
import sys
import zipfile

FORMS = {
    "SC 14D9",
    "SC 14D9/A",
    "SC 14D9-C",
    "SC TO-T",
    "SC TO-C",
    "PREM14A",
    "DEFM14A",
    "DEFA14A",
    "425",
    "25-NSE",
    "25",
    "15-12B",
    "15-12G",
    "SC 13E3",
    "PREM14C",
    "DEFM14C",
}
path, out = sys.argv[1], sys.argv[2]
res = {}
with zipfile.ZipFile(path) as z:
    names = set(z.namelist())
    for name in sorted(n for n in names if n.startswith("CIK") and "-submissions-" not in n):
        doc = json.loads(z.read(name))
        cik = name[3:13]
        blocks = [doc["filings"]["recent"]]
        for e in doc["filings"].get("files") or []:
            if isinstance(e, dict) and e.get("name") in names:
                blocks.append(json.loads(z.read(e["name"])))
        rows = []
        for b in blocks:
            forms = b.get("form") or []
            for i, f in enumerate(forms):
                if f in FORMS:
                    rows.append([f, b["filingDate"][i], b["accessionNumber"][i]])
        if rows:
            res[cik] = {
                "name": doc.get("name"),
                "tickers": doc.get("tickers") or [],
                "filings": sorted(rows, key=lambda r: r[1]),
            }
json.dump(res, open(out, "w"))
print(len(res), "ciks")
