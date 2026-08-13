# Third-Party Notices

eBull bundles or depends on the following open-source software. Each
entry below lists the project, its license, and an upstream link. Where
permitted, we exercise commercial / closed / paywalled distribution
rights granted by these licenses.

For licenses requiring redistribution of the original notice (MIT,
BSD-3-Clause, Apache-2.0), the full license text is reproduced under
[`LICENSES/`](LICENSES/) at the project root and shipped with every
distribution.

## Backend (Python)

### Production

| Package | License | Upstream |
|---|---|---|
| anthropic | MIT | <https://github.com/anthropics/anthropic-sdk-python> |
| fastapi | MIT | <https://github.com/tiangolo/fastapi> |
| uvicorn (with `standard` extras) | BSD-3-Clause | <https://github.com/encode/uvicorn> |
| pydantic | MIT | <https://github.com/pydantic/pydantic> |
| pydantic-settings | MIT | <https://github.com/pydantic/pydantic-settings> |
| psycopg (with `binary` extras) | LGPL-3.0-or-later | <https://github.com/psycopg/psycopg> |
| psycopg-pool | LGPL-3.0-or-later | <https://github.com/psycopg/psycopg> |
| httpx | BSD-3-Clause | <https://github.com/encode/httpx> |
| argon2-cffi | MIT | <https://github.com/hynek/argon2-cffi> |
| cryptography | Apache-2.0 OR BSD-3-Clause | <https://github.com/pyca/cryptography> |
| platformdirs | MIT | <https://github.com/platformdirs/platformdirs> |
| apscheduler | MIT | <https://github.com/agronholm/apscheduler> |
| redis (with `hiredis` extras) | MIT | <https://github.com/redis/redis-py> |
| edgartools | MIT | <https://github.com/dgunning/edgartools> |

**psycopg LGPL note.** psycopg uses LGPL-3.0-or-later, which permits
linking from non-GPL applications (including closed-source ones) so
long as the LGPL'd component itself remains replaceable. Because we
import psycopg as an unmodified PyPI dependency, the standard
"dynamic linking" exemption applies: redistribution of eBull does
not require source disclosure of eBull itself, but does require us
to (a) include the LGPL notice + license text, (b) provide the
psycopg source on request (a link to the upstream repository
satisfies this), and (c) not statically combine our code with a
modified psycopg without complying with §4 of the LGPL. We do
none of (c).

### Dev-only (not redistributed)

These ship in the dev dependency group and run on developer machines /
CI runners only. They do not enter the production runtime artifact.

| Package | License | Upstream |
|---|---|---|
| ruff | MIT | <https://github.com/astral-sh/ruff> |
| pyright | MIT | <https://github.com/microsoft/pyright> |
| pytest | MIT | <https://github.com/pytest-dev/pytest> |
| pytest-asyncio | Apache-2.0 | <https://github.com/pytest-dev/pytest-asyncio> |
| pytest-xdist | MIT | <https://github.com/pytest-dev/pytest-xdist> |
| pytest-testmon | AGPL-3.0 | <https://github.com/tarpas/pytest-testmon> |

**pytest-testmon AGPL note.** AGPL-3.0 obligations attach to
distribution / network-service interaction with the AGPL'd code.
pytest-testmon runs only on developer / CI machines as a test
selection helper; it is never linked into the eBull runtime, never
network-served, and never redistributed as part of eBull. As long as
this remains true, AGPL obligations do not propagate to eBull. If
the project ever ships testmon in a runtime path or as a hosted
service, this notice must be revisited.

## Frontend (Node)

### Production

| Package | License | Upstream |
|---|---|---|
| react | MIT | <https://github.com/facebook/react> |
| react-dom | MIT | <https://github.com/facebook/react> |
| react-router-dom | MIT | <https://github.com/remix-run/react-router> |
| @tanstack/react-query | MIT | <https://github.com/TanStack/query> |
| lightweight-charts | Apache-2.0 | <https://github.com/tradingview/lightweight-charts> |
| recharts | MIT | <https://github.com/recharts/recharts> |

### Dev-only

| Package | License | Upstream |
|---|---|---|
| typescript | Apache-2.0 | <https://github.com/microsoft/TypeScript> |
| vite | MIT | <https://github.com/vitejs/vite> |
| @vitejs/plugin-react | MIT | <https://github.com/vitejs/vite-plugin-react> |
| vitest | MIT | <https://github.com/vitest-dev/vitest> |
| jsdom | MIT | <https://github.com/jsdom/jsdom> |
| @testing-library/react | MIT | <https://github.com/testing-library/react-testing-library> |
| @testing-library/jest-dom | MIT | <https://github.com/testing-library/jest-dom> |
| @testing-library/user-event | MIT | <https://github.com/testing-library/user-event> |
| tailwindcss | MIT | <https://github.com/tailwindlabs/tailwindcss> |
| autoprefixer | MIT | <https://github.com/postcss/autoprefixer> |
| postcss | MIT | <https://github.com/postcss/postcss> |
| @types/* | MIT (DefinitelyTyped) | <https://github.com/DefinitelyTyped/DefinitelyTyped> |

## Public data sources

eBull consumes data from the following sources. None imposes a
commercial-use restriction on derived analyses; all are public and
either US-government works or contractually-clean public APIs.

| Source | Terms | Notes |
|---|---|---|
| SEC EDGAR | US-government public domain (17 USC §105) | 10 req/s fair-use cap; User-Agent header required (set via `EBULL_SEC_USER_AGENT`). |
| FINRA published files | Free public data | Bimonthly short-interest, daily RegSHO short-volume. |
| eToro API | Per eToro Public API Terms | Account / quote / order endpoints. Operator must hold valid eToro credentials. |
| Companies House (UK) | Open Government Licence v3.0 | Filings + company metadata for UK issuers. |

## Generative AI (Anthropic Claude)

eBull invokes the Anthropic API for thesis writing, critic review, and
narrative generation. Anthropic's API ToS govern usage; the SDK
(`anthropic`) is MIT-licensed (see backend table). Generated content
is owned by the operator subject to Anthropic's usage policy.

## How this file is maintained

Update this file in the same PR that adds, removes, or upgrades any
top-level dependency. Direct edits to `pyproject.toml` `dependencies` /
`dependency-groups.dev` and `frontend/package.json`
`dependencies` / `devDependencies` MUST be reflected here. Transitive
dependencies are not enumerated; the lockfiles (`uv.lock` and
`frontend/pnpm-lock.yaml`) are the authoritative complete inventory and
can be audited via `uv export` + `pnpm licenses list` when assembling
a redistributable build.

## License

eBull's own source is currently unlicensed (proprietary). Distribution
or modification of eBull source requires explicit operator consent.
Open-source dependencies above retain their own licenses irrespective
of eBull's status.
