# R6 tax-wrapper verdict (#2915)

**Verdict: no supported public-API route into eToro's Stocks & Shares ISA is
established as of 2026-08-23.** eBull must therefore treat the ISA as
unreachable and must not deploy an automated pot there today. This is a
support and deployment verdict, not proof that no private, partner-only or
undocumented route exists.

R6 keeps the ordinary eToro trading account as its prospective execution
venue. Any eventual sleeve must still pass the remaining programme gates
before it can hold money.

The live eToro portal was verified on 2026-08-23. The last OpenAPI version
recorded in this repository was v1.342.0 on 2026-08-14; the rendered portal no
longer exposed a version string, so this memo does not claim that v1.342.0 was
still current.

## API reachability

The public API documents a **balance read** for Moneyfarm-labelled accounts:
`GET /api/v1/balances/{accountType}` accepts `moneyfarm`, alongside `trading`,
`cash`, `options`, `crypto`, and `spaceship`. The response does not identify a
balance as an ISA. A balance label is not an order route.

On the dated public-documentation snapshot:

- the live API index contained no `ISA` or Moneyfarm execution endpoint;
- `POST /api/v2/trading/execution/orders` exposed no account identifier,
  account type or sub-account selector; and
- the separate sub-account API documented only provider `etoro-trading`.

The ISA is powered by Moneyfarm, and eToro's setup guide directs customers to
the Moneyfarm partner platform. These facts establish that the documented
eToro trading writer is not a supported ISA writer. Absence from a public
index cannot disprove a private or undocumented interface.

The configured eBull credential inventory at verification time was:

```text
[('etoro', 'demo', 'api_key', 'valid', 1),
 ('etoro', 'demo', 'user_key', 'valid', 1)]
```

There were zero active real credentials. Reproduce that derived fact without
printing secrets:

```sql
SELECT provider, environment, label, health_state, count(*)
FROM broker_credentials
WHERE revoked_at IS NULL
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;
```

A read-only probe with the configured demo credentials to
`GET /api/v1/balances/moneyfarm?includeZeroBalances=true&expand=equityDetails`
returned `403 InsufficientPermissions`. That proves only that these demo
credentials cannot read the endpoint; it is not evidence about execution.

The deployment gate can be reversed only when current eToro or Moneyfarm
documentation/support identifies the ISA execution contract, eBull can prove
the account identity with appropriately scoped credentials, and the writer can
be validated through the repository's sandbox and execution guards. A future
solution might be an evolution of the existing API or a new integration; this
memo does not prejudge which.

## Eligible instruments

The public facts establish advertised categories, not an instrument-level
eligibility list:

- eToro advertises thousands of UK, US and European stocks, ETFs, bonds and
  mutual funds in the DIY ISA; this does not establish that every security in
  those regions or categories is eligible;
- eToro's ISA guide says CFDs are not ISA-eligible and the account cannot be
  used to short; and
- non-GBP instruments incur a 0.70% conversion fee on each buy and each sell.

No public source or API field found in this investigation exposed the exact
ISA catalogue. Investment selection occurs on the separately administered
Moneyfarm platform. The overlap with eBull's ordinary-account universe is
therefore unknown and must not be inferred.

Because API execution fails first, #2915 does not narrow the research universe
for later arms. R6 retains the repository's full, survivorship-free US research
population and must prove ordinary-account broker eligibility at execution
time. It must not claim ISA deployability.

## Tax and FX sensitivity in pounds

The ticket's phrase “a certain 18–24% on realised gains” is too broad. For UK
individuals in 2026/27, net gains above the £3,000 annual exempt amount can be
split between 18% and 24% depending on taxable income. Dividends first use any
unused Personal Allowance, then a £500 dividend allowance, and can be split
between 10.75%, 35.75% and 39.35% bands. ISA gains and dividends are not
charged UK Capital Gains Tax or dividend tax.

The reproducible table is deliberately a sensitivity, not a personal tax
estimate or return forecast. It freezes these assumptions:

- £50,000 is foreign-asset **purchase consideration**, requiring £50,350 cash
  after the initial 0.70% FX charge. It represents prior-year accumulation or
  a compliant ISA transfer, not a one-year new subscription;
- 100% of holdings are non-GBP, and each rebalanced pound is sold and replaced,
  so both the 0.70% sell and 0.70% buy FX charges apply;
- one-way turnover is sold notional divided by mean equity: 25% means £12,500
  sold and £12,500 bought;
- every sale has a positive gain equal to 20% of sold proceeds, with no losses;
- the cash dividend yield is 2%;
- the Personal Allowance is fully consumed by other income, while the CGT and
  dividend allowances are wholly unused elsewhere; and
- “all lower rate” and “all higher rate” put every taxable pound at the named
  rate. They are the two prescribed scenarios, not exhaustive bounds or labels
  for an actual taxpayer; the additional-rate dividend scenario is not shown.

The comparator is also deliberately narrow. eBull's settled ordinary-account
lane is long/x1/real, with USD orders, a USD demo account and a USD universe,
so its trading FX is structurally zero. The difference below is incremental to
that lane only; it is not a universal GIA-versus-ISA comparison.

Run:

```bash
uv run python -m scripts.report_2915_isa_tax_wrapper
```

| rate scenario / turnover | assumed realised gain | hypothetical GIA CGT | hypothetical GIA dividend tax | ISA initial FX | ISA rebalance FX | tax minus ISA FX, first year | tax minus ISA FX, no initial purchase |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| all lower rate / 25.00% | £2,500.00 | £0.00 | £53.75 | £350.00 | £175.00 | **−£471.25** | **−£121.25** |
| all higher rate / 25.00% | £2,500.00 | £0.00 | £178.75 | £350.00 | £175.00 | **−£346.25** | **+£3.75** |
| all lower rate / 100% | £10,000.00 | £1,260.00 | £53.75 | £350.00 | £700.00 | **+£263.75** | **+£613.75** |
| all higher rate / 100% | £10,000.00 | £1,680.00 | £178.75 | £350.00 | £700.00 | **+£808.75** | **+£1,158.75** |

Positive means the hypothetical GIA tax exceeds only the specified ISA FX;
negative means the specified ISA FX is larger. Neither is a personal net
benefit. “No initial purchase” is the same one-period sensitivity with the
initial purchase charge excluded, not a later-year forecast.

The ISA has 0% dealing commission and no annual custody fee from 29 July 2026.
The £20,000 annual allowance limits new subscriptions, while compliant ISA
transfers do not consume that allowance.

The sensitivity omits funding FX to an existing account, spreads, market
impact, other fees, foreign withholding and tax credits, terminal conversion,
actual share matching, carried losses, other income and disposals. It therefore
is not net return. The existing `app.services.tax_ledger` supports disposal
matching and CGT calculations, but dividend and fee ingestion is incomplete;
an eventual live sleeve needs that gap closed or a separate validated tax
calculation before reporting realised GIA tax.

## Programme consequence

1. Do not narrow Tier 2 to an assumed ISA catalogue and do not claim ISA
   deployability.
2. Keep turnover as a first-order gate. In this all-foreign ISA sensitivity it
   creates an exact 1.40% round-trip FX charge on replacement notional; in the
   assumed GIA construction it also controls the pace of gain realisation.
3. Keep the ordinary eToro account explicit as the prospective R6 execution
   venue until the ISA gate's reversal conditions are met.
4. If the gate is later reversed, perform an authenticated instrument-level
   catalogue census and execution proof before changing any arm's universe.

## Primary sources

- [HMRC: ISA rules and eligible asset classes](https://www.gov.uk/individual-savings-accounts/how-isas-work)
- [HMRC: 2026/27 Capital Gains Tax rates and £3,000 exemption](https://www.gov.uk/capital-gains-tax/rates)
- [HMRC: 2026/27 dividend allowance and rates](https://www.gov.uk/tax-on-dividends)
- [eToro: DIY Stocks & Shares ISA](https://www.etoro.com/investing/isa/stocks-shares/)
- [eToro: guide to its Stocks & Shares ISA](https://www.etoro.com/investing/what-is-a-stocks-and-shares-isa/)
- [eToro: ISA fee removal, FX charge and Moneyfarm partnership](https://www.etoro.com/news-and-analysis/press-releases/etoro-removes-fees-from-stocks-shares-isa-launches-market-leading-cash-isa-rate/)
- [eToro live API index](https://api-portal.etoro.com/llms.txt)
- [eToro API: balances by account type](https://api-portal.etoro.com/api-reference/balances/get-balances-by-account-type)
- [eToro API: real order creation](https://api-portal.etoro.com/api-reference/trading--real/create-an-order)
- [eToro API: supported sub-accounts](https://api-portal.etoro.com/api-reference/sub-accounts/get-my-sub-accounts)
- [Moneyfarm terms: 0.70% on each non-GBP buy and sell](https://mfm-prod-site-assets.moneyfarm.com/uk/2024/02/21145138/Terms-of-Business-Execution-Only-Proposition-13.02.2024-Bonds-3.pdf)
