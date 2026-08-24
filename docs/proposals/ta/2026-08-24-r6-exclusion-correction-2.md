# R6 #2908 preregistration correction 2 — result date serialization

Status: **FROZEN AFTER FINAL-ENCODING FAILURE AND BEFORE ANY RETURN CELL WAS EMITTED OR OBSERVED**

- Original declaration SHA-256: `91ec11351d8851e4b3b89ba51f965b649608916346f2e10d9a7cdede9fd2c62f`
- Correction 1 SHA-256: `cd694a39f392cf438e4331a29b9fe8613048127ee37770295c00650758f376fa`
- Correction 1 commit: `316a8594bfc1f5f370a5180de94ece8a3dcdaa6d`

The correction-1 run reached final JSON encoding, then Python rejected `date` objects in the event audit. Redirected
stdout remained exactly zero bytes. No partial or complete return, cost, haircut, overlap, population, or verdict
cell was emitted or inspected.

The only change is a JSON encoder that converts `datetime.date` event keys to ISO `YYYY-MM-DD` strings and refuses
every other unsupported type. Population, price reads, signals, portfolio arithmetic, turnover, spread, termination
bounds, haircuts and verdict logic are unchanged.

Corrected outcome-runner SHA-256:
`c9d5a717d8b2cf7b0ddf15a7e61543737bbbd09bc449f3d126f11f5da51a7981`.

The next command is the same declared outcome command and must publish the complete matrix without selection.
