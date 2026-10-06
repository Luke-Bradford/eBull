"""#3609 step 2 slice 1: the extended SUB publisher and its stage-B access gate, on fixtures only."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.services.factor_book_ledger import StageBAccessError, recorded_access_id
from app.services.factor_book_reference import step2_sub_quarters
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from scripts import publish_3609_step2_sub as publisher

RUN = "a" * 32


def _started(run_id: str = RUN) -> dict[str, Any]:
    return {"run_id": run_id, "event": "started"}


def _recorded(run_id: str = RUN, access_id: Any = 7) -> dict[str, Any]:
    return {"run_id": run_id, "event": "access_recorded", "access_id": access_id}


def test_the_gate_returns_the_access_id_of_a_started_and_recorded_run() -> None:
    rows = [_started("b" * 32), _started(), _recorded()]
    assert recorded_access_id(rows, RUN, before="sub_published") == 7


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([], "no single leading 'started'"),
        ([_recorded(), _started()], "no single leading 'started'"),
        ([_started(), _started(), _recorded()], "no single leading 'started'"),
        ([_started()], "0 'access_recorded'"),
        ([_started(), _recorded(), _recorded()], "2 'access_recorded'"),
        ([_started(), _recorded(), {"run_id": RUN, "event": "failed"}], "already ended"),
        ([_started(), _recorded(), {"run_id": RUN, "event": "completed"}], "already ended"),
        ([_started(), _recorded(), {"run_id": RUN, "event": "sub_published"}], "already written"),
        ([_started(), _recorded(access_id=True)], "not a positive integer"),
        ([_started(), _recorded(access_id="7")], "not a positive integer"),
        ([_started(), _recorded(access_id=0)], "not a positive integer"),
        ([_started(), _recorded(access_id=None)], "not a positive integer"),
    ],
)
def test_the_gate_refuses_a_run_that_has_not_recorded_its_access(rows: list[dict[str, Any]], message: str) -> None:
    with pytest.raises(StageBAccessError, match=message):
        recorded_access_id(rows, RUN, before="sub_published")


def test_another_runs_rows_do_not_open_the_gate() -> None:
    with pytest.raises(StageBAccessError):
        recorded_access_id([_started("b" * 32), _recorded("b" * 32)], RUN, before="sub_published")


def test_the_quarters_are_contiguous_after_step_1s_pin() -> None:
    from app.services.factor_panel_reference import fsds_sub_quarters

    quarters = step2_sub_quarters()
    assert quarters[0] == "2021q3" and quarters[-1] == "2024q3" and len(quarters) == 13
    assert fsds_sub_quarters()[-1] == "2021q2"
    years = [(int(q[:4]), int(q[-1])) for q in quarters]
    assert all(b == ((a[0] + 1, 1) if a[1] == 4 else (a[0], a[1] + 1)) for a, b in zip(years, years[1:], strict=False))


def _sub_zip(quarter: str) -> bytes:
    year, number = int(quarter[:4]), int(quarter[-1])
    accepted = f"{year}-{3 * number - 2:02d}-15 16:05:00.0"
    text = (
        "adsh\tcik\tname\tsic\tform\taccepted\n"
        f"0000320193-{year % 100:02d}-00000{number}\t320193\tAPPLE INC\t3571\t10-Q\t{accepted}\n"
        f"0000950170-{year % 100:02d}-00000{number}\t789019\tMICROSOFT\t\t10-K\t{accepted}\n"
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("sub.txt", text.encode("latin-1"))
        archive.writestr("num.txt", "unused")
    return output.getvalue()


class _Sec:
    def __init__(self, missing: str | None = None) -> None:
        self.requests: list[str] = []
        self.missing = missing

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        quarter = request.url.path.rsplit("/", 1)[-1].removesuffix(".zip")
        if quarter == self.missing:
            return httpx.Response(404)
        return httpx.Response(200, content=_sub_zip(quarter))


@pytest.fixture
def clean(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publisher, "_clean_head", lambda: "f" * 40)
    monkeypatch.setattr(publisher, "COMMITTED_LEDGER_PATH", Path("/nonexistent/3609-ledger.jsonl"))


def _ledger(tmp_path: Path, *rows: dict[str, Any]) -> Path:
    path = tmp_path / "ledger.jsonl"
    for row in rows:
        append_ledger(path, row)
    return path


def _publish(tmp_path: Path, ledger: Path, sec: _Sec, confirmed: list[tuple[str, int]] | None = None) -> str:
    def confirm(run_id: str, access_id: int) -> None:
        if confirmed is None:
            raise StageBAccessError("no committed access")
        confirmed.append((run_id, access_id))

    with httpx.Client(transport=httpx.MockTransport(sec)) as client:
        return publisher.publish(tmp_path / "sub", RUN, ledger=ledger, client=client, confirm_access=confirm)


@pytest.mark.usefixtures("clean")
def test_publish_writes_every_quarter_the_manifest_and_the_ledger_row(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, _started(), _recorded())
    sec = _Sec()
    confirmed: list[tuple[str, int]] = []
    digest = _publish(tmp_path, ledger, sec, confirmed)

    assert confirmed == [(RUN, 7)]
    assert len(sec.requests) == 13
    assert sec.requests[0].endswith("/financial-statement-data-sets/2021q3.zip")
    out = tmp_path / "sub"
    manifest_bytes = (out / "manifest.json").read_bytes()
    assert hashlib.sha256(manifest_bytes).hexdigest() == digest
    manifest = json.loads(manifest_bytes)
    assert manifest["schema"] == "factor-book-3609-step2-sub-v1"
    assert (manifest["run_id"], manifest["access_id"], manifest["git_sha"]) == (RUN, 7, "f" * 40)
    assert [entry["quarter"] for entry in manifest["fsds_sub"]] == list(step2_sub_quarters())
    first = manifest["fsds_sub"][0]
    assert (first["rows"], first["sic_null"], first["rows_10k_family"], first["rows_10q_family"]) == (2, 1, 1, 1)
    assert first["accepted_outside_quarter"] == 0
    assert hashlib.sha256((out / first["path"]).read_bytes()).hexdigest() == first["sha256"]
    # Only sub.txt is kept; the ZIPs and the staging directory are gone.
    assert sorted(p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file()) == sorted(
        ["manifest.json", *(f"inputs/fsds_sub/{q}.txt" for q in step2_sub_quarters())]
    )
    last = read_ledger(ledger)[-1]
    assert (last["run_id"], last["event"], last["manifest_sha256"]) == (RUN, "sub_published", digest)


@pytest.mark.usefixtures("clean")
@pytest.mark.parametrize("rows", [(), (_started(),), (_started(), _recorded(), {"run_id": RUN, "event": "failed"})])
def test_publish_reads_nothing_before_the_access_is_recorded(tmp_path: Path, rows: tuple[dict[str, Any], ...]) -> None:
    sec = _Sec()
    with pytest.raises(StageBAccessError):
        _publish(tmp_path, _ledger(tmp_path, *rows), sec, [])
    assert sec.requests == [] and not (tmp_path / "sub").exists()


@pytest.mark.usefixtures("clean")
def test_publish_reads_nothing_when_the_access_row_is_not_committed(tmp_path: Path) -> None:
    sec = _Sec()
    ledger = _ledger(tmp_path, _started(), _recorded())
    with pytest.raises(StageBAccessError, match="no committed access"):
        _publish(tmp_path, ledger, sec, None)
    assert sec.requests == [] and not (tmp_path / "sub").exists()
    assert [row["event"] for row in read_ledger(ledger)] == ["started", "access_recorded"]


@pytest.mark.usefixtures("clean")
def test_a_run_publishes_once(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, _started(), _recorded())
    _publish(tmp_path, ledger, _Sec(), [])
    sec = _Sec()
    with httpx.Client(transport=httpx.MockTransport(sec)) as client, pytest.raises(StageBAccessError, match="already"):
        publisher.publish(tmp_path / "again", RUN, ledger=ledger, client=client, confirm_access=lambda r, a: None)
    assert sec.requests == []


@pytest.mark.usefixtures("clean")
def test_a_failed_download_removes_the_directory_and_ends_the_run(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, _started(), _recorded())
    with pytest.raises(httpx.HTTPStatusError):
        _publish(tmp_path, ledger, _Sec(missing="2023q1"), [])
    assert not (tmp_path / "sub").exists()
    rows = read_ledger(ledger)
    assert [row["event"] for row in rows] == ["started", "access_recorded", "failed"]
    assert rows[-1]["step"] == "sub_published" and "404" in rows[-1]["error"]
    # Earlier quarters were read, so the run is over: a retry under the same run id is refused unread.
    sec = _Sec()
    with httpx.Client(transport=httpx.MockTransport(sec)) as client, pytest.raises(StageBAccessError, match="ended"):
        publisher.publish(tmp_path / "retry", RUN, ledger=ledger, client=client, confirm_access=lambda r, a: None)
    assert sec.requests == []


@pytest.mark.usefixtures("clean")
def test_an_existing_directory_is_refused_not_resumed(tmp_path: Path) -> None:
    (tmp_path / "sub").mkdir()
    sec = _Sec()
    with pytest.raises(FileExistsError):
        _publish(tmp_path, _ledger(tmp_path, _started(), _recorded()), sec, [])
    assert sec.requests == [] and (tmp_path / "sub").exists()


def test_a_dirty_checkout_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publisher, "is_dirty", lambda: True)
    with pytest.raises(RuntimeError, match="dirty"):
        publisher._clean_head()
    monkeypatch.setattr(publisher, "is_dirty", lambda: None)
    with pytest.raises(RuntimeError, match="dirty"):
        publisher._clean_head()
