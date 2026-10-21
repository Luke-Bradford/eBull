"""#3899 — the SEC edge's injected bot-manager script is stripped at both raw body boundaries.

The fixture tail is verbatim from a 2026-10-20 re-fetch of
``0000006955-17-000047`` (``atuproxyfy2017-amendment1.htm``): the stored body
ends ``…46</font></div></div>\\t</body>…`` and the re-fetch carries the tag in
between. Without the strip the re-fetch's sha256 never equals the stored one.
"""

from __future__ import annotations

import hashlib

import httpx
import pytest

from app.providers.implementations.sec_edgar import SecFilingsProvider, strip_sec_edge_script
from app.providers.resilient_client import ResilientClient

_FILED = '<div><font style="font-size:10pt;">46</font></div></div>\t</body>\n</html>\n'
_INJECTED = (
    '<div><font style="font-size:10pt;">46</font></div></div>\t'
    '<script type="text/javascript"  src="/-QOA/64Vm/ig/zTjS/gt8A/m1aEQkGLcNmQ4Q/XiUsWQ/Yl0/HHTAfbn4"></script>'
    "</body>\n</html>\n"
)


def test_strip_restores_the_filed_bytes() -> None:
    assert strip_sec_edge_script(_INJECTED) == _FILED
    assert hashlib.sha256(strip_sec_edge_script(_INJECTED).encode()).hexdigest() == (
        hashlib.sha256(_FILED.encode()).hexdigest()
    )


def test_strip_is_a_no_op_on_a_clean_body_and_idempotent() -> None:
    assert strip_sec_edge_script(_FILED) == _FILED
    assert strip_sec_edge_script(strip_sec_edge_script(_INJECTED)) == _FILED


def test_an_uppercase_body_tag_strips_the_same() -> None:
    # 110 of the 253 stored cases close with </BODY>.
    assert strip_sec_edge_script(_INJECTED.replace("</body>", "</BODY>")) == _FILED.replace("</body>", "</BODY>")


def test_a_rotated_token_strips_the_same() -> None:
    rotated = _INJECTED.replace(
        "/-QOA/64Vm/ig/zTjS/gt8A/m1aEQkGLcNmQ4Q/XiUsWQ/Yl0/HHTAfbn4", "/N3AEOPt9/dJoipWz/AQsW1I"
    )
    assert strip_sec_edge_script(rotated) == _FILED


@pytest.mark.parametrize(
    "kept",
    [
        # Each shape sits where the edge puts its tag, so only the shape decides.
        '<script type="text/javascript"  src="https://example.com/x.js"></script></body>',
        '<script type="text/javascript"  src="//example.com/asset.js"></script></body>',
        '<script type="text/javascript"  src="/asset.js?v=1"></script></body>',
        '<script type="text/javascript"  src="/a">alert(1)</script></body>',
        # SEC's own XBRL viewer script inside a .txt submission (R pages).
        '<script type="text/javascript" src="Show.js">/* Do Not Remove This Comment */</script></body>',
        # The edge's shape anywhere but immediately before </body> is not the edge's tag.
        '<p><script type="text/javascript"  src="/a"></script></p></body>',
        "<p>The script of the meeting is below.</p></body>",
    ],
)
def test_strip_leaves_other_shapes_alone(kept: str) -> None:
    assert strip_sec_edge_script(kept) == kept


def test_fetch_document_text_returns_the_stripped_body() -> None:
    provider = SecFilingsProvider(user_agent="test test@example.com")
    provider._tickers_client = httpx.Client(  # noqa: SLF001
        headers={"User-Agent": "test test@example.com"},
        transport=httpx.MockTransport(lambda r: httpx.Response(200, text=_INJECTED)),
    )
    provider._http_tickers = ResilientClient(provider._tickers_client, min_request_interval_s=0.0)  # noqa: SLF001
    assert provider.fetch_document_text("https://www.sec.gov/Archives/edgar/data/6955/x.htm") == _FILED


def test_prefetch_document_texts_returns_the_stripped_body(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.services.sec_pipelined_fetcher import prefetch_document_texts

    transport = httpx.MockTransport(lambda r: httpx.Response(200, text=_INJECTED))
    original_init = httpx.AsyncClient.__init__

    def patched_init(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        kwargs["transport"] = transport
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "__init__", patched_init)
    url = "https://www.sec.gov/Archives/edgar/data/6955/x.htm"
    assert prefetch_document_texts([url], user_agent="test/1.0") == {url: _FILED}


def test_every_known_token_is_stripped() -> None:
    from scripts.repair_3899_sec_edge_script import _KNOWN_TOKENS, _with_token

    for token in _KNOWN_TOKENS:
        injected = _with_token(_FILED, token)
        assert injected is not None and token in injected
        assert strip_sec_edge_script(injected) == _FILED


def test_swept_hash_classification() -> None:
    """The ``--swept`` repair re-records a hash only when the recorded one is reproduced exactly."""
    from scripts.repair_3899_sec_edge_script import _KNOWN_TOKENS, _sha, _with_token, classify_swept

    injected = _with_token(_FILED, _KNOWN_TOKENS[-1])
    assert injected == _INJECTED
    assert classify_swept(_sha(_INJECTED), _FILED) == "reconstructed"
    assert classify_swept(_sha(_FILED), _FILED) == "canonical"
    assert classify_swept(_sha(_INJECTED.replace("HHTAfbn4", "unknown")), _FILED) == "unverifiable"
    assert classify_swept(_sha(_FILED), None) == "gone"
    assert _with_token("plain text, no body tag", _KNOWN_TOKENS[0]) is None
    # Reconstruction inserts exactly where the strip removes, in either case.
    for body in (_FILED, _FILED.replace("</body>", "</BODY>")):
        for token in _KNOWN_TOKENS:
            injected = _with_token(body, token)
            assert injected is not None and strip_sec_edge_script(injected) == body
