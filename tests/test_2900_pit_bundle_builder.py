from __future__ import annotations

from scripts.build_2900_pit_bundle import build_bundle

FORMATION = "2024-06-28T16:00:00"


def test_builder_merges_only_unambiguous_dated_common_equities() -> None:
    identity = {
        "records": [
            {
                "accepted_at": "2024-02-01T12:00:00",
                "cik": "0000000001",
                "exact_formation_session_bar": True,
                "exchange": "NYSE",
                "normalized_price_symbol": "AAA",
                "price_filename_match": True,
                "security_title": "Common Stock",
            },
            {
                "accepted_at": "2024-02-01T12:00:00",
                "cik": "0000000002",
                "exact_formation_session_bar": True,
                "exchange": "NASDAQ",
                "normalized_price_symbol": "AAAW",
                "price_filename_match": True,
                "security_title": "Warrants to purchase common stock",
            },
        ],
        "duplicate_normalized_symbol_ciks": {},
    }
    cover = {"formation_identity_census": {FORMATION: identity}}
    shares = {
        "formations": {
            FORMATION: {
                "complete_pairs": [
                    {
                        "accepted": "2024-03-01T12:00:00",
                        "cik": "0000000001",
                        "current_shares": "120.0000",
                        "prior_shares": "100.0000",
                    }
                ]
            }
        }
    }
    flags = {
        "formations": {
            FORMATION: {
                "records": [
                    {"cik": "0000000001", "complete_recent_history": True, "red_flag_scores": [0.7]},
                    {"cik": "0000000002", "complete_recent_history": True, "red_flag_scores": []},
                ]
            }
        }
    }

    payload, census = build_bundle(
        cover=cover,
        shares=shares,
        red_flags=flags,
        source_hashes={"cover": "a" * 64, "shares": "b" * 64, "red_flags": "c" * 64},
        code_hashes={"builder": "d" * 64},
    )

    assert [row["symbol"] for row in payload["records"]] == ["AAA"]
    assert payload["records"][0]["current_shares"] == "120.0000"
    assert census[FORMATION]["included_common_equities"] == 1
    assert census[FORMATION]["exclusion_reasons"] == {"excluded_title:warrant": 1}
