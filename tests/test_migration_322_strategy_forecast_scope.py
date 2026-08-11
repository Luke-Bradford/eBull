"""Migration 322 safely attributes or invalidates legacy assessment rows."""

from pathlib import Path
from typing import LiteralString, cast

import psycopg
from psycopg import sql

_MIGRATION = Path(__file__).resolve().parents[1] / "sql/322_strategy_forecast_assessment_strategy_scope.sql"


def test_migration_322_handles_populated_legacy_tables(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.execute("SET LOCAL search_path=pg_temp")
    conn.execute(
        "CREATE TEMP TABLE strategy_forecast_calibrations (calibration_id text PRIMARY KEY, model_version text)"
    )
    conn.execute(
        "CREATE TEMP TABLE strategy_signals (signal_id bigint PRIMARY KEY, strategy_id text, strategy_version text)"
    )
    conn.execute(
        """
        CREATE TEMP TABLE strategy_opportunity_forecasts (
            forecast_id bigint PRIMARY KEY, signal_id bigint, forecast_policy_version text,
            setup_version text, exit_policy_version text, calibration_id text, decided_at timestamptz
        )
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE strategy_forecast_assessments (
            assessment_id bigint PRIMARY KEY, policy_id text, forecast_policy_version text,
            model_version text, calibration_id text, setup_version text, exit_policy_version text,
            resolver_version text, input_rule_set_version text, window_start date, window_end date,
            evidence_hash text, total_forecasts integer,
            CONSTRAINT strategy_forecast_assessments_evidence_unique UNIQUE (
                policy_id,forecast_policy_version,model_version,calibration_id,setup_version,
                exit_policy_version,resolver_version,input_rule_set_version,evidence_hash
            )
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_strategy_forecast_assessments_scope ON strategy_forecast_assessments (
            policy_id,forecast_policy_version,model_version,calibration_id,setup_version,
            exit_policy_version,resolver_version,input_rule_set_version,assessment_id DESC
        )
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE strategy_forecast_assessment_current (
            policy_id text, forecast_policy_version text, model_version text, calibration_id text,
            setup_version text, exit_policy_version text, resolver_version text,
            input_rule_set_version text, assessment_id bigint, checked_at timestamptz,
            PRIMARY KEY (
                policy_id,forecast_policy_version,model_version,calibration_id,setup_version,
                exit_policy_version,resolver_version,input_rule_set_version
            )
        )
        """
    )
    conn.execute("INSERT INTO strategy_forecast_calibrations VALUES ('cal','model')")
    conn.execute("INSERT INTO strategy_signals VALUES (1,'S-A','v1'),(2,'S-A','v1'),(3,'S-B','v1')")
    conn.execute(
        """
        INSERT INTO strategy_opportunity_forecasts VALUES
          (1,1,'forecast-v1','exact','exit-v1','cal','2026-08-01T12:00:00Z'),
          (2,2,'forecast-v1','mixed','exit-v1','cal','2026-08-01T12:00:00Z'),
          (3,3,'forecast-v1','mixed','exit-v1','cal','2026-08-02T12:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO strategy_forecast_assessments VALUES
          (10,'policy','forecast-v1','model','cal','exact','exit-v1','resolver','rules',
           '2026-08-01','2026-08-02','hash-1',1),
          (20,'policy','forecast-v1','model','cal','mixed','exit-v1','resolver','rules',
           '2026-08-01','2026-08-02','hash-2',2)
        """
    )
    conn.execute(
        """
        INSERT INTO strategy_forecast_assessment_current VALUES
          ('policy','forecast-v1','model','cal','exact','exit-v1','resolver','rules',10,now()),
          ('policy','forecast-v1','model','cal','mixed','exit-v1','resolver','rules',20,now())
        """
    )

    conn.execute(sql.SQL(cast(LiteralString, _MIGRATION.read_text())))

    assert conn.execute(
        "SELECT assessment_id,strategy_id,strategy_version FROM strategy_forecast_assessments"
    ).fetchall() == [(10, "S-A", "v1")]
    assert conn.execute(
        "SELECT assessment_id,strategy_id,strategy_version FROM strategy_forecast_assessment_current"
    ).fetchall() == [(10, "S-A", "v1")]
    assert conn.execute(
        """
        SELECT is_nullable FROM information_schema.columns
        WHERE table_schema LIKE 'pg_temp%%' AND table_name='strategy_forecast_assessments'
          AND column_name='strategy_id'
        """
    ).fetchone() == ("NO",)
