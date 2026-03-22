"""Unit tests for chatbot/weather_chatbot.py — run_query()."""

import pytest
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ingestion import config
from ingestion.models import WeatherRecord
from ingestion.schema import daily_weather, initialize_db
from chatbot.weather_chatbot import run_query


# ---------------------------------------------------------------------------
# Self-contained test DB
# ---------------------------------------------------------------------------
# Tests must not depend on the live db/weather.db.  This module-scoped fixture
# creates an in-memory-equivalent temp DB with two known rows and patches
# config.DB_PATH so run_query() connects to it instead.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def _test_db(tmp_path_factory):
    """Create a temp DB with known rows and redirect run_query() to it."""
    db_path = str(tmp_path_factory.mktemp("chatbot") / "test.db")
    engine = initialize_db(db_path)

    rows = [
        # Mar 17 — hottest day at 98 °F (sourced from PDF)
        WeatherRecord(
            observation_date="2026-03-17",
            temp_max_f=98.0, temp_min_f=64.0, temp_avg_f=81.0,
            data_source="pdf", source_file="weather_mar17.pdf",
        ),
        # Mar 18 — all NULL (PDF reported "M" for every field)
        WeatherRecord(
            observation_date="2026-03-18",
            temp_max_f=None, temp_min_f=None, temp_avg_f=None,
            data_source="pdf", source_file="weather_mar18.pdf",
        ),
    ]

    with engine.begin() as conn:
        for rec in rows:
            conn.execute(
                sqlite_insert(daily_weather)
                .values(**rec.to_db_dict())
                .prefix_with("OR IGNORE")
            )

    original = config.DB_PATH
    config.DB_PATH = db_path
    yield
    config.DB_PATH = original


# ── SQL safety guard ──────────────────────────────────────────────────────────

class TestRunQuerySafetyGuard:
    def test_select_is_allowed(self):
        result = run_query(
            "SELECT observation_date FROM active_daily_weather LIMIT 1"
        )
        assert "observation_date" in result

    def test_with_cte_is_allowed(self):
        result = run_query(
            "WITH t AS (SELECT 1 AS n) SELECT n FROM t"
        )
        assert "Query error" not in result

    def test_drop_is_blocked(self):
        result = run_query("DROP TABLE daily_weather")
        assert result == "Error: only SELECT queries are permitted."

    def test_insert_is_blocked(self):
        result = run_query(
            "INSERT INTO daily_weather (observation_date) VALUES ('2099-01-01')"
        )
        assert result == "Error: only SELECT queries are permitted."

    def test_update_is_blocked(self):
        result = run_query("UPDATE daily_weather SET temp_max_f = 0")
        assert result == "Error: only SELECT queries are permitted."

    def test_delete_is_blocked(self):
        result = run_query("DELETE FROM daily_weather")
        assert result == "Error: only SELECT queries are permitted."


# ── Output format ─────────────────────────────────────────────────────────────

class TestRunQueryOutput:
    def test_returns_header_row(self):
        result = run_query(
            "SELECT observation_date, temp_max_f FROM active_daily_weather "
            "WHERE observation_date = '2026-03-17'"
        )
        assert "observation_date" in result
        assert "temp_max_f" in result

    def test_returns_correct_value(self):
        """Mar 17 is the hottest day at 98°F — sourced from the PDF."""
        result = run_query(
            "SELECT temp_max_f FROM active_daily_weather "
            "WHERE observation_date = '2026-03-17'"
        )
        assert "98.0" in result

    def test_null_rendered_as_null(self):
        """Mar 18 has all NULL values in the database."""
        result = run_query(
            "SELECT temp_max_f FROM active_daily_weather "
            "WHERE observation_date = '2026-03-18'"
        )
        assert "NULL" in result

    def test_empty_result_message(self):
        result = run_query(
            "SELECT * FROM active_daily_weather WHERE observation_date = '1900-01-01'"
        )
        assert result == "Query returned no rows."

    def test_bad_sql_returns_error_string(self):
        result = run_query("SELECT * FROM nonexistent_table_xyz")
        assert result.startswith("Query error:")

    def test_bad_sql_does_not_raise(self):
        # run_query must never propagate exceptions — Claude needs a string back.
        try:
            run_query("SELECT * FROM nonexistent_table_xyz")
        except Exception as exc:
            pytest.fail(f"run_query raised an exception: {exc}")
