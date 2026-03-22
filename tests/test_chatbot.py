"""Unit tests for chatbot/weather_chatbot.py — run_query()."""

import pytest

from chatbot.weather_chatbot import run_query


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
