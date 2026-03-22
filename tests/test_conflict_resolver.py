"""Unit tests for ingestion/conflict_resolver.py."""

import json
from datetime import date

import pytest
from sqlalchemy import select, text

from ingestion.conflict_resolver import get_conflicts, resolve_conflicts
from ingestion.models import WeatherRecord
from ingestion.schema import daily_weather, initialize_db


@pytest.fixture
def test_engine(tmp_path):
    """Return a freshly initialised in-memory-like SQLite engine for each test."""
    db_path = str(tmp_path / "test.db")
    engine = initialize_db(db_path)
    return engine


def _insert_record(engine, obs_date: str, data_source: str, temp_max: float | None = None):
    rec = WeatherRecord(
        observation_date=obs_date,
        temp_max_f=temp_max,
        data_source=data_source,  # type: ignore[arg-type]
        source_file=f"test_{data_source}.{'pdf' if data_source == 'pdf' else 'csv'}",
    )
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    with engine.begin() as conn:
        conn.execute(
            sqlite_insert(daily_weather)
            .values(**rec.to_db_dict())
            .prefix_with("OR IGNORE")
        )


class TestResolveConflicts:
    def test_no_conflicts_returns_empty(self, test_engine):
        _insert_record(test_engine, "2026-01-01", "csv", 68.0)
        result = resolve_conflicts(test_engine)
        assert result == []

    def test_conflict_detected(self, test_engine):
        _insert_record(test_engine, "2026-03-09", "pdf", 74.0)
        _insert_record(test_engine, "2026-03-09", "csv", 74.0)
        result = resolve_conflicts(test_engine)
        assert "2026-03-09" in result

    def test_pdf_row_flagged_as_winner(self, test_engine):
        _insert_record(test_engine, "2026-03-09", "pdf", 74.0)
        _insert_record(test_engine, "2026-03-09", "csv", 74.0)
        resolve_conflicts(test_engine)
        with test_engine.connect() as conn:
            row = conn.execute(
                select(daily_weather.c.parse_flags, daily_weather.c.is_resolved_conflict)
                .where(
                    daily_weather.c.observation_date == "2026-03-09",
                    daily_weather.c.data_source == "pdf",
                )
            ).fetchone()
        flags = json.loads(row[0] or "[]")
        assert "conflict_won_over_csv" in flags
        assert row[1] is True

    def test_csv_row_flagged_as_loser(self, test_engine):
        _insert_record(test_engine, "2026-03-09", "pdf", 74.0)
        _insert_record(test_engine, "2026-03-09", "csv", 74.0)
        resolve_conflicts(test_engine)
        with test_engine.connect() as conn:
            row = conn.execute(
                select(daily_weather.c.parse_flags, daily_weather.c.is_resolved_conflict)
                .where(
                    daily_weather.c.observation_date == "2026-03-09",
                    daily_weather.c.data_source == "csv",
                )
            ).fetchone()
        flags = json.loads(row[0] or "[]")
        assert "conflict_lost_to_pdf" in flags
        assert row[1] is True

    def test_idempotent(self, test_engine):
        """Calling resolve_conflicts twice should not duplicate flags."""
        _insert_record(test_engine, "2026-03-09", "pdf", 74.0)
        _insert_record(test_engine, "2026-03-09", "csv", 74.0)
        resolve_conflicts(test_engine)
        resolve_conflicts(test_engine)
        with test_engine.connect() as conn:
            row = conn.execute(
                select(daily_weather.c.parse_flags)
                .where(
                    daily_weather.c.observation_date == "2026-03-09",
                    daily_weather.c.data_source == "pdf",
                )
            ).fetchone()
        flags = json.loads(row[0] or "[]")
        assert flags.count("conflict_won_over_csv") == 1


class TestActiveView:
    def test_pdf_wins_for_conflict_date(self, test_engine):
        """active_daily_weather must return the PDF row for a conflict date."""
        _insert_record(test_engine, "2026-03-09", "pdf", 74.0)
        _insert_record(test_engine, "2026-03-09", "csv", 70.0)  # different max
        resolve_conflicts(test_engine)
        with test_engine.connect() as conn:
            rows = conn.execute(
                text("SELECT data_source, temp_max_f FROM active_daily_weather WHERE observation_date = '2026-03-09'")
            ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "pdf"
        assert rows[0][1] == 74.0

    def test_csv_row_returned_for_non_pdf_date(self, test_engine):
        """Jan/Feb dates (CSV-only) must appear in active_daily_weather."""
        _insert_record(test_engine, "2026-01-15", "csv", 80.0)
        with test_engine.connect() as conn:
            rows = conn.execute(
                text("SELECT data_source FROM active_daily_weather WHERE observation_date = '2026-01-15'")
            ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "csv"

    def test_one_row_per_date_after_resolution(self, test_engine):
        """active_daily_weather must return exactly one row per date."""
        _insert_record(test_engine, "2026-03-09", "pdf", 74.0)
        _insert_record(test_engine, "2026-03-09", "csv", 74.0)
        _insert_record(test_engine, "2026-01-01", "csv", 68.0)
        resolve_conflicts(test_engine)
        with test_engine.connect() as conn:
            rows = conn.execute(
                text("SELECT observation_date, COUNT(*) FROM active_daily_weather GROUP BY observation_date HAVING COUNT(*) > 1")
            ).fetchall()
        assert rows == [], f"Duplicate dates in active view: {rows}"
