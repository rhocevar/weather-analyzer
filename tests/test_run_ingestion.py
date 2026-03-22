"""Smoke tests for ingestion/run_ingestion.py — the pipeline orchestrator.

All file-parsing is mocked so tests run without real I/O.  The orchestration
logic (DB init, idempotency, conflict resolution, error handling) is what is
exercised here; the individual parsers are tested in their own test files.
"""

from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import func, select, text

from ingestion import config
from ingestion.models import MonthlySummaryRecord, WeatherRecord
from ingestion.run_ingestion import run
from ingestion.schema import daily_weather, initialize_db


# ---------------------------------------------------------------------------
# Minimal fixture records — parsers return these instead of touching real data
# ---------------------------------------------------------------------------

_CSV_RECORD = WeatherRecord(
    observation_date="2026-01-15",
    temp_max_f=70.0,
    temp_min_f=50.0,
    data_source="csv",
    source_file="3month_weather.csv",
)
_CSV_SUMMARY = MonthlySummaryRecord(
    month_year="2026-01",
    summary_type="average",
    temp_max_f=70.0,
    source_file="3month_weather.csv",
)
_CSV_PARSE_RESULT = ([_CSV_RECORD], [_CSV_SUMMARY], [])


def _pdf_record(path: str, _client=None) -> WeatherRecord:
    """Return a minimal WeatherRecord for any PDF path without calling the API."""
    from ingestion.pdf_parser import _date_from_filename

    name = Path(path).name
    return WeatherRecord(
        observation_date=_date_from_filename(name) or "2026-03-09",
        temp_max_f=75.0,
        temp_min_f=55.0,
        data_source="pdf",
        source_file=name,
    )


def _run(db_path: str, **kwargs) -> None:
    """Call run() with both parsers mocked out."""
    with (
        patch("ingestion.run_ingestion.parse_csv", return_value=_CSV_PARSE_RESULT),
        patch("ingestion.run_ingestion.parse_pdf", side_effect=_pdf_record),
    ):
        run(db_path=db_path, pdf_dir=config.PDF_DIR, csv_dir=config.CSV_DIR, **kwargs)


@pytest.fixture()
def temp_db(tmp_path):
    return str(tmp_path / "test.db")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRunIngestion:
    def test_smoke_run_completes(self, temp_db):
        """Full pipeline completes without error and produces daily records."""
        _run(temp_db)

        engine = initialize_db(temp_db)
        with engine.connect() as conn:
            count = conn.execute(
                select(func.count()).select_from(daily_weather)
            ).scalar()
        assert count > 0

    def test_idempotent(self, temp_db):
        """Running the pipeline twice produces the same row count."""
        _run(temp_db)
        engine = initialize_db(temp_db)
        with engine.connect() as conn:
            first_count = conn.execute(
                select(func.count()).select_from(daily_weather)
            ).scalar()

        _run(temp_db)
        with engine.connect() as conn:
            second_count = conn.execute(
                select(func.count()).select_from(daily_weather)
            ).scalar()

        assert first_count == second_count

    def test_force_reingest_same_count(self, temp_db):
        """--force-reingest re-runs ingestion and yields the same final row count."""
        _run(temp_db)
        engine = initialize_db(temp_db)
        with engine.connect() as conn:
            first_count = conn.execute(
                select(func.count()).select_from(daily_weather)
            ).scalar()

        _run(temp_db, force_reingest=True)
        with engine.connect() as conn:
            second_count = conn.execute(
                select(func.count()).select_from(daily_weather)
            ).scalar()

        assert first_count == second_count

    def test_pdf_parse_failure_does_not_abort_pipeline(self, temp_db):
        """A single PDF failure is logged but other records are still inserted."""
        call_count = {"n": 0}

        def _flaky(path, _client=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("Simulated API failure")
            return _pdf_record(path)

        with (
            patch("ingestion.run_ingestion.parse_csv", return_value=_CSV_PARSE_RESULT),
            patch("ingestion.run_ingestion.parse_pdf", side_effect=_flaky),
        ):
            run(db_path=temp_db, pdf_dir=config.PDF_DIR, csv_dir=config.CSV_DIR)

        engine = initialize_db(temp_db)
        with engine.connect() as conn:
            pdf_count = conn.execute(
                select(func.count()).select_from(daily_weather).where(
                    daily_weather.c.data_source == "pdf"
                )
            ).scalar()
        # One PDF failed; the rest should still be present
        assert pdf_count >= 1

    def test_active_view_accessible(self, temp_db):
        """active_daily_weather view exists and returns rows after ingestion."""
        _run(temp_db)

        engine = initialize_db(temp_db)
        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM active_daily_weather")
            ).scalar()
        assert count > 0
