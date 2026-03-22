"""Unit tests for ingestion/csv_parser.py."""

from pathlib import Path

import pytest

from ingestion import config
from ingestion.csv_parser import parse_csv, safe_float


# ── safe_float ────────────────────────────────────────────────────────────────

class TestSafeFloat:
    def test_valid_integer(self):
        assert safe_float("72", "f", []) == 72.0

    def test_valid_float(self):
        assert safe_float("72.5", "f", []) == 72.5

    def test_m_sentinel(self):
        assert safe_float("M", "f", []) is None

    def test_m_lowercase(self):
        assert safe_float("m", "f", []) is None

    def test_error_sentinel(self):
        assert safe_float("ERROR", "f", []) is None

    def test_no_weather_sentinel(self):
        assert safe_float("NO WEATHER", "f", []) is None

    def test_blank(self):
        assert safe_float("", "f", []) is None

    def test_dash(self):
        assert safe_float("-", "f", []) is None

    def test_none_input(self):
        assert safe_float(None, "f", []) is None

    def test_percent_stripped(self):
        flags = []
        result = safe_float("45%", "temp_departure_f", flags)
        assert result == 45.0
        assert any("percent_sign_stripped" in f for f in flags)

    def test_parse_error_appended(self):
        flags = []
        result = safe_float("not_a_number", "f", flags)
        assert result is None
        assert any("parse_error" in f for f in flags)

    def test_whitespace_stripped(self):
        assert safe_float("  72.5  ", "f", []) == 72.5


# ── parse_csv integration ─────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def csv_parsed():
    daily, summaries, errors = parse_csv(str(Path(config.CSV_DIR) / "3month_weather.csv"))
    return daily, summaries, errors


class TestCSVParserDailyRecords:
    def test_all_numeric_fields_are_float_or_none(self, csv_parsed):
        daily, _, _ = csv_parsed
        numeric_fields = [
            "temp_max_f", "temp_min_f", "temp_avg_f", "temp_departure_f",
            "heating_degree_days", "cooling_degree_days",
            "precipitation_in", "snow_depth_in",
        ]
        for rec in daily:
            for field in numeric_fields:
                val = getattr(rec, field)
                assert val is None or isinstance(val, float), (
                    f"{rec.observation_date} {field}={val!r} is not float or None"
                )

    def test_data_source_is_csv(self, csv_parsed):
        daily, _, _ = csv_parsed
        assert all(r.data_source == "csv" for r in daily)

    def test_jan_records_present(self, csv_parsed):
        daily, _, _ = csv_parsed
        jan = [r for r in daily if str(r.observation_date).startswith("2026-01")]
        assert len(jan) == 31

    def test_march_total_records(self, csv_parsed):
        daily, _, _ = csv_parsed
        mar = [r for r in daily if str(r.observation_date).startswith("2026-03")]
        assert len(mar) == 31

    def test_march_records_with_data(self, csv_parsed):
        daily, _, _ = csv_parsed
        mar_with_data = [
            r for r in daily
            if str(r.observation_date).startswith("2026-03") and r.temp_max_f is not None
        ]
        assert len(mar_with_data) == 17

    def test_error_field_becomes_null(self, csv_parsed):
        """2/23/26 has 'ERROR' for max temp — should be None."""
        daily, _, _ = csv_parsed
        feb23 = next(r for r in daily if str(r.observation_date) == "2026-02-23")
        assert feb23.temp_max_f is None

    def test_percent_sign_departure_stored(self, csv_parsed):
        """2/26/26 has '45%' departure — should be stored as 45.0 with a flag."""
        daily, _, _ = csv_parsed
        feb26 = next(r for r in daily if str(r.observation_date) == "2026-02-26")
        assert feb26.temp_departure_f == 45.0
        assert any("percent_sign_stripped" in f for f in feb26.parse_flags)

    def test_no_weather_row_has_null_max(self, csv_parsed):
        """3/18/26 has 'NO WEATHER' — max_temp should be None."""
        daily, _, _ = csv_parsed
        mar18 = next(r for r in daily if str(r.observation_date) == "2026-03-18")
        assert mar18.temp_max_f is None

    def test_m_rows_have_null_values(self, csv_parsed):
        """3/19-3/31 have 'M' for all fields — all numeric fields should be None."""
        daily, _, _ = csv_parsed
        mar_missing = [
            r for r in daily
            if str(r.observation_date) >= "2026-03-19"
        ]
        assert len(mar_missing) == 13
        for r in mar_missing:
            assert r.temp_max_f is None
            assert r.precipitation_in is None


class TestCSVParserDuplicates:
    def test_duplicate_dates_logged(self, csv_parsed):
        """Feb 2/17-2/22 appear twice — duplicates should be logged as errors."""
        _, _, errors = csv_parsed
        dup_errors = [e for e in errors if "csv_internal_duplicate" in e]
        assert len(dup_errors) == 6, f"Expected 6 duplicate errors, got {dup_errors}"

    def test_first_occurrence_wins(self, csv_parsed):
        """Feb 17 first occurrence has dep=-5.6; second has -2.7. First should win."""
        daily, _, _ = csv_parsed
        feb17_recs = [r for r in daily if str(r.observation_date) == "2026-02-17"]
        assert len(feb17_recs) == 1
        # First occurrence departure is -5.6
        assert feb17_recs[0].temp_departure_f == pytest.approx(-5.6, abs=0.01)


class TestCSVParserSummaryRows:
    def test_summary_rows_excluded_from_daily(self, csv_parsed):
        """Sum/Average/Normal rows must not appear in daily records."""
        daily, _, _ = csv_parsed
        dates = [str(r.observation_date) for r in daily]
        assert not any(d in ("Sum", "Average", "Normal") for d in dates)

    def test_nine_summary_records(self, csv_parsed):
        """3 months × 3 summary types = 9 summary records."""
        _, summaries, _ = csv_parsed
        assert len(summaries) == 9

    def test_summary_types(self, csv_parsed):
        _, summaries, _ = csv_parsed
        types = {s.summary_type for s in summaries}
        assert types == {"sum", "average", "normal"}

    def test_all_three_months_have_summaries(self, csv_parsed):
        _, summaries, _ = csv_parsed
        months = {s.month_year for s in summaries}
        assert months == {"2026-01", "2026-02", "2026-03"}
