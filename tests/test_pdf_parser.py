"""Unit tests for ingestion/pdf_parser.py.

Note: TestExtractPdfContent is an integration test that reads real PDF files
from data/pdf/.  It is skipped automatically when that directory is absent
(e.g. on a fresh checkout before the data files are added).
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import anthropic
import pytest

from ingestion import config
from ingestion.pdf_parser import _date_from_filename, _extract_pdf_content, parse_pdf

_PDF_DIR_AVAILABLE = Path(config.PDF_DIR).is_dir() and any(
    Path(config.PDF_DIR).glob("*.pdf")
)


# ── _date_from_filename ───────────────────────────────────────────────────────

class TestDateFromFilename:
    def test_standard_filename(self):
        assert _date_from_filename("weather_mar09.pdf") == "2026-03-09"

    def test_single_digit_day(self):
        assert _date_from_filename("weather_mar9.pdf") == "2026-03-09"

    def test_two_digit_day(self):
        assert _date_from_filename("weather_mar18.pdf") == "2026-03-18"

    def test_uppercase(self):
        assert _date_from_filename("weather_MAR15.pdf") == "2026-03-15"

    def test_unrecognised_filename(self):
        assert _date_from_filename("unknown_file.pdf") is None


# ── _extract_pdf_content ──────────────────────────────────────────────────────

@pytest.mark.skipif(not _PDF_DIR_AVAILABLE, reason="data/pdf/ directory not present")
class TestExtractPdfContent:
    @pytest.mark.parametrize("pdf_name", [
        "weather_mar09.pdf", "weather_mar10.pdf", "weather_mar11.pdf",
        "weather_mar12.pdf", "weather_mar13.pdf", "weather_mar14.pdf",
        "weather_mar15.pdf", "weather_mar16.pdf", "weather_mar17.pdf",
        "weather_mar18.pdf",
    ])
    def test_content_is_non_empty(self, pdf_name):
        path = str(Path(config.PDF_DIR) / pdf_name)
        content = _extract_pdf_content(path)
        assert len(content) > 20, f"{pdf_name}: extracted content too short"

    @pytest.mark.parametrize("pdf_name", [
        "weather_mar09.pdf", "weather_mar10.pdf", "weather_mar11.pdf",
        "weather_mar12.pdf", "weather_mar13.pdf", "weather_mar14.pdf",
        "weather_mar15.pdf", "weather_mar16.pdf", "weather_mar17.pdf",
        "weather_mar18.pdf",
    ])
    def test_content_contains_temperature_keyword(self, pdf_name):
        path = str(Path(config.PDF_DIR) / pdf_name)
        content = _extract_pdf_content(path).lower()
        assert "temp" in content or "max" in content or "min" in content, (
            f"{pdf_name}: no temperature-related keyword found in extracted content"
        )


# ── parse_pdf (with mocked Claude) ───────────────────────────────────────────

def _make_mock_client(response_dict: dict):
    mock_content = MagicMock()
    mock_content.text = json.dumps(response_dict)
    mock_message = MagicMock()
    mock_message.content = [mock_content]
    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_message
    return mock_client


class TestParsePdf:
    def test_full_extraction(self):
        client = _make_mock_client({
            "observation_date": "2026-03-09",
            "temp_max_f": 74,
            "temp_min_f": 54,
            "temp_avg_f": 64.0,
            "temp_departure_f": 3.4,
            "heating_degree_days": 1,
            "cooling_degree_days": 0,
            "precipitation_in": 0.00,
            "snow_depth_in": 0,
            "parse_notes": "",
        })
        r = parse_pdf(str(Path(config.PDF_DIR) / "weather_mar09.pdf"), client=client)
        assert r.observation_date.isoformat() == "2026-03-09"
        assert r.temp_max_f == 74.0
        assert r.temp_min_f == 54.0
        assert r.temp_avg_f == 64.0
        assert r.temp_departure_f == pytest.approx(3.4)
        assert r.heating_degree_days == 1.0
        assert r.cooling_degree_days == 0.0
        assert r.precipitation_in == 0.0
        assert r.snow_depth_in == 0.0
        assert r.data_source == "pdf"
        assert r.parse_flags == []

    def test_null_values_allowed(self):
        """Claude returns null for missing fields — must map to None."""
        client = _make_mock_client({
            "observation_date": "2026-03-18",
            "temp_max_f": None,
            "temp_min_f": None,
            "temp_avg_f": None,
            "temp_departure_f": None,
            "heating_degree_days": None,
            "cooling_degree_days": None,
            "precipitation_in": None,
            "snow_depth_in": None,
            "parse_notes": "All observations missing (M)",
        })
        r = parse_pdf(str(Path(config.PDF_DIR) / "weather_mar18.pdf"), client=client)
        assert r.temp_max_f is None
        assert r.temp_min_f is None
        assert any("All observations missing" in f for f in r.parse_flags)

    def test_date_from_filename_fallback(self):
        """If Claude returns no date, fall back to filename."""
        client = _make_mock_client({
            "observation_date": None,
            "temp_max_f": 80,
            "temp_min_f": 55,
            "temp_avg_f": 67.5,
            "temp_departure_f": None,
            "heating_degree_days": None,
            "cooling_degree_days": None,
            "precipitation_in": 0,
            "snow_depth_in": None,
            "parse_notes": "",
        })
        r = parse_pdf(str(Path(config.PDF_DIR) / "weather_mar15.pdf"), client=client)
        assert r.observation_date.isoformat() == "2026-03-15"
        assert any("date_from_filename_fallback" in f for f in r.parse_flags)

    def test_date_mismatch_flagged(self):
        """If Claude's date differs from the filename date, a flag is added."""
        client = _make_mock_client({
            "observation_date": "2026-03-10",  # wrong — filename says mar09
            "temp_max_f": 74,
            "temp_min_f": 54,
            "temp_avg_f": 64.0,
            "temp_departure_f": None,
            "heating_degree_days": None,
            "cooling_degree_days": None,
            "precipitation_in": 0,
            "snow_depth_in": None,
            "parse_notes": "",
        })
        r = parse_pdf(str(Path(config.PDF_DIR) / "weather_mar09.pdf"), client=client)
        assert any("date_filename_mismatch" in f for f in r.parse_flags)

    def test_claude_api_failure_returns_minimal_record(self):
        """If Claude API fails entirely, return a minimal record with filename date."""
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("Network error")
        r = parse_pdf(str(Path(config.PDF_DIR) / "weather_mar12.pdf"), client=mock_client)
        assert r.observation_date.isoformat() == "2026-03-12"
        assert r.temp_max_f is None
        assert any("claude_api_error" in f for f in r.parse_flags)
        assert any("claude_extraction_failed" in f for f in r.parse_flags)

    def test_all_pdf_files_return_a_date(self):
        """Every PDF must produce a record with a non-None observation_date."""
        for pdf_path in sorted(Path(config.PDF_DIR).glob("*.pdf")):
            # Use filename fallback (no API call needed)
            mock_client = MagicMock()
            mock_client.messages.create.side_effect = Exception("Simulated failure")
            r = parse_pdf(str(pdf_path), client=mock_client)
            assert r.observation_date is not None, f"{pdf_path.name}: no date"
