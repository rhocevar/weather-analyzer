"""
CSV parser for data/csv files.

CSV files may have a non-standard side-by-side layout:
  - Rows 1-37  : January (cols 0-7) and February (cols 8-16) in parallel
  - Row 38     : blank separator
  - Rows 39-76 : March (cols 0-8) in its own section (includes Snow Depth)

Known data-quality issues handled here:
  - "M", "ERROR", "NO WEATHER", "-", blank → NULL (via safe_float)
  - "45%"  in a numeric field  → strip %, store float, flag
  - Duplicate date rows (Feb 2/17-2/22 appear twice) → first wins, second logged
  - Missing Feb date on row for Jan 1/3  → row skipped with flag
  - Sum / Average / Normal footer rows   → routed to MonthlySummaryRecord
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd
from dateutil import parser as dateutil_parser

from ingestion.models import MonthlySummaryRecord, WeatherRecord

logger = logging.getLogger(__name__)

# ── Column indices within each block ────────────────────────────────────────
# January block starts at CSV column 0
_JAN_COLS = {
    "date": 0,
    "temp_max_f": 1,
    "temp_min_f": 2,
    "temp_avg_f": 3,
    "temp_departure_f": 4,
    "heating_degree_days": 5,
    "cooling_degree_days": 6,
    "precipitation_in": 7,
}

# February block starts at CSV column 8
# Note: col 14 is a stray blank column between HDD and CDD in the source
_FEB_COLS = {
    "date": 8,
    "temp_max_f": 9,
    "temp_min_f": 10,
    "temp_avg_f": 11,
    "temp_departure_f": 12,
    "heating_degree_days": 13,
    # col 14 is blank/extra — skipped
    "cooling_degree_days": 15,
    "precipitation_in": 16,
}

# March block starts at CSV column 0, with Snow Depth at col 8
_MAR_COLS = {
    "date": 0,
    "temp_max_f": 1,
    "temp_min_f": 2,
    "temp_avg_f": 3,
    "temp_departure_f": 4,
    "heating_degree_days": 5,
    "cooling_degree_days": 6,
    "precipitation_in": 7,
    "snow_depth_in": 8,
}

_SUMMARY_LABELS = {"sum", "average", "normal"}

_MISSING_SENTINELS = {"m", "error", "no weather", "-", ""}


def safe_float(value: object, field: str, flags: list[str]) -> Optional[float]:
    """Coerce *value* to float, returning None for missing/unreadable values.

    Appends a descriptive string to *flags* when a coercion issue is found.
    Never raises.
    """
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() in _MISSING_SENTINELS:
        return None

    # Strip a trailing "%" sign (e.g. "45%" in departure column)
    if s.endswith("%"):
        s = s[:-1]
        flags.append(f"percent_sign_stripped:{field}={s}")

    try:
        return float(s)
    except ValueError:
        flags.append(f"parse_error:{field}={value!r}")
        return None


def _try_parse_date(value: object) -> Optional[str]:
    """Return ISO date string 'YYYY-MM-DD' or None if *value* is not a date."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in _MISSING_SENTINELS:
        return None
    # Quick check: must look like M/D/YY or similar
    if not re.match(r"\d{1,2}/\d{1,2}/\d{2,4}", s):
        return None
    try:
        dt = dateutil_parser.parse(s, dayfirst=False)
        # Two-digit years: dateutil may guess 1926 → correct to 2026
        if dt.year < 2000:
            dt = dt.replace(year=dt.year + 100)
        return dt.date().isoformat()
    except Exception:
        return None


def _extract_month_year(iso_date: str) -> str:
    """'2026-01-15' → '2026-01'"""
    return iso_date[:7]


def _build_record(
    row: pd.Series,
    col_map: dict,
    source_file: str,
    data_source: str = "csv",
    seen_dates: Optional[set] = None,
    duplicate_log: Optional[list] = None,
) -> Optional[WeatherRecord]:
    """Build a WeatherRecord from a raw DataFrame row using *col_map*.

    Returns None for rows that should be skipped (missing date, all-missing data,
    or duplicate).
    """
    date_str = _try_parse_date(row.iloc[col_map["date"]] if "date" in col_map else None)
    if date_str is None:
        return None

    # Duplicate detection within the CSV source
    if seen_dates is not None:
        if date_str in seen_dates:
            msg = f"csv_internal_duplicate:{date_str}"
            logger.warning(msg)
            if duplicate_log is not None:
                duplicate_log.append(msg)
            return None
        seen_dates.add(date_str)

    flags: list[str] = []

    def get(field: str) -> Optional[float]:
        idx = col_map.get(field)
        if idx is None:
            return None
        raw = row.iloc[idx] if idx < len(row) else None
        return safe_float(raw, field, flags)

    record = WeatherRecord(
        observation_date=date_str,
        temp_max_f=get("temp_max_f"),
        temp_min_f=get("temp_min_f"),
        temp_avg_f=get("temp_avg_f"),
        temp_departure_f=get("temp_departure_f"),
        heating_degree_days=get("heating_degree_days"),
        cooling_degree_days=get("cooling_degree_days"),
        precipitation_in=get("precipitation_in"),
        snow_depth_in=get("snow_depth_in"),
        data_source=data_source,  # type: ignore[arg-type]
        source_file=source_file,
        parse_flags=flags,
    )
    return record


def _build_summary(
    row: pd.Series,
    col_map: dict,
    month_year: str,
    summary_type: str,
    source_file: str,
) -> MonthlySummaryRecord:
    flags: list[str] = []

    def get(field: str) -> Optional[float]:
        idx = col_map.get(field)
        if idx is None:
            return None
        raw = row.iloc[idx] if idx < len(row) else None
        return safe_float(raw, field, flags)

    return MonthlySummaryRecord(
        month_year=month_year,
        summary_type=summary_type,
        temp_max_f=get("temp_max_f"),
        temp_min_f=get("temp_min_f"),
        temp_avg_f=get("temp_avg_f"),
        temp_departure_f=get("temp_departure_f"),
        heating_degree_days=get("heating_degree_days"),
        cooling_degree_days=get("cooling_degree_days"),
        precipitation_in=get("precipitation_in"),
        source_file=source_file,
    )


# ── Public interface ─────────────────────────────────────────────────────────

def parse_csv(
    csv_path: str,
) -> tuple[list[WeatherRecord], list[MonthlySummaryRecord], list[str]]:
    """Parse the 3-month weather CSV.

    Returns:
        daily   : list of WeatherRecord (one per valid day)
        summaries: list of MonthlySummaryRecord (Sum/Average/Normal rows)
        errors  : list of error/warning strings for ingestion_log
    """
    source_file = Path(csv_path).name
    df = pd.read_csv(csv_path, header=None, dtype=str, keep_default_na=False)

    daily: list[WeatherRecord] = []
    summaries: list[MonthlySummaryRecord] = []
    errors: list[str] = []

    # ── Section 1: January + February (rows 0-36, i.e. lines 1-37) ──────────
    # Data rows start at index 3 (after 3 header rows)
    seen_jan: set[str] = set()
    seen_feb: set[str] = set()
    jan_feb_data = df.iloc[3:37]

    for _, row in jan_feb_data.iterrows():
        first_val = str(row.iloc[0]).strip()

        # ── January: col 0 holds the date ───────────────────────────────
        if first_val.lower() in _SUMMARY_LABELS:
            s = _build_summary(row, _JAN_COLS, "2026-01", first_val.lower(), source_file)
            summaries.append(s)
        else:
            rec = _build_record(row, _JAN_COLS, source_file, seen_dates=seen_jan, duplicate_log=errors)
            if rec:
                daily.append(rec)

        # ── February: col 8 holds the date ──────────────────────────────
        feb_date_raw = str(row.iloc[_FEB_COLS["date"]]).strip()
        if feb_date_raw.lower() in _SUMMARY_LABELS:
            s = _build_summary(row, _FEB_COLS, "2026-02", feb_date_raw.lower(), source_file)
            summaries.append(s)
        elif _try_parse_date(feb_date_raw) is None:
            # Col 8 is not a date — the Feb date is missing for this row
            if feb_date_raw not in ("", "nan"):
                errors.append(f"feb_date_missing:row_with_jan={first_val},col8={feb_date_raw!r}")
        else:
            rec = _build_record(row, _FEB_COLS, source_file, seen_dates=seen_feb, duplicate_log=errors)
            if rec:
                daily.append(rec)

    # ── Section 2: March (rows 38-75, i.e. lines 39-76) ─────────────────────
    # Row 38 is the blank separator; row 39 = "26-Mar" header; rows 40-41 = sub-headers
    # Data rows start at index 42 (0-indexed), which is df.iloc[42]
    seen_mar: set[str] = set()

    # Find where March data starts: first row after row 38 where col 0 looks like a date
    mar_section = df.iloc[41:]
    for _, row in mar_section.iterrows():
        first_val = str(row.iloc[0]).strip()
        if not first_val or first_val.startswith("Above Normals"):
            continue

        if first_val.lower() in _SUMMARY_LABELS:
            s = _build_summary(row, _MAR_COLS, "2026-03", first_val.lower(), source_file)
            summaries.append(s)
        else:
            rec = _build_record(row, _MAR_COLS, source_file, seen_dates=seen_mar, duplicate_log=errors)
            if rec:
                daily.append(rec)

    logger.info(
        "CSV parse complete: %d daily records, %d summaries, %d errors/warnings",
        len(daily),
        len(summaries),
        len(errors),
    )
    return daily, summaries, errors


def parse_csv_dir(
    csv_dir: str,
) -> tuple[list[WeatherRecord], list[MonthlySummaryRecord], list[str]]:
    """Parse all CSV files in *csv_dir* and combine results.

    Returns the same tuple as parse_csv — daily records, monthly summaries,
    and a combined list of errors/warnings from all files.
    """
    csv_paths = sorted(Path(csv_dir).glob("*.csv"))
    if not csv_paths:
        logger.warning("No CSV files found in %s", csv_dir)
        return [], [], []

    all_daily: list[WeatherRecord] = []
    all_summaries: list[MonthlySummaryRecord] = []
    all_errors: list[str] = []

    for path in csv_paths:
        logger.info("Parsing CSV: %s", path.name)
        daily, summaries, errors = parse_csv(str(path))
        all_daily.extend(daily)
        all_summaries.extend(summaries)
        all_errors.extend(errors)

    return all_daily, all_summaries, all_errors
