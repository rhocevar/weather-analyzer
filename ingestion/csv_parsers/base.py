"""
Shared utilities for all CSV parsers.

Every parser in this package must produce WeatherRecord and
MonthlySummaryRecord instances using the helpers here, so that
the output contract is consistent regardless of source format.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

import pandas as pd
from dateutil import parser as dateutil_parser

from ingestion.models import MonthlySummaryRecord, WeatherRecord

logger = logging.getLogger(__name__)

SUMMARY_LABELS = {"sum", "average", "normal"}

MISSING_SENTINELS = {"m", "error", "no weather", "-", ""}


def safe_float(value: object, field: str, flags: list[str]) -> Optional[float]:
    """Coerce *value* to float, returning None for missing/unreadable values.

    Appends a descriptive string to *flags* when a coercion issue is found.
    Never raises.
    """
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() in MISSING_SENTINELS:
        return None

    if s.endswith("%"):
        s = s[:-1]
        flags.append(f"percent_sign_stripped:{field}={s}")

    try:
        return float(s)
    except ValueError:
        flags.append(f"parse_error:{field}={value!r}")
        return None


def try_parse_date(value: object) -> Optional[str]:
    """Return ISO date string 'YYYY-MM-DD', or None if *value* is not a date."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in MISSING_SENTINELS:
        return None
    if not re.match(r"\d{1,2}/\d{1,2}/\d{2,4}", s):
        return None
    try:
        dt = dateutil_parser.parse(s, dayfirst=False)
        if dt.year < 2000:
            dt = dt.replace(year=dt.year + 100)
        return dt.date().isoformat()
    except Exception:
        return None


def build_record(
    row: pd.Series,
    col_map: dict,
    source_file: str,
    seen_dates: Optional[set] = None,
    duplicate_log: Optional[list] = None,
) -> Optional[WeatherRecord]:
    """Build a WeatherRecord from a DataFrame row using *col_map*.

    col_map maps field names to integer column indices within the row.
    Returns None for rows with a missing date or a duplicate date.
    """
    date_str = try_parse_date(row.iloc[col_map["date"]] if "date" in col_map else None)
    if date_str is None:
        return None

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

    return WeatherRecord(
        observation_date=date_str,
        temp_max_f=get("temp_max_f"),
        temp_min_f=get("temp_min_f"),
        temp_avg_f=get("temp_avg_f"),
        temp_departure_f=get("temp_departure_f"),
        heating_degree_days=get("heating_degree_days"),
        cooling_degree_days=get("cooling_degree_days"),
        precipitation_in=get("precipitation_in"),
        snow_depth_in=get("snow_depth_in"),
        data_source="csv",
        source_file=source_file,
        parse_flags=flags,
    )


def build_summary(
    row: pd.Series,
    col_map: dict,
    month_year: str,
    summary_type: str,
    source_file: str,
) -> MonthlySummaryRecord:
    """Build a MonthlySummaryRecord from a Sum/Average/Normal footer row."""
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
