"""
CSV parser dispatcher — public interface for the ingestion pipeline.

parse_csv(path)     — parse a single CSV file
parse_csv_dir(dir)  — parse all *.csv files in a directory

How it works
------------
Each format in ingestion/csv_parsers/ is a class with two classmethods:

    can_parse(df: pd.DataFrame) -> bool
        Inspect the raw DataFrame and return True if this parser
        knows how to handle the file.

    parse(csv_path: str) -> (daily, summaries, errors)
        Extract records from the file and return them.

Parsers are tried in REGISTRY order; the first one that returns
True from can_parse() wins.  To add support for a new CSV format:
  1. Create ingestion/csv_parsers/your_format.py with the two classmethods.
  2. Add the class to REGISTRY below.

No other file needs to change.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ingestion.models import MonthlySummaryRecord, WeatherRecord
from ingestion.csv_parsers.noaa_monthly import NOAAMonthlyParser

logger = logging.getLogger(__name__)

# ── Parser registry ───────────────────────────────────────────────────────────
# Tried in order; first match wins.

REGISTRY = [
    NOAAMonthlyParser,
]


# ── Public interface ──────────────────────────────────────────────────────────

def parse_csv(
    csv_path: str,
) -> tuple[list[WeatherRecord], list[MonthlySummaryRecord], list[str]]:
    """Parse a single CSV file using the first matching registered parser.

    Returns:
        daily     : list of WeatherRecord (one per valid day)
        summaries : list of MonthlySummaryRecord (Sum/Average/Normal rows)
        errors    : list of error/warning strings for ingestion_log
    """
    df = pd.read_csv(csv_path, header=None, dtype=str, keep_default_na=False)

    for parser_cls in REGISTRY:
        if parser_cls.can_parse(df):
            logger.debug("Using %s for %s", parser_cls.__name__, Path(csv_path).name)
            return parser_cls.parse(csv_path)

    logger.error("No registered parser can handle %s", csv_path)
    return [], [], [f"no_parser_found:{Path(csv_path).name}"]


def parse_csv_dir(
    csv_dir: str,
) -> tuple[list[WeatherRecord], list[MonthlySummaryRecord], list[str]]:
    """Parse all *.csv files in *csv_dir* and combine results."""
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
