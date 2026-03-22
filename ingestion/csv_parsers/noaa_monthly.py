"""
Parser for the NOAA-style 3-month side-by-side CSV format.

Layout:
  - Row 0      : "26-Jan" at col 0, "26-Feb" at col 8 (Jan and Feb share a section)
  - Rows 1-2   : group headers + field names for the Jan/Feb section
  - Rows 3-36  : Jan data (31 days + Sum/Average/Normal)
  - Rows 3-33  : Feb data (28 days + Sum/Average/Normal, parallel to Jan)
  - Row 37     : blank separator
  - Row 38     : "26-Mar" at col 0 (March in its own section)
  - Rows 39-40 : group headers + field names for the March section
  - Rows 41-75 : March data (31 days + Sum/Average/Normal + footnote)

Detection:
  Any cell in the DataFrame matches the pattern "YY-Mon" (e.g. "26-Jan").

Known data-quality issues in the source file:
  - "M", "ERROR", "NO WEATHER" in numeric fields  → NULL
  - "45%" in departure column                     → strip %, flag
  - Duplicate date rows for Feb 2/17-2/22         → first wins, logged
  - Missing Feb date on the Jan 1/3 row           → Feb row skipped
  - Sum / Average / Normal footer rows            → MonthlySummaryRecord
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

from ingestion.models import MonthlySummaryRecord, WeatherRecord
from ingestion.csv_parsers.base import (
    SUMMARY_LABELS,
    build_record,
    build_summary,
    try_parse_date,
)

logger = logging.getLogger(__name__)

_HEADER_PATTERN = re.compile(r"^\d{2}-[A-Za-z]{3}$")  # e.g. "26-Jan"

_MONTH_NUM = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


class NOAAMonthlyParser:
    """Handles the NOAA 3-month side-by-side CSV layout."""

    @classmethod
    def can_parse(cls, df: pd.DataFrame) -> bool:
        """Return True if any cell in the DataFrame looks like 'YY-Mon'."""
        if df.empty or len(df.columns) < 2:
            return False
        for row_idx in range(min(50, len(df))):  # month headers always appear in the first few rows
            for col_idx in range(len(df.columns)):
                if _HEADER_PATTERN.match(str(df.iloc[row_idx, col_idx]).strip()):
                    return True
        return False

    @classmethod
    def parse(
        cls, csv_path: str
    ) -> tuple[list[WeatherRecord], list[MonthlySummaryRecord], list[str]]:
        source_file = Path(csv_path).name
        df = pd.read_csv(csv_path, header=None, dtype=str, keep_default_na=False)

        daily: list[WeatherRecord] = []
        summaries: list[MonthlySummaryRecord] = []
        errors: list[str] = []

        month_blocks = cls._detect_month_blocks(df)

        for block in month_blocks:
            cls._parse_block(df, block, source_file, daily, summaries, errors)

        logger.info(
            "%s: %d daily records, %d summaries, %d warnings",
            source_file, len(daily), len(summaries), len(errors),
        )
        return daily, summaries, errors

    # ── Internal helpers ─────────────────────────────────────────────────────

    @classmethod
    def _detect_month_blocks(cls, df: pd.DataFrame) -> list[dict]:
        """Scan the entire DataFrame and return a list of block descriptors.

        Each descriptor carries the month_year string, the column map, and
        the slice of rows that contain the data for that month.

        Strategy:
        - Scan every cell for the "YY-Mon" pattern to find all month headers,
          recording (row_idx, col_idx, label) for each match.
        - Group headers by row_idx: headers in the same row form a "section"
          (e.g. Jan and Feb both appear in row 0).
        - For each section, find the data row range using the first block's
          date column so that stray values in other columns don't cut the range.
        - Build a col_map for each block using the section's header rows
          (header_row_idx + 1 for group headers, + 2 for field names).
        """
        # Collect all month-header cells
        header_cells: list[tuple[int, int, str]] = []
        for row_idx in range(len(df)):
            for col_idx in range(len(df.columns)):
                val = str(df.iloc[row_idx, col_idx]).strip()
                if _HEADER_PATTERN.match(val):
                    header_cells.append((row_idx, col_idx, val))

        if not header_cells:
            return []

        # Group by row → sections
        sections: dict[int, list[tuple[int, str]]] = defaultdict(list)
        for row_idx, col_idx, label in header_cells:
            sections[row_idx].append((col_idx, label))

        section_rows = sorted(sections.keys())
        blocks: list[dict] = []

        for sec_idx, header_row_idx in enumerate(section_rows):
            headers_in_section = sorted(sections[header_row_idx])  # by col

            # Section ends just before the next section's header row
            next_section_start = (
                section_rows[sec_idx + 1]
                if sec_idx + 1 < len(section_rows)
                else len(df)
            )

            # Use the first block's date column as the anchor for data boundaries.
            # This avoids stray numeric values in other date columns (e.g. "78" in
            # the Feb column on the Jan 1/3 row) from cutting the range short.
            first_date_col = headers_in_section[0][0]
            data_start, data_end = cls._find_data_rows(
                df, first_date_col, header_row_idx, next_section_start
            )

            for i, (col_idx, label) in enumerate(headers_in_section):
                month_abbr = label[3:].lower()
                month_num = _MONTH_NUM.get(month_abbr)
                if not month_num:
                    continue
                year = "20" + label[:2]
                month_year = f"{year}-{month_num}"

                next_col = (
                    headers_in_section[i + 1][0]
                    if i + 1 < len(headers_in_section)
                    else len(df.columns)
                )
                block_cols = list(range(col_idx, next_col))
                col_map = cls._build_col_map(df, block_cols, header_row_idx)

                blocks.append({
                    "month_year": month_year,
                    "col_map": col_map,
                    "data_start": data_start,
                    "data_end": data_end,
                })

        return blocks

    @staticmethod
    def _build_col_map(
        df: pd.DataFrame, block_cols: list[int], header_row_idx: int
    ) -> dict:
        """Build a field→absolute_column_index map by reading the header rows.

        For section starting at *header_row_idx*:
          - header_row_idx + 1 → group header row (Temperature, HDD, CDD, …)
          - header_row_idx + 2 → field name row (Date, Maximum, Minimum, …)
        """
        field_row = header_row_idx + 2
        group_row = header_row_idx + 1

        field_cells = {
            str(df.iloc[field_row, c]).strip().lower(): c
            for c in block_cols
            if str(df.iloc[field_row, c]).strip()
        }

        aliases = {
            "date": ["date"],
            "temp_max_f": ["maximum", "max", "max temp", "max temperature"],
            "temp_min_f": ["minimum", "min", "min temp", "min temperature"],
            "temp_avg_f": ["average", "avg", "mean"],
            "temp_departure_f": ["departure"],
            "heating_degree_days": ["hdd"],
            "cooling_degree_days": ["cdd"],
            "precipitation_in": ["precipitation"],
            "snow_depth_in": ["snow depth", "snow"],
        }

        col_map: dict[str, int] = {}
        for field, candidates in aliases.items():
            for candidate in candidates:
                if candidate in field_cells:
                    col_map[field] = field_cells[candidate]
                    break

        # HDD / CDD sometimes live in the group header row rather than the
        # field name row when the layout uses merged headers.
        group_cells = {
            str(df.iloc[group_row, c]).strip().lower(): c
            for c in block_cols
            if str(df.iloc[group_row, c]).strip()
        }
        if "heating_degree_days" not in col_map and "hdd" in group_cells:
            col_map["heating_degree_days"] = group_cells["hdd"]
        if "cooling_degree_days" not in col_map and "cdd" in group_cells:
            col_map["cooling_degree_days"] = group_cells["cdd"]

        return col_map

    @staticmethod
    def _find_data_rows(
        df: pd.DataFrame,
        date_col: int,
        section_header_row: int,
        section_end_row: int,
    ) -> tuple[int, int]:
        """Return (first_data_row, last_data_row+1) for the section.

        Scans *date_col* from *section_header_row* up to (but not including)
        *section_end_row*. Data rows are those where the date column contains
        a parseable date or a summary label (Sum/Average/Normal).

        Blank rows (empty date cell) do not end the range; any other
        non-data, non-empty row does (including footnotes like "Above Normals…").
        """
        start = None
        end = section_end_row
        for idx in range(section_header_row, section_end_row):
            val = str(df.iloc[idx, date_col]).strip().lower()
            is_data = (
                try_parse_date(df.iloc[idx, date_col]) is not None
                or val in SUMMARY_LABELS
            )
            if is_data and start is None:
                start = idx
            elif not is_data and start is not None:
                # Any non-empty, non-data row ends the range.
                # Blank separator rows (val=="") are skipped silently.
                if val:
                    end = idx
                    break
        return (start if start is not None else section_header_row + 3, end)

    @classmethod
    def _parse_block(
        cls,
        df: pd.DataFrame,
        block: dict,
        source_file: str,
        daily: list,
        summaries: list,
        errors: list,
    ) -> None:
        col_map = block["col_map"]
        month_year = block["month_year"]
        date_col = col_map.get("date")
        if date_col is None:
            errors.append(f"no_date_col_detected:{month_year}")
            return

        seen: set[str] = set()
        for idx in range(block["data_start"], block["data_end"]):
            row = df.iloc[idx]
            first_val = str(row.iloc[date_col]).strip()

            if first_val.lower() in SUMMARY_LABELS:
                s = build_summary(row, col_map, month_year, first_val.lower(), source_file)
                summaries.append(s)
            else:
                if try_parse_date(first_val) is None:
                    if first_val not in ("", "nan"):
                        errors.append(
                            f"date_missing:{month_year},cell={first_val!r}"
                        )
                    continue
                rec = build_record(row, col_map, source_file,
                                   seen_dates=seen, duplicate_log=errors)
                if rec:
                    daily.append(rec)
