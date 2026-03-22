"""
Conflict resolution for overlapping PDF and CSV data.

Both sources cover March 9-17 (possibly 18).  Resolution policy:
  PDF wins — PDFs are single-day primary sources; the CSV is a compiled
  monthly table with documented quality problems.

Both the PDF row and the CSV row are retained in daily_weather with
is_resolved_conflict=TRUE and appropriate parse_flags so the full
audit trail is preserved.

The active_daily_weather VIEW (defined in schema.py) provides the
resolved single-row-per-date dataset for all downstream queries.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from sqlalchemy import select, text, update
from sqlalchemy.engine import Engine

from ingestion.schema import daily_weather

logger = logging.getLogger(__name__)


def resolve_conflicts(engine: Engine) -> list[str]:
    """Find dates where both a PDF row and a CSV row exist; mark both.

    - PDF row gets parse_flag  'conflict_won_over_csv'
    - CSV row gets parse_flag  'conflict_lost_to_pdf'
    - Both rows get is_resolved_conflict = TRUE

    Returns a list of ISO date strings that had conflicts (for logging).
    """
    conflict_dates: list[str] = []

    with engine.begin() as conn:
        # Find dates that have entries from both sources
        pdf_dates_q = select(daily_weather.c.observation_date).where(
            daily_weather.c.data_source == "pdf"
        )
        csv_dates_q = select(daily_weather.c.observation_date).where(
            daily_weather.c.data_source == "csv"
        )

        pdf_dates = {row[0] for row in conn.execute(pdf_dates_q)}
        csv_dates = {row[0] for row in conn.execute(csv_dates_q)}
        overlapping = pdf_dates & csv_dates

        if not overlapping:
            logger.info("No PDF/CSV conflicts found.")
            return []

        for date_str in sorted(overlapping):
            conflict_dates.append(date_str)

            # Mark PDF row
            _add_flag(conn, date_str, "pdf", "conflict_won_over_csv")
            # Mark CSV row
            _add_flag(conn, date_str, "csv", "conflict_lost_to_pdf")

    logger.info(
        "Resolved %d conflicts (PDF wins): %s",
        len(conflict_dates),
        ", ".join(conflict_dates),
    )
    return conflict_dates


def _add_flag(conn, date_str: str, data_source: str, new_flag: str) -> None:
    """Append *new_flag* to the parse_flags JSON array for the given row
    and set is_resolved_conflict = TRUE.

    This issues one SELECT + one UPDATE per (date, source) call.  For the
    current dataset (9 conflict dates × 2 sources = 18 calls) the overhead
    is negligible.  For datasets with thousands of conflict rows, batch the
    flag updates into a single SQL expression instead.
    """
    row = conn.execute(
        select(daily_weather.c.parse_flags).where(
            daily_weather.c.observation_date == date_str,
            daily_weather.c.data_source == data_source,
        )
    ).fetchone()

    if row is None:
        return

    existing = json.loads(row[0] or "[]")
    if new_flag not in existing:
        existing.append(new_flag)

    conn.execute(
        update(daily_weather)
        .where(
            daily_weather.c.observation_date == date_str,
            daily_weather.c.data_source == data_source,
        )
        .values(
            parse_flags=json.dumps(existing),
            is_resolved_conflict=True,
        )
    )


def get_conflicts(engine: Engine) -> list[dict]:
    """Return a list of dicts describing each conflict date.

    Useful for reporting and testing.  Each dict has:
      {'date': '2026-03-09', 'pdf_max': 74.0, 'csv_max': 74.0, 'match': True}
    """
    with engine.connect() as conn:
        pdf_rows = {
            row.observation_date: row
            for row in conn.execute(
                select(daily_weather).where(daily_weather.c.data_source == "pdf")
            ).mappings()
        }
        csv_rows = {
            row.observation_date: row
            for row in conn.execute(
                select(daily_weather).where(daily_weather.c.data_source == "csv")
            ).mappings()
        }

    conflicts = []
    for date_str in sorted(set(pdf_rows) & set(csv_rows)):
        p = pdf_rows[date_str]
        c = csv_rows[date_str]
        conflicts.append(
            {
                "date": date_str,
                "pdf_max": p["temp_max_f"],
                "csv_max": c["temp_max_f"],
                "match": p["temp_max_f"] == c["temp_max_f"],
            }
        )
    return conflicts
