"""
SQLAlchemy Core table definitions and database initialisation.

All other modules call get_engine() and reference the Table objects
exported here.  Switching from SQLite to PostgreSQL requires only
changing the connection string passed to get_engine().
"""

from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine

metadata = MetaData()

# ---------------------------------------------------------------------------
# daily_weather
# ---------------------------------------------------------------------------
# One row per (observation_date, data_source).  Both a PDF row and a CSV
# row may exist for the same date — the active_daily_weather VIEW resolves
# conflicts so that PDF wins.
# ---------------------------------------------------------------------------

daily_weather = Table(
    "daily_weather",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    # DATE stored as TEXT in SQLite ISO format 'YYYY-MM-DD'
    Column("observation_date", String(10), nullable=False, index=True),
    # All weather values are nullable; bad source values become NULL
    Column("temp_max_f", Float),
    Column("temp_min_f", Float),
    Column("temp_avg_f", Float),
    Column("temp_departure_f", Float),     # deviation from historical normal
    Column("heating_degree_days", Float),
    Column("cooling_degree_days", Float),
    Column("precipitation_in", Float),
    Column("snow_depth_in", Float),        # NULL for months without snow data
    # Provenance
    Column("data_source", String(3), nullable=False),   # 'pdf' or 'csv'
    Column("source_file", Text, nullable=False),        # original filename
    # Conflict tracking
    Column("is_resolved_conflict", Boolean, nullable=False, default=False),
    Column("parse_flags", Text),           # JSON array of warning strings
    Column("ingested_at", DateTime, nullable=False),
    UniqueConstraint("observation_date", "data_source", name="uq_date_source"),
)

# ---------------------------------------------------------------------------
# monthly_summary
# ---------------------------------------------------------------------------
# Sum / Average / Normal footer rows from the CSV.
# These are aggregates, not daily observations, and must not pollute daily_weather.
# ---------------------------------------------------------------------------

monthly_summary = Table(
    "monthly_summary",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("month_year", String(7), nullable=False),  # e.g. '2026-01'
    Column("summary_type", String(10), nullable=False),  # 'sum', 'average', 'normal'
    Column("temp_max_f", Float),
    Column("temp_min_f", Float),
    Column("temp_avg_f", Float),
    Column("temp_departure_f", Float),
    Column("heating_degree_days", Float),
    Column("cooling_degree_days", Float),
    Column("precipitation_in", Float),
    Column("source_file", Text, nullable=False),
    Column("ingested_at", DateTime, nullable=False),
    UniqueConstraint("month_year", "summary_type", name="uq_month_summary"),
)

# ---------------------------------------------------------------------------
# ingestion_log
# ---------------------------------------------------------------------------
# One row per source file per ingestion run.  Answers "what happened last
# time?" without digging through stdout logs.
# ---------------------------------------------------------------------------

ingestion_log = Table(
    "ingestion_log",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(36), nullable=False),  # UUID
    Column("source_file", Text, nullable=False),
    Column("rows_parsed", Integer, nullable=False, default=0),
    Column("rows_inserted", Integer, nullable=False, default=0),
    Column("rows_skipped", Integer, nullable=False, default=0),
    Column("errors", Text),   # JSON array of error strings
    Column("run_at", DateTime, nullable=False),
)


# ---------------------------------------------------------------------------
# active_daily_weather VIEW
# ---------------------------------------------------------------------------
_ACTIVE_VIEW_DDL = """
CREATE VIEW IF NOT EXISTS active_daily_weather AS
SELECT * FROM daily_weather
WHERE data_source = 'pdf'
UNION ALL
SELECT * FROM daily_weather
WHERE data_source = 'csv'
  AND observation_date NOT IN (
      SELECT observation_date FROM daily_weather WHERE data_source = 'pdf'
  )
"""


def get_engine(db_path: str | None = None) -> Engine:
    """Return a SQLAlchemy engine for the given SQLite database path.

    Creates the parent directory if it does not exist.
    """
    if db_path is None:
        from ingestion.config import DB_PATH
        db_path = DB_PATH

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{db_path}", future=True)


def initialize_db(db_path: str | None = None) -> Engine:
    """Create all tables (if not exist) and the active_daily_weather view.

    Safe to call on every pipeline run — idempotent.
    Returns the engine for immediate use.
    """
    engine = get_engine(db_path)
    metadata.create_all(engine)

    with engine.connect() as conn:
        conn.execute(text(_ACTIVE_VIEW_DDL))
        conn.commit()

    return engine
