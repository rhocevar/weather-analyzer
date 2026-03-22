"""
Pydantic models — shared output contract for all parsers.

Both csv_parser and pdf_parser must return lists of WeatherRecord
(or MonthlySummaryRecord).  This single definition:
  - enforces types so bad values become None rather than crashing the pipeline
  - decouples the parsers from the SQLAlchemy schema
  - makes it easy to add new fields without changing parser logic
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, field_validator, model_validator


class WeatherRecord(BaseModel):
    """One day of weather observations from a single source file."""

    observation_date: date
    temp_max_f: Optional[float] = None
    temp_min_f: Optional[float] = None
    temp_avg_f: Optional[float] = None
    temp_departure_f: Optional[float] = None    # deviation from historical normal
    heating_degree_days: Optional[float] = None
    cooling_degree_days: Optional[float] = None
    precipitation_in: Optional[float] = None
    snow_depth_in: Optional[float] = None       # NULL for months without snow data

    data_source: Literal["pdf", "csv"]
    source_file: str                            # filename only, e.g. 'weather_mar09.pdf'
    parse_flags: list[str] = []
    ingested_at: datetime = None  # type: ignore[assignment]

    @model_validator(mode="before")
    @classmethod
    def set_ingested_at(cls, values: dict) -> dict:
        if not values.get("ingested_at"):
            values["ingested_at"] = datetime.now(timezone.utc)
        return values

    def to_db_dict(self) -> dict:
        """Return a flat dict ready for SQLAlchemy Core insert into daily_weather."""
        import json

        return {
            "observation_date": self.observation_date.isoformat(),
            "temp_max_f": self.temp_max_f,
            "temp_min_f": self.temp_min_f,
            "temp_avg_f": self.temp_avg_f,
            "temp_departure_f": self.temp_departure_f,
            "heating_degree_days": self.heating_degree_days,
            "cooling_degree_days": self.cooling_degree_days,
            "precipitation_in": self.precipitation_in,
            "snow_depth_in": self.snow_depth_in,
            "data_source": self.data_source,
            "source_file": self.source_file,
            "is_resolved_conflict": False,
            "parse_flags": json.dumps(self.parse_flags) if self.parse_flags else "[]",
            "ingested_at": self.ingested_at.replace(tzinfo=None),  # SQLite stores naive
        }


class MonthlySummaryRecord(BaseModel):
    """Sum / Average / Normal footer row from the CSV.

    These are aggregates, not daily observations.
    """

    month_year: str                             # e.g. '2026-01'
    summary_type: Literal["sum", "average", "normal"]
    temp_max_f: Optional[float] = None
    temp_min_f: Optional[float] = None
    temp_avg_f: Optional[float] = None
    temp_departure_f: Optional[float] = None
    heating_degree_days: Optional[float] = None
    cooling_degree_days: Optional[float] = None
    precipitation_in: Optional[float] = None
    source_file: str
    ingested_at: datetime = None  # type: ignore[assignment]

    @model_validator(mode="before")
    @classmethod
    def set_ingested_at(cls, values: dict) -> dict:
        if not values.get("ingested_at"):
            values["ingested_at"] = datetime.now(timezone.utc)
        return values

    def to_db_dict(self) -> dict:
        return {
            "month_year": self.month_year,
            "summary_type": self.summary_type,
            "temp_max_f": self.temp_max_f,
            "temp_min_f": self.temp_min_f,
            "temp_avg_f": self.temp_avg_f,
            "temp_departure_f": self.temp_departure_f,
            "heating_degree_days": self.heating_degree_days,
            "cooling_degree_days": self.cooling_degree_days,
            "precipitation_in": self.precipitation_in,
            "source_file": self.source_file,
            "ingested_at": self.ingested_at.replace(tzinfo=None),
        }
