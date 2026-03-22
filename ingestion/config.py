"""
Central configuration for the ingestion pipeline.
All tunables are read from environment variables with sensible defaults.

Override at runtime, e.g.:
    CLAUDE_MODEL=claude-opus-4-6 python -m ingestion.run_ingestion
    WEATHER_DB_PATH=/tmp/test.db pytest tests/
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------

# Model used for PDF structured-data extraction.
# Swap to any Anthropic model without changing parser code.
CLAUDE_MODEL: str = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")

# Maximum tokens Claude may return when extracting a single PDF's data.
CLAUDE_MAX_TOKENS: int = int(os.getenv("CLAUDE_MAX_TOKENS", "1024"))

# Maximum number of concurrent Claude API calls when parsing PDFs.
# Each PDF triggers one API call; these are I/O-bound so parallelism helps.
PDF_MAX_WORKERS: int = int(os.getenv("PDF_MAX_WORKERS", "5"))

# ---------------------------------------------------------------------------
# Paths  (all relative to repo root by default)
# ---------------------------------------------------------------------------

_ROOT = Path(__file__).parent.parent

DB_PATH: str = os.getenv(
    "WEATHER_DB_PATH", str(_ROOT / "db" / "weather.db")
)

PDF_DIR: str = os.getenv(
    "WEATHER_PDF_DIR", str(_ROOT / "data" / "pdf")
)

CSV_DIR: str = os.getenv(
    "WEATHER_CSV_DIR", str(_ROOT / "data" / "csv")
)
