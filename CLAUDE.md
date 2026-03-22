# Weather Analyzer — Claude Code Guide

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
.venv\Scripts\activate           # Windows
pip install -r requirements.txt
pip install -e .                 # makes ingestion + chatbot packages importable
```

## Run the ingestion pipeline

```bash
python -m ingestion.run_ingestion
```

Parses all PDFs and the CSV, loads data into `db/weather.db`, resolves conflicts.
The pipeline is idempotent — safe to run multiple times.

```bash
# Re-ingest everything from scratch
python -m ingestion.run_ingestion --force-reingest

# Use a different Claude model for PDF extraction
CLAUDE_MODEL=claude-opus-4-6 python -m ingestion.run_ingestion

# Override paths
python -m ingestion.run_ingestion --db-path /tmp/test.db --pdf-dir data/pdf --csv-dir data/csv
```

## Run the chatbot

```bash
python chatbot/weather_chatbot.py
```

Starts an interactive CLI. Each answer is grounded in a live SQL query against `db/weather.db`. Requires `ANTHROPIC_API_KEY` in `.env` and the database to exist (run ingestion first).

## Run the analysis notebook

```bash
jupyter notebook analysis/weather_analysis.ipynb
```

Opens in browser. Run all cells top-to-bottom (Kernel → Restart & Run All). Requires `db/weather.db` to exist — run the ingestion pipeline first if it doesn't.

## Run tests

```bash
pytest tests/ -v
```

Tests use mocked Claude clients — no API key required to run the test suite.

## Project structure

```
ingestion/
  config.py            # all tunables (model, db path, data dirs) — set via env vars
  schema.py            # SQLAlchemy tables + initialize_db() + active_daily_weather view
  models.py            # Pydantic WeatherRecord / MonthlySummaryRecord
  csv_parser.py        # pandas-based CSV parser
  pdf_parser.py        # pdfplumber + Claude API PDF parser
  conflict_resolver.py # PDF wins over CSV for overlapping dates
  run_ingestion.py     # orchestrator entry point

tests/
  test_csv_parser.py
  test_pdf_parser.py
  test_conflict_resolver.py
  test_chatbot.py
  test_run_ingestion.py

data/
  pdf/                 # 10 daily PDFs (Mar 9-18 2026)
  csv/                 # Jan-Mar 2026 CSV, side-by-side layout

db/
  weather.db           # generated SQLite database (gitignored)

analysis/
  weather_analysis.ipynb  # Phase 2 — 5 analyses (launch with: jupyter notebook)

chatbot/
  weather_chatbot.py      # Phase 3 — CLI chatbot (launch with: python chatbot/weather_chatbot.py)

docs/
  schema_overview.md  # schema design rationale and data quality catalog
  project-tracker.md   # overall project progress tracker

pyproject.toml         # package config — run `pip install -e .` to make ingestion + chatbot importable
```

## Database

```bash
sqlite3 db/weather.db ".tables"
sqlite3 db/weather.db "SELECT observation_date, data_source, temp_max_f FROM active_daily_weather ORDER BY observation_date;"
```

Key tables:
- `daily_weather` — one row per (date, source); both PDF and CSV rows coexist pre-resolution
- `monthly_summary` — Sum/Average/Normal footer rows from the CSV
- `ingestion_log` — audit log of every ingestion run
- `active_daily_weather` (view) — conflict-resolved single row per date; **use this for analysis**

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY` | (required) | Anthropic API key for PDF extraction |
| `CLAUDE_MODEL` | `claude-sonnet-4-6` | Model used for PDF extraction |
| `CLAUDE_MAX_TOKENS` | `2048` | Max tokens Claude may return per PDF extraction |
| `PDF_MAX_WORKERS` | `5` | Parallel workers for PDF parsing |
| `WEATHER_DB_PATH` | `db/weather.db` | SQLite database path |
| `WEATHER_PDF_DIR` | `data/pdf` | Directory containing PDF files |
| `WEATHER_CSV_DIR` | `data/csv` | Directory containing CSV files |
