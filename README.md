# Weather Analyzer

Analysis of past weather in Downtown Los Angeles — data ingestion, exploration, and an AI chatbot grounded in the data.

## Quick start

```bash
# 1. Clone and enter the repo
git clone <repo-url> && cd weather-analyzer

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
.venv\Scripts\activate           # Windows

# 3. Install dependencies
pip install -r requirements.txt
pip install -e .                 # makes ingestion + chatbot packages importable

# 4. Add your Anthropic API key
cp .env.example .env
# Edit .env and set ANTHROPIC_API_KEY=your_key_here

# 5. Run the ingestion pipeline
python -m ingestion.run_ingestion

# 6. Run the tests
pytest tests/ -v
```

## Project structure

```
weather-analyzer/
├── data/
│   ├── csv/
│   │   └── 3month_weather.csv        # Jan–Mar 2026 weather data
│   └── pdf/
│       └── weather_mar09.pdf … 18    # 10 daily weather reports
├── db/
│   └── weather.db                    # generated SQLite database (gitignored)
├── ingestion/
│   ├── config.py                     # tunables — model, paths (env-var overridable)
│   ├── schema.py                     # SQLAlchemy tables + active_daily_weather view
│   ├── models.py                     # Pydantic WeatherRecord / MonthlySummaryRecord
│   ├── csv_parser.py                 # CSV extraction and normalization
│   ├── pdf_parser.py                 # pdfplumber → Claude API → structured data
│   ├── conflict_resolver.py          # PDF wins for overlapping Mar 9-17 dates
│   └── run_ingestion.py              # pipeline entry point
├── analysis/
│   └── weather_analysis.ipynb        # Phase 2 — 5 analyses (Jupyter Notebook)
├── chatbot/
│   └── weather_chatbot.py            # Phase 3 — CLI chatbot (Claude API + SQL tool use)
├── tests/                            # 66 unit tests (no API key required)
├── docs/
│   ├── schema_decisions.md           # schema rationale and data quality catalog
│   └── project-tracker.md           # phase-by-phase progress tracker
├── .env.example                      # environment variable template
├── requirements.txt
└── CLAUDE.md                         # developer quick-reference
```

## Data sources

| File | Period | Notes |
|---|---|---|
| `data/pdf/weather_mar09.pdf` … `mar18.pdf` | Mar 9–18, 2026 | 10 daily reports; each formatted differently |
| `data/csv/3month_weather.csv` | Jan 1 – Mar 18, 2026 | Side-by-side 3-month layout; several data quality issues |

Both sources cover Mar 9–17. The pipeline keeps both rows and uses **PDF as the authoritative source** for that overlap (see [docs/schema_decisions.md](docs/schema_decisions.md)).

## Architecture

### Phase 1 — Data ingestion (complete)

```
PDF files
  └─ pdfplumber  (raw text + table extraction)
       └─ Claude API  (structured JSON — handles any format without hardcoded rules)
            └─ Pydantic  (validates output; bad values → null)
                 └─ SQLite  (daily_weather table)

CSV file
  └─ pandas  (reshape side-by-side layout)
       └─ safe_float()  (coerce bad values to null)
            └─ Pydantic  (validates output)
                 └─ SQLite  (daily_weather + monthly_summary tables)
```

The `active_daily_weather` view provides a conflict-resolved, single-row-per-date dataset for all downstream queries.

### Phase 2 — Analysis (complete)

Jupyter Notebook (`analysis/weather_analysis.ipynb`) with 5 analyses:

| # | Analysis | Stakeholder question |
|---|---|---|
| 1 | 3-month temperature overview (line chart) | Any trends over 3 months? |
| 2 | Hottest days vs. historical normal, Mar 9–18 (bar chart) | Hottest day and how far above normal? |
| 3 | Daily temperature swing high−low, all days (bar chart) | Biggest temperature swing? |
| 4 | Departure from normal, Mar 9–18 (signed bar chart) | How many days warmer than average? |
| 5 | Heating & cooling degree days vs. normal by month (grouped bars) | Records and 3-month energy trends |

**Launch the notebook:**

```bash
jupyter notebook analysis/weather_analysis.ipynb
```

All charts are sourced from `active_daily_weather` and `monthly_summary` in `db/weather.db` — no hardcoded values.

### Phase 3 — AI Chatbot (complete)

Claude API with **SQL tool use**. Every answer is grounded in a live SQL query against `db/weather.db` — Claude never guesses.

```
User question → Claude API → query_weather(sql) tool
  → SQLite executes SELECT → rows returned to Claude
  → Claude formats a natural-language answer
```

**Launch the chatbot:**

```bash
python chatbot/weather_chatbot.py
```

<video src="docs/demo.mp4" controls width="100%"></video>

Try asking any of the 5 stakeholder questions:
1. *What was the hottest day in the last 10 days, and how far above the daily normal was it?*
2. *Which day had the biggest swing between the high and low temperature?*
3. *How many of the last 10 days were warmer than average?*
4. *Did any of the last 10 days set or come close to a record high or low?*
5. *What are the trends over the past 3 months?*

## Configuration

All settings can be overridden via environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY` | *(required)* | Anthropic API key |
| `CLAUDE_MODEL` | `claude-sonnet-4-6` | Model used for PDF extraction |
| `CLAUDE_MAX_TOKENS` | `2048` | Max tokens Claude may return per PDF extraction |
| `PDF_MAX_WORKERS` | `5` | Parallel workers for PDF parsing |
| `WEATHER_DB_PATH` | `db/weather.db` | SQLite database path |
| `WEATHER_PDF_DIR` | `data/pdf` | Directory containing PDF files |
| `WEATHER_CSV_DIR` | `data/csv` | Directory containing CSV files |

## Pipeline options

```bash
# Normal run (idempotent — safe to run multiple times)
python -m ingestion.run_ingestion

# Force re-ingestion from scratch
python -m ingestion.run_ingestion --force-reingest

# Use a different CSV directory
python -m ingestion.run_ingestion --csv-dir data/csv

# Use a different model
CLAUDE_MODEL=claude-opus-4-6 python -m ingestion.run_ingestion

# Run tests (no API key required)
pytest tests/ -v
```
