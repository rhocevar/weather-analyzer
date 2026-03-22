# Weather Analyzer — Project Tracker

## Overview

Take-home assessment for Apple. Analyze past weather in Downtown LA across three phases:
- **Phase 1** — Data Ingestion & Storage
- **Phase 2** — Data Analysis (Jupyter Notebook)
- **Phase 3** — AI Chatbot

---

## Tech Stack

| Layer | Choice | Rationale |
|---|---|---|
| Language | Python 3.11+ | Specified by user |
| Database | SQLite + SQLAlchemy (Core) | File-based, zero infrastructure; connection string swap to PostgreSQL if needed |
| PDF extraction | pdfplumber + Claude API | pdfplumber gets raw text; Claude extracts structured JSON — handles any format without hardcoded rules |
| CSV parsing | pandas | Known (if messy) format; rule-based reshape is appropriate |
| Validation | Pydantic | Shared `WeatherRecord` output contract for both parsers; catches unexpected values gracefully |
| LLM | Anthropic Claude (`claude-sonnet-4-6`) | Configurable via `CLAUDE_MODEL` env var |
| Analysis | Jupyter Notebook + pandas + matplotlib/seaborn | Phase 2 |
| Chatbot | Claude API with SQL tool use | Phase 3 — LLM generates SQL queries against SQLite; no vector DB needed |

---

## Data Sources

| File | Coverage | Notes |
|---|---|---|
| `data/pdf/weather_mar09.pdf` … `weather_mar18.pdf` | Mar 9–18, 2026 (10 days) | Each PDF may be formatted differently |
| `data/csv/3month_weather.csv` | Jan 1 – Mar ~17, 2026 | Side-by-side 3-month layout; numerous data quality issues |

**Overlap:** Both sources cover Mar 9–17. **Resolution: PDF wins.** Both rows retained in DB for audit; `active_daily_weather` view surfaces the resolved dataset.

---

## Database Schema

### `daily_weather`
One row per day per source. Unique on `(observation_date, data_source)`.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `observation_date` | DATE | ISO `YYYY-MM-DD` |
| `temp_max_f` | REAL | nullable |
| `temp_min_f` | REAL | nullable |
| `temp_avg_f` | REAL | nullable |
| `temp_departure_f` | REAL | nullable — deviation from historical normal |
| `heating_degree_days` | REAL | nullable |
| `cooling_degree_days` | REAL | nullable |
| `precipitation_in` | REAL | nullable |
| `snow_depth_in` | REAL | nullable — NULL for Jan/Feb |
| `data_source` | TEXT | `'pdf'` or `'csv'` |
| `source_file` | TEXT | original filename |
| `is_resolved_conflict` | BOOLEAN | TRUE if row was part of a PDF/CSV conflict |
| `parse_flags` | TEXT | JSON array of data-quality warnings |
| `ingested_at` | TIMESTAMP | UTC |

### `monthly_summary`
Sum / Average / Normal footer rows from the CSV. Kept separate from daily observations.

### `ingestion_log`
One row per file per ingestion run. Tracks rows parsed / inserted / skipped and any errors.

### View: `active_daily_weather`
PDF wins for conflict dates; CSV fills in all other dates. **This is what Phase 2 and Phase 3 query.**

---

## Known Data Quality Issues

| Source | Issue | Handling |
|---|---|---|
| CSV | Side-by-side 3-month layout | Block detection + reshape |
| CSV | `"M"`, `"ERROR"`, `"NO WEATHER"` in numeric fields | → NULL + flag |
| CSV | `"45%"` in departure column | Strip `%`, store 45.0, flag |
| CSV | Duplicate rows for 2/17–2/22 | First occurrence wins; duplicate logged |
| CSV | Sum/Average/Normal footer rows | Route to `monthly_summary` |
| PDF | Varying formats across 10 files | Claude extracts regardless of format |
| Both | Same date in both sources (Mar 9–17) | PDF wins; both rows retained |

---

## Phase 1 Progress

| Step | Description | Status |
|---|---|---|
| 1 | Requirements, config, project skeleton, project-tracker.md | ✅ Done |
| 2 | SQLAlchemy schema + db init (`ingestion/schema.py`) | ✅ Done |
| 3 | Pydantic models — shared output contract (`ingestion/models.py`) | ✅ Done |
| 4 | CSV parser (`ingestion/csv_parser.py`) | ✅ Done |
| 5 | PDF parser — LLM-based (`ingestion/pdf_parser.py`) | ✅ Done |
| 6 | Conflict resolver + view (`ingestion/conflict_resolver.py`) | ✅ Done |
| 7 | Ingestion orchestrator (`ingestion/run_ingestion.py`) | ✅ Done |
| 8 | Unit tests (`tests/`) — 66/66 passing | ✅ Done |
| 9 | Schema decisions doc (`docs/schema_decisions.md`) | ✅ Done |
| 10 | `CLAUDE.md` | ✅ Done |

---

## Future Improvements

| Area | Idea | Notes |
|---|---|---|
| PDF ingestion | Parallel Claude API calls | PDFs are currently parsed sequentially. Since each file is independent, requests could be sent concurrently (e.g. `concurrent.futures.ThreadPoolExecutor`) to reduce total ingestion time from ~O(n) API round trips to ~O(1). Worth doing if the PDF corpus grows significantly. |

---

## Phase 2 Progress

| Step | Description | Status |
|---|---|---|
| 1 | Add notebook dependencies to `requirements.txt` (`matplotlib`, `seaborn`, `jupyter`, `ipykernel`) | ✅ Done |
| 2 | Create `analysis/weather_analysis.ipynb` with 5 analyses | ✅ Done |
| 3 | Update `README.md`, `CLAUDE.md` with notebook instructions | ✅ Done |

---

## Phase 3 Progress

| Step | Description | Status |
|---|---|---|
| 1 | Create `chatbot/weather_chatbot.py` — system prompt, SQL tool, conversation loop | ✅ Done |
| 2 | Update `README.md`, `CLAUDE.md` with chatbot instructions | ✅ Done |

### Chatbot Architecture (planned)

Natural language queries are answered via **SQL tool use**, not RAG/vector search:

1. Claude receives the `daily_weather` schema in its system prompt
2. Claude is given a `query_weather(sql)` tool
3. User asks a question → Claude generates SQL → we execute → Claude formats the answer

This produces exact, grounded answers from actual database values.
