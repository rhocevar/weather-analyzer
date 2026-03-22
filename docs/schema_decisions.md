# Schema Decisions & Assumptions

This document describes the database schema design for the Weather Analyzer ingestion pipeline and explains the key decisions made at each step.

---

## 1. Why SQLite?

The dataset is small (~100–200 rows) and self-contained. SQLite is:
- **File-based** — zero infrastructure, no server to run
- **Built into Python's stdlib** — no additional dependencies for basic use
- **Portable** — the entire database is a single file (`db/weather.db`) easy to share

The connection string lives in `ingestion/config.py`.  Switching to PostgreSQL requires changing exactly one line — SQLAlchemy abstracts the rest.

---

## 2. One Row = One Day Per Source

The original CSV uses a side-by-side display format (January, February, and March laid out horizontally in the same rows). This is a **presentation format**, not a data model. The schema stores one row per day per source file, making every query trivial and enabling correct date indexing.

---

## 3. All Numeric Fields Are `REAL` (nullable)

Bad source values — `"M"`, `"ERROR"`, `"NO WEATHER"`, `"-"`, blank — are coerced to `NULL` at parse time. Storing them as strings (e.g. storing `"ERROR"` literally) would require every downstream query to special-case those strings. `NULL` is the correct relational representation of missing data.

---

## 4. `(observation_date, data_source)` Unique Constraint

Both a PDF row and a CSV row may exist for the same date (March 9–17 is covered by both sources). The unique constraint allows **both to coexist** in the table, preserving the full audit trail. The `active_daily_weather` VIEW resolves conflicts at query time.

---

## 5. Conflict Resolution: PDF Wins

When the same date appears in both a PDF and the CSV:

- **PDF rows are authoritative.** PDFs represent individual daily weather reports produced as primary sources on that specific day.
- **The CSV is a compiled monthly table** with documented quality problems (duplicate rows, `ERROR` placeholders, `"45%"` in a numeric field, `"NO WEATHER"`, `"M"` sentinels).

Both rows are retained in `daily_weather` with `is_resolved_conflict = TRUE` and appropriate `parse_flags` (`conflict_won_over_csv` / `conflict_lost_to_pdf`). No data is discarded — the audit trail is complete.

The `active_daily_weather` VIEW surfaces the resolved perspective:

```sql
CREATE VIEW active_daily_weather AS
SELECT * FROM daily_weather WHERE data_source = 'pdf'
UNION ALL
SELECT * FROM daily_weather
WHERE data_source = 'csv'
  AND observation_date NOT IN (
      SELECT observation_date FROM daily_weather WHERE data_source = 'pdf'
  );
```

Phase 2 analysis and Phase 3 chatbot queries exclusively target this view.

---

## 6. `monthly_summary` Table

The CSV contains `Sum`, `Average`, and `Normal` footer rows at the bottom of each month. These are **aggregates**, not daily observations. They must not pollute `daily_weather` or distort averages. They are stored in a separate `monthly_summary` table with a `summary_type` column (`sum`, `average`, `normal`).

---

## 7. `parse_flags` as a JSON Array

Each row has a `parse_flags` column storing a JSON array of strings describing any data quality issues encountered during parsing. Examples:

- `"percent_sign_stripped:temp_departure_f=45"` — the `45%` value in 2/26/26's departure column
- `"conflict_lost_to_pdf"` — this CSV row was overridden by a PDF row for the same date
- `"date_from_filename_fallback"` — the PDF's date was inferred from the filename, not the document body
- `"claude_note:All observations missing (M)"` — Claude noted an issue during extraction

Using a JSON array avoids needing a separate `flags` table while keeping all quality metadata attached to the row. The trade-off (flags are not SQL-indexable) is acceptable at this dataset size.

---

## 8. `snow_depth_in` Is Always Present

Rather than conditionally omitting the snow depth column for January and February (where the original CSV has no snow depth data), the column exists in every row and is `NULL` for those months. This is simpler than a schema migration if snow data for other months is added later.

---

## 9. `ingested_at` and `ingestion_log`

- `ingested_at` (on each data row) records the UTC timestamp of insertion. If you re-run the pipeline after a bug fix, you can distinguish old vs new records by timestamp.
- `ingestion_log` records a summary of each ingestion run (per source file): rows parsed, inserted, skipped, and any error messages. It answers "what happened last time?" without digging through stdout logs and supports idempotency checks (skip files already successfully ingested).

---

## 10. LLM-Based PDF Parsing (Extensibility)

The 10 PDF files use 10 different formats — different column names, different units, different layouts, different sections. Rather than hardcoding rules for each format, the PDF parser uses the **Claude API** as the extraction layer:

1. `pdfplumber` extracts raw text and table data from the PDF (no format assumptions)
2. Claude receives the raw content and a structured extraction prompt
3. Claude returns a JSON object with the target fields
4. Pydantic validates the response and coerces bad values to `None`

**Why this is more extensible than rule-based parsing:** Adding a new PDF format requires no code changes — just feed the new PDF to the same pipeline. Novel inconsistencies that Claude can't interpret become `null` rather than crashing the pipeline.

The Claude model is configurable via the `CLAUDE_MODEL` environment variable (default: `claude-sonnet-4-6`). No parser code changes are needed to switch models.

---

## 11. Chatbot System Prompt — Data Quality Notes

The Phase 3 chatbot (`chatbot/weather_chatbot.py`) passes a `SYSTEM_PROMPT` to Claude on every request. That prompt includes a set of data quality notes telling Claude how to interpret NULLs and known bad values. These notes are **hardcoded as a string constant** in the file, not derived from the database at runtime.

**Why hardcoded?**
The dataset is fixed. Deriving structural facts (date range, null patterns) from the database at startup would add code complexity without improving correctness for this static dataset. More importantly, notes that require human judgment — such as "the value 45.0 on 2026-02-26 is a data error, not a real departure" — cannot be auto-detected from the data alone regardless of how sophisticated the derivation logic is.

**Where each note comes from:**

| Note in system prompt | Origin |
|---|---|
| Feb 26 `temp_departure_f = 45.0` is a data error | `parse_flags` on that row: `percent_sign_stripped:temp_departure_f=45` (source CSV had `"45%"`) |
| Mar 13 / Mar 16 have no departure | `parse_flags`: `claude_note: temp_departure_f could not be computed` — PDF reports lacked normal temperature context |
| Mar 18 all NULL | `parse_flags`: `claude_note: All daily observations are missing (M)` — source PDF had `"M"` for every field |
| Precipitation NULL for Jan/Feb | That column was absent from the 3-month CSV entirely |
| Mar 19–31 are placeholder NULLs | CSV had `"M"` for every field on future dates; `safe_float("M")` → `NULL` |
| Last 10 days = Mar 9–18 | The 10 PDF source files cover exactly those dates |

All of the above are also visible at row level in the `parse_flags` column of `daily_weather` and in the Data Quality Catalog below.

---

## Data Quality Catalog

| Source | Issue | Handling |
|---|---|---|
| CSV | Side-by-side 3-month layout | Block detection: scan for month label, slice columns per block, reshape to tidy one-row-per-day format |
| CSV | `"M"` in all fields (Mar 19–31) | `safe_float("M")` → `NULL` |
| CSV | `"ERROR"` in 2/23/26 max temp | `safe_float("ERROR")` → `NULL` |
| CSV | `"NO WEATHER"` in 3/18/26 | `safe_float("NO WEATHER")` → `NULL` |
| CSV | `"45%"` in 2/26/26 departure | Strip `%`, store `45.0`, flag `percent_sign_stripped` |
| CSV | Duplicate rows for 2/17–2/22 | First occurrence wins; duplicate logged in `ingestion_log` errors |
| CSV | Missing Feb date on Jan 1/3 row | Feb row skipped, `feb_date_missing` logged |
| CSV | Feb dates 2/3–2/9 absent | Simply missing from source; no rows created |
| CSV | `Sum`/`Average`/`Normal` footer rows | Routed to `monthly_summary`, not `daily_weather` |
| PDF | 10 different formats | Claude handles all; no per-format code |
| PDF | Date absent from PDF body | Fallback to filename (e.g. `weather_mar09.pdf`); `date_from_filename_fallback` flagged |
| PDF | `"M"` observed values (Mar 18) | Claude returns `null`; all fields become `NULL` |
| Both | Same date in both sources (Mar 9–17) | PDF wins; both rows retained; conflict flags set |
