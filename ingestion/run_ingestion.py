"""
Ingestion orchestrator — entry point for the data pipeline.

Usage:
    python -m ingestion.run_ingestion
    python -m ingestion.run_ingestion --db-path db/weather.db --force-reingest
    CLAUDE_MODEL=claude-opus-4-6 python -m ingestion.run_ingestion

Pipeline:
  1. Initialize the SQLite database (create tables/view if not exist)
  2. Parse the CSV → insert daily rows + monthly summaries
  3. Parse each PDF via Claude API → insert daily rows
  4. Resolve PDF/CSV conflicts
  5. Print a summary report

The pipeline is idempotent: running it twice produces the same result.
Files already ingested (per ingestion_log) are skipped unless --force-reingest.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import anthropic
from sqlalchemy import insert, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ingestion import config
from ingestion.conflict_resolver import get_conflicts, resolve_conflicts
from ingestion.csv_parser import parse_csv, parse_csv_dir
from ingestion.models import MonthlySummaryRecord, WeatherRecord
from ingestion.pdf_parser import parse_pdf
from ingestion.schema import (
    daily_weather,
    ingestion_log,
    initialize_db,
    monthly_summary,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Database helpers ─────────────────────────────────────────────────────────

def _already_ingested(engine, source_file: str) -> bool:
    """Return True if source_file was successfully ingested in a prior run."""
    with engine.connect() as conn:
        row = conn.execute(
            select(ingestion_log.c.id)
            .where(ingestion_log.c.source_file == source_file)
            .limit(1)
        ).fetchone()
    return row is not None


def _upsert_daily_records(engine, records: list[WeatherRecord]) -> tuple[int, int]:
    """Insert records into daily_weather, skipping duplicates.

    Returns (inserted, skipped).
    """
    inserted = skipped = 0
    with engine.begin() as conn:
        for rec in records:
            stmt = (
                sqlite_insert(daily_weather)
                .values(**rec.to_db_dict())
                .prefix_with("OR IGNORE")
            )
            result = conn.execute(stmt)
            if result.rowcount:
                inserted += 1
            else:
                skipped += 1
    return inserted, skipped


def _upsert_summary_records(engine, records: list[MonthlySummaryRecord]) -> tuple[int, int]:
    """Insert records into monthly_summary, skipping duplicates."""
    inserted = skipped = 0
    with engine.begin() as conn:
        for rec in records:
            stmt = (
                sqlite_insert(monthly_summary)
                .values(**rec.to_db_dict())
                .prefix_with("OR IGNORE")
            )
            result = conn.execute(stmt)
            if result.rowcount:
                inserted += 1
            else:
                skipped += 1
    return inserted, skipped


def _log_run(
    engine,
    run_id: str,
    source_file: str,
    rows_parsed: int,
    rows_inserted: int,
    rows_skipped: int,
    errors: list[str],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            insert(ingestion_log).values(
                run_id=run_id,
                source_file=source_file,
                rows_parsed=rows_parsed,
                rows_inserted=rows_inserted,
                rows_skipped=rows_skipped,
                errors=json.dumps(errors),
                run_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
        )


def _truncate_data_tables(engine) -> None:
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM daily_weather"))
        conn.execute(text("DELETE FROM monthly_summary"))
        conn.execute(text("DELETE FROM ingestion_log"))
    logger.info("Truncated daily_weather, monthly_summary, and ingestion_log for re-ingestion.")


# ── Main pipeline ────────────────────────────────────────────────────────────

def run(
    db_path: str = config.DB_PATH,
    pdf_dir: str = config.PDF_DIR,
    csv_dir: str = config.CSV_DIR,
    force_reingest: bool = False,
    model: str = config.CLAUDE_MODEL,
) -> None:
    config.CLAUDE_MODEL = model  # propagates to pdf_parser at call time
    run_id = str(uuid.uuid4())
    logger.info("Starting ingestion run %s", run_id)
    logger.info("DB: %s | PDFs: %s | CSVs: %s | model: %s", db_path, pdf_dir, csv_dir, model)

    # Step 1: Initialize DB
    engine = initialize_db(db_path)
    logger.info("Database ready.")

    if force_reingest:
        _truncate_data_tables(engine)

    # Step 2: CSVs
    csv_paths = sorted(Path(csv_dir).glob("*.csv"))
    if not csv_paths:
        logger.warning("No CSV files found in %s", csv_dir)
    for csv_path in csv_paths:
        csv_source = csv_path.name
        if not force_reingest and _already_ingested(engine, csv_source):
            logger.info("CSV already ingested — skipping: %s", csv_source)
            continue
        logger.info("Parsing CSV: %s", csv_source)
        daily_recs, summary_recs, csv_errors = parse_csv(str(csv_path))
        ins_d, skip_d = _upsert_daily_records(engine, daily_recs)
        ins_s, skip_s = _upsert_summary_records(engine, summary_recs)
        _log_run(
            engine, run_id, csv_source,
            rows_parsed=len(daily_recs) + len(summary_recs),
            rows_inserted=ins_d + ins_s,
            rows_skipped=skip_d + skip_s,
            errors=csv_errors,
        )
        logger.info(
            "CSV %s: %d daily inserted, %d skipped | %d summaries inserted | %d warnings",
            csv_source, ins_d, skip_d, ins_s, len(csv_errors),
        )

    # Step 3: PDFs
    pdf_paths = sorted(Path(pdf_dir).glob("*.pdf"))
    if not pdf_paths:
        logger.warning("No PDF files found in %s", pdf_dir)
    else:
        from concurrent.futures import ThreadPoolExecutor

        # Filter out already-ingested PDFs before submitting any work.
        pending = [
            p for p in pdf_paths
            if force_reingest or not _already_ingested(engine, p.name)
        ]
        skipped_pdfs = len(pdf_paths) - len(pending)
        if skipped_pdfs:
            logger.info("Skipping %d already-ingested PDF(s).", skipped_pdfs)

        if pending:
            anthropic_client = anthropic.Anthropic()
            logger.info(
                "Parsing %d PDF(s) with up to %d workers…",
                len(pending), config.PDF_MAX_WORKERS,
            )

            # Parse all PDFs in parallel (I/O-bound — Claude API round trips).
            # DB inserts happen serially on the main thread afterward to avoid
            # SQLite write contention.
            futures = {}
            with ThreadPoolExecutor(max_workers=config.PDF_MAX_WORKERS) as executor:
                for pdf_path in pending:
                    future = executor.submit(parse_pdf, str(pdf_path), anthropic_client)
                    futures[future] = pdf_path.name

            # Collect results and insert serially.
            for future, pdf_source in futures.items():
                try:
                    record = future.result()
                except Exception as exc:
                    logger.error("PDF %s failed: %s", pdf_source, exc)
                    _log_run(
                        engine, run_id, pdf_source,
                        rows_parsed=0, rows_inserted=0, rows_skipped=0,
                        errors=[f"parse_failed:{exc}"],
                    )
                    continue
                ins, skip = _upsert_daily_records(engine, [record])
                pdf_errors = [
                    f"{pdf_source}:{f}"
                    for f in record.parse_flags
                    if "error" in f.lower() or "failed" in f.lower()
                ]
                _log_run(
                    engine, run_id, pdf_source,
                    rows_parsed=1,
                    rows_inserted=ins,
                    rows_skipped=skip,
                    errors=pdf_errors,
                )
                logger.info(
                    "PDF %s: date=%s max=%s min=%s flags=%s",
                    pdf_source, record.observation_date,
                    record.temp_max_f, record.temp_min_f, record.parse_flags,
                )

    # Step 4: Conflict resolution
    logger.info("Resolving PDF/CSV conflicts...")
    conflicts = resolve_conflicts(engine)
    if conflicts:
        logger.info("Conflicts resolved (PDF wins): %s", ", ".join(conflicts))
    else:
        logger.info("No conflicts found.")

    # Step 5: Summary report
    _print_summary(engine, conflicts)


def _print_summary(engine, conflicts: list[str]) -> None:
    from sqlalchemy import func, text

    with engine.connect() as conn:
        total_daily = conn.execute(
            select(func.count()).select_from(daily_weather)
        ).scalar()
        total_pdf = conn.execute(
            select(func.count()).select_from(daily_weather).where(
                daily_weather.c.data_source == "pdf"
            )
        ).scalar()
        total_csv = conn.execute(
            select(func.count()).select_from(daily_weather).where(
                daily_weather.c.data_source == "csv"
            )
        ).scalar()
        active_count = conn.execute(
            text("SELECT COUNT(*) FROM active_daily_weather")
        ).scalar()

    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)
    print(f"  daily_weather rows : {total_daily} ({total_pdf} PDF + {total_csv} CSV)")
    print(f"  active_daily_weather (resolved): {active_count} unique dates")
    print(f"  conflicts resolved : {len(conflicts)}" + (f" ({', '.join(conflicts)})" if conflicts else ""))
    print("=" * 60 + "\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest weather PDFs and CSV into SQLite."
    )
    parser.add_argument("--db-path", default=config.DB_PATH, help="Path to SQLite database file")
    parser.add_argument("--pdf-dir", default=config.PDF_DIR, help="Directory containing weather PDFs")
    parser.add_argument("--csv-dir", default=config.CSV_DIR, help="Directory containing weather CSV files")
    parser.add_argument("--model", default=config.CLAUDE_MODEL, help="Claude model used for PDF extraction")
    parser.add_argument(
        "--force-reingest",
        action="store_true",
        help="Truncate existing data and re-ingest all sources",
    )
    args = parser.parse_args()

    run(
        db_path=args.db_path,
        pdf_dir=args.pdf_dir,
        csv_dir=args.csv_dir,
        force_reingest=args.force_reingest,
        model=args.model,
    )


if __name__ == "__main__":
    main()
