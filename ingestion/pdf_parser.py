"""
LLM-based PDF parser.

Pipeline for each PDF file:
  1. Extract raw text + table data with pdfplumber
  2. Send combined content to Claude with a structured extraction prompt
  3. Parse Claude's JSON response into a WeatherRecord via Pydantic

Using Claude as the extraction layer means this parser handles arbitrary
PDF layouts without any format-specific rules.  Novel formats (new file
sources, different column names, different units) just work.

If Claude cannot determine a value it returns null; that flows through to
None in the WeatherRecord.  If the API call fails entirely, the parser
falls back to returning a minimal record with observation_date inferred
from the filename and a parse_flag explaining the failure.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Optional

import anthropic
import pdfplumber

from ingestion.config import CLAUDE_MAX_TOKENS, CLAUDE_MODEL
from ingestion.models import WeatherRecord

logger = logging.getLogger(__name__)

# ── Extraction prompt ────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """You are a weather data extraction assistant.
You will receive the raw text and table content extracted from a daily weather PDF report for Downtown Los Angeles.
Your job is to extract the daily observed weather values and return them as a JSON object.

Return ONLY a JSON object with these fields (use null for any field you cannot determine):
{
  "observation_date": "YYYY-MM-DD",
  "temp_max_f": <number or null>,
  "temp_min_f": <number or null>,
  "temp_avg_f": <number or null>,
  "temp_departure_f": <number or null>,
  "heating_degree_days": <number or null>,
  "cooling_degree_days": <number or null>,
  "precipitation_in": <number or null>,
  "snow_depth_in": <number or null>,
  "parse_notes": "<any concerns or caveats about the extraction, or empty string>"
}

Rules:
- Extract DAILY observed values only — not monthly/year-to-date aggregates.
- Strip unit labels (°F, in, etc.) and return bare numbers.
- If a value is "M", "missing", "—", "?", or similar, return null.
- temp_departure_f is the departure of the daily average from the historical normal.
  If not stated directly, compute it as temp_avg_f (observed) minus temp_avg_f (normal) if both are present.
  Otherwise return null.
- For observation_date: the year is 2026 unless clearly stated otherwise.
  Two-digit years like "26" mean 2026.
- Return ONLY the JSON object — no markdown, no explanation.
"""


def _extract_pdf_content(pdf_path: str) -> str:
    """Return a single string combining the table and raw text from page 1."""
    parts: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]

        table = page.extract_table()
        if table:
            table_lines = []
            for row in table:
                if row:
                    table_lines.append(" | ".join(str(c) if c else "" for c in row))
            parts.append("TABLE:\n" + "\n".join(table_lines))

        text = page.extract_text()
        if text:
            parts.append("TEXT:\n" + text)

    return "\n\n".join(parts)


def _date_from_filename(filename: str) -> Optional[str]:
    """Infer the observation date from the filename 'weather_marDD.pdf'.

    Returns ISO date string '2026-03-DD' or None.
    """
    m = re.search(r"weather_mar(\d{1,2})", filename, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        return f"2026-03-{day:02d}"
    return None


def _call_claude(content: str, client: anthropic.Anthropic) -> dict:
    """Call Claude and return the parsed JSON dict.

    Raises ValueError if the response cannot be parsed as JSON.
    """
    message = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=CLAUDE_MAX_TOKENS,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": content}],
    )
    raw = message.content[0].text.strip()

    # Strip markdown code fences if Claude added them
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw.rstrip())

    return json.loads(raw)


def parse_pdf(pdf_path: str, client: Optional[anthropic.Anthropic] = None) -> WeatherRecord:
    """Parse a single PDF file and return a WeatherRecord.

    Args:
        pdf_path: Absolute or relative path to the PDF.
        client:   Optional pre-constructed Anthropic client (useful for batching
                  to reuse the same client across all PDFs).

    Returns:
        WeatherRecord with data_source='pdf'.  All unextractable fields are None.
    """
    source_file = Path(pdf_path).name
    filename_date = _date_from_filename(source_file)
    flags: list[str] = []

    if client is None:
        client = anthropic.Anthropic()

    # Step 1: Extract raw content from PDF
    try:
        content = _extract_pdf_content(pdf_path)
    except Exception as exc:
        logger.error("pdfplumber failed on %s: %s", source_file, exc)
        flags.append(f"pdfplumber_error:{exc}")
        return WeatherRecord(
            observation_date=filename_date or "2026-01-01",
            data_source="pdf",
            source_file=source_file,
            parse_flags=flags + ["extraction_failed"],
        )

    # Step 2: Ask Claude to extract structured data
    try:
        data = _call_claude(content, client)
    except Exception as exc:
        logger.error("Claude API failed on %s: %s", source_file, exc)
        flags.append(f"claude_api_error:{exc}")
        return WeatherRecord(
            observation_date=filename_date or "2026-01-01",
            data_source="pdf",
            source_file=source_file,
            parse_flags=flags + ["claude_extraction_failed"],
        )

    # Step 3: Resolve observation_date
    extracted_date = data.get("observation_date")
    if not extracted_date:
        if filename_date:
            extracted_date = filename_date
            flags.append("date_from_filename_fallback")
        else:
            extracted_date = "2026-01-01"
            flags.append("date_unknown")
    elif filename_date and extracted_date != filename_date:
        # Log discrepancy but trust Claude's extraction over the filename
        flags.append(f"date_filename_mismatch:filename={filename_date},extracted={extracted_date}")

    # Carry over any notes Claude included
    parse_notes = data.get("parse_notes", "")
    if parse_notes:
        flags.append(f"claude_note:{parse_notes}")

    def get(field: str) -> Optional[float]:
        v = data.get(field)
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            flags.append(f"coerce_error:{field}={v!r}")
            return None

    return WeatherRecord(
        observation_date=extracted_date,
        temp_max_f=get("temp_max_f"),
        temp_min_f=get("temp_min_f"),
        temp_avg_f=get("temp_avg_f"),
        temp_departure_f=get("temp_departure_f"),
        heating_degree_days=get("heating_degree_days"),
        cooling_degree_days=get("cooling_degree_days"),
        precipitation_in=get("precipitation_in"),
        snow_depth_in=get("snow_depth_in"),
        data_source="pdf",
        source_file=source_file,
        parse_flags=flags,
    )


def parse_all_pdfs(
    pdf_dir: str,
    client: Optional[anthropic.Anthropic] = None,
) -> tuple[list[WeatherRecord], list[str]]:
    """Parse all PDF files in *pdf_dir*.

    Returns:
        records: list of WeatherRecord (one per PDF)
        errors:  list of error/warning strings for ingestion_log
    """
    if client is None:
        client = anthropic.Anthropic()

    pdf_paths = sorted(Path(pdf_dir).glob("*.pdf"))
    records: list[WeatherRecord] = []
    errors: list[str] = []

    for path in pdf_paths:
        logger.info("Parsing PDF: %s", path.name)
        record = parse_pdf(str(path), client=client)
        records.append(record)
        if record.parse_flags:
            for flag in record.parse_flags:
                if "error" in flag.lower() or "failed" in flag.lower() or "fallback" in flag.lower():
                    errors.append(f"{path.name}:{flag}")

    logger.info("PDF parse complete: %d records, %d errors", len(records), len(errors))
    return records, errors
