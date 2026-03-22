"""
Weather Chatbot — Downtown Los Angeles, Jan–Mar 2026.

Architecture: Claude API with SQL tool use.
  - Claude receives the database schema in its system prompt.
  - Claude is given a query_weather(sql) tool.
  - When a question arrives, Claude generates a SELECT statement,
    we execute it against db/weather.db, and Claude formats a
    grounded answer from the real rows.

How it connects to the data:
  - All answers are produced by executing live SQL queries against
    the SQLite database built by the ingestion pipeline (Phase 1).
  - Claude never invents numbers — it always calls query_weather
    first and cites values from the result.

Usage:
    python chatbot/weather_chatbot.py

Requirements: ANTHROPIC_API_KEY must be set in .env or the environment.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import anthropic

# ---------------------------------------------------------------------------
# Make the ingestion package importable when run from any working directory
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ingestion import config  # noqa: E402  (import after path fix)

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
# Hardcoded for this fixed dataset. Data quality notes describe known issues
# discovered during ingestion so Claude does not misinterpret NULL values or
# the one erroneous departure figure.
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """
You are a weather data assistant for Downtown Los Angeles.
Your job is to answer questions about weather observations from January 1 to March 18, 2026.

IMPORTANT RULES:
- Always call the query_weather tool to look up data before answering.
- Never guess, estimate, or recall values from training data — every number in your answer must come from a tool result.
- If the data needed to answer a question is NULL or absent, say so clearly.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATABASE SCHEMA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

View: active_daily_weather  ← USE THIS for all daily queries
  observation_date     TEXT  'YYYY-MM-DD'  date of observation
  temp_max_f           REAL  daily high temperature (°F), nullable
  temp_min_f           REAL  daily low temperature (°F), nullable
  temp_avg_f           REAL  daily average temperature (°F), nullable
  temp_departure_f     REAL  daily avg minus historical normal avg (°F), nullable
  heating_degree_days  REAL  HDD for the day, nullable
  cooling_degree_days  REAL  CDD for the day, nullable
  precipitation_in     REAL  precipitation in inches, nullable
  snow_depth_in        REAL  snow depth in inches, nullable
  data_source          TEXT  'pdf' or 'csv'

Table: monthly_summary  ← USE THIS for monthly totals and normals
  month_year           TEXT  'YYYY-MM'  e.g. '2026-01'
  summary_type         TEXT  'sum', 'average', or 'normal'
  temp_max_f           REAL  nullable
  temp_min_f           REAL  nullable
  temp_avg_f           REAL  nullable
  temp_departure_f     REAL  nullable
  heating_degree_days  REAL  nullable
  cooling_degree_days  REAL  nullable
  precipitation_in     REAL  nullable

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATA NOTES (known issues from ingestion)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- Data covers Jan 1 – Mar 31, 2026 in the database, but Mar 19–31 are
  placeholder rows with ALL values NULL (future dates at time of collection).
  Limit queries to observation_date <= '2026-03-18' unless the user asks
  about the full date range.

- "The last 10 days" in this dataset = Mar 9–18, 2026. These rows come
  from daily PDF reports and are the most authoritative source.

- Mar 18 has ALL values NULL (the source PDF reported "M" — missing — for
  every field).

- Mar 13 and Mar 16 have no temp_departure_f (the PDF reports did not
  include enough context to compute it).

- The temp_departure_f value on 2026-02-26 is 45.0, which is a data
  quality error (originated from "45%" in the source CSV). Treat it as
  NULL and do not use it in calculations or comparisons.

- precipitation_in is only available for Mar 9–18 (PDF-sourced rows).
  All Jan and Feb rows have NULL precipitation.

- Historical "normal" values are stored in monthly_summary where
  summary_type = 'normal'. These represent full-month normals, not
  daily normals. Use them for month-level comparisons.
""".strip()

# ---------------------------------------------------------------------------
# Tool definition
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "query_weather",
        "description": (
            "Execute a read-only SQL SELECT query against the weather database "
            "and return the results. Always call this tool before answering any "
            "question that requires data."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A SQL SELECT (or WITH … SELECT) statement.",
                }
            },
            "required": ["sql"],
        },
    }
]

# ---------------------------------------------------------------------------
# Query executor
# ---------------------------------------------------------------------------

def run_query(sql: str) -> str:
    """Execute *sql* against the weather database and return a text table.

    Only SELECT (and WITH … SELECT) statements are permitted.
    Never raises — errors are returned as strings so Claude can report them.
    """
    stripped = sql.strip().upper().lstrip("(")
    if not (stripped.startswith("SELECT") or stripped.startswith("WITH")):
        return "Error: only SELECT queries are permitted."

    try:
        conn = sqlite3.connect(config.DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql).fetchall()
        conn.close()

        if not rows:
            return "Query returned no rows."

        cols = list(rows[0].keys())
        lines = [" | ".join(cols)]
        lines.append("-" * len(lines[0]))
        for row in rows:
            lines.append(
                " | ".join(
                    str(v) if v is not None else "NULL" for v in row
                )
            )
        return "\n".join(lines)

    except Exception as exc:  # noqa: BLE001
        return f"Query error: {exc}"


# ---------------------------------------------------------------------------
# Conversation loop
# ---------------------------------------------------------------------------

def chat() -> None:
    client = anthropic.Anthropic()
    messages: list[dict] = []

    print("━" * 60)
    print("  Weather Chatbot — Downtown Los Angeles, Jan–Mar 2026")
    print("━" * 60)
    print("Ask any weather question. Type 'quit' to exit.\n")

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            print("Goodbye.")
            break

        messages.append({"role": "user", "content": user_input})

        # Inner loop: Claude may call the tool more than once before answering.
        while True:
            with client.messages.stream(
                model=config.CLAUDE_MODEL,
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            ) as stream:
                # Stream the answer token-by-token. Print the "Assistant: "
                # prefix only when the first text token arrives so it doesn't
                # appear on tool-use turns that produce no text output.
                first_token = True
                for text in stream.text_stream:
                    if first_token:
                        print("\nAssistant: ", end="", flush=True)
                        first_token = False
                    print(text, end="", flush=True)

                final = stream.get_final_message()

            if final.stop_reason == "tool_use":
                # Let the user know a database lookup is happening.
                print("\n⏳ Querying database...", flush=True)

                # Collect all tool calls and execute them.
                tool_results = []
                for block in final.content:
                    if block.type == "tool_use":
                        result = run_query(block.input["sql"])
                        tool_results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": result,
                            }
                        )

                # Append assistant turn + tool results, then loop back.
                messages.append({"role": "assistant", "content": final.content})
                messages.append({"role": "user", "content": tool_results})

            elif final.stop_reason == "end_turn":
                print("\n")
                messages.append({"role": "assistant", "content": final.content})
                break

            else:
                print(f"(Unexpected stop reason: {final.stop_reason})")
                break


if __name__ == "__main__":
    chat()
