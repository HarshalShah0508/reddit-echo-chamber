"""NL -> SQL query interface: Gemini writes a read-only DuckDB query against the
Parquet/CSV data using processed/DUCKDB_SCHEMA.md as schema context, then this
script validates and runs it. No local LLM, no fine-tuning -- Gemini generates
the SQL on demand per question.

Usage:
    python -m reddit_kolkata.query_interface "How many posts mention metro per month?"

Requires GEMINI_API_KEY in the environment (project-root .env).
"""
from __future__ import annotations

import argparse
import json
import re
import sys

import time

import duckdb
import httpx
from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types

from .collection_code import config

MODEL = "gemini-flash-lite-latest"

# Gemini's free tier has shown frequent transient 503 "high demand" responses and the
# occasional bare dropped connection (httpx.HTTPError) during testing -- neither is a real
# rejection of the request, so retry a couple of times before surfacing an error.
_RETRYABLE_STATUS = (429, 500, 502, 503, 504)
_MAX_RETRIES = 3
_RETRY_BACKOFF_S = (2, 5, 10)

# "low" omits thinking_config entirely -- gemini-flash-lite-latest 400s on an explicit
# thinking_budget=0 (confirmed empirically), so None must mean "don't send the field" not
# "disable thinking via budget 0".
_EFFORT_THINKING_BUDGET = {
    "low": None,
    "medium": 1024,
    "high": 4096,
    "xhigh": 8192,
    "max": 24576,
}

_FORBIDDEN_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|ATTACH|DETACH|COPY|EXPORT|IMPORT"
    r"|PRAGMA|CALL|INSTALL|LOAD|VACUUM|GRANT|REVOKE|SET|BEGIN|COMMIT|ROLLBACK)\b",
    re.IGNORECASE,
)

SCHEMA_PATH = config.PROCESSED_DIR / "DUCKDB_SCHEMA.md"

_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "sql": {
            "type": "string",
            "description": "A single read-only DuckDB SQL query answering the question.",
        },
        "notes": {
            "type": "string",
            "description": "Caveats the user should know about this query's result. Empty string if none.",
        },
    },
    "required": ["sql", "notes"],
}


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        f"CREATE OR REPLACE VIEW posts AS "
        f"SELECT * FROM read_parquet('{config.POSTS_PARQUET_DIR}/**/*.parquet', hive_partitioning=1)"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW comments AS "
        f"SELECT * FROM read_parquet('{config.COMMENTS_PARQUET_DIR}/**/*.parquet', hive_partitioning=1)"
    )
    for name in ("crossposts", "users", "monthly_activity"):
        csv_path = config.PROCESSED_DIR / f"{name}.csv"
        if csv_path.exists():
            con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_csv_auto('{csv_path}')")
    return con


def _build_system_prompt() -> str:
    schema_doc = SCHEMA_PATH.read_text()
    return (
        "You translate a natural-language question about Reddit Kolkata data into a "
        "single DuckDB SQL query.\n\n"
        "Below is the complete schema reference for the dataset, including known "
        "data-quality caveats. Treat the caveats as binding constraints on what the "
        "data can answer -- do not write a query that silently ignores a documented "
        "gap (e.g. assuming full comment coverage in months marked partial/missing).\n\n"
        f"{schema_doc}\n\n"
        "Rules for the query you write:\n"
        "- Read-only: a single SELECT (or WITH ... SELECT) statement only. Never write "
        "INSERT/UPDATE/DELETE/DROP/ALTER/ATTACH/COPY/PRAGMA or any other statement that "
        "mutates state.\n"
        "- Query only the tables documented above: `posts` and `comments` (DuckDB views "
        "over the Parquet files) and, if relevant, `crossposts`/`users`/`monthly_activity` "
        "(views over the CSV files) -- all five are already registered as views, so refer "
        "to them by name directly.\n"
        "- If the question cannot be answered reliably given the documented caveats, "
        "still write the closest honest query and explain the caveat in `notes`."
    )


def generate_sql(question: str, effort: str = "low") -> tuple[str, str]:
    client = genai.Client()
    budget = _EFFORT_THINKING_BUDGET.get(effort)
    config_obj = genai_types.GenerateContentConfig(
        system_instruction=_build_system_prompt(),
        response_mime_type="application/json",
        response_schema=_RESPONSE_SCHEMA,
        thinking_config=genai_types.ThinkingConfig(thinking_budget=budget) if budget else None,
    )

    response = None
    last_err: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(model=MODEL, contents=question, config=config_obj)
            last_err = None
            break
        except genai_errors.APIError as e:
            last_err = e
            if e.code not in _RETRYABLE_STATUS:
                raise SystemExit(f"Gemini API error ({e.code}): {e}") from e
        except httpx.HTTPError as e:
            last_err = e
        if attempt < _MAX_RETRIES:
            time.sleep(_RETRY_BACKOFF_S[attempt])

    if last_err is not None:
        raise SystemExit(f"Gemini API unreachable after {_MAX_RETRIES + 1} attempts: {last_err}") from last_err

    if not response.candidates:
        reason = getattr(response.prompt_feedback, "block_reason", None)
        raise SystemExit(f"Gemini declined to generate SQL for this question ({reason}).")

    finish_reason = response.candidates[0].finish_reason
    if finish_reason is not None and finish_reason.name not in ("STOP", "MAX_TOKENS"):
        raise SystemExit(f"Gemini declined to generate SQL for this question ({finish_reason.name}).")

    text = response.text
    if not text:
        raise SystemExit("Gemini returned an empty response.")
    payload = json.loads(text)
    return payload["sql"], payload["notes"]


def validate_readonly(sql: str) -> None:
    statements = [s for s in sql.split(";") if s.strip()]
    if len(statements) != 1:
        raise ValueError(f"Expected exactly one statement, got {len(statements)}: {sql!r}")
    stripped = statements[0].strip()
    if not re.match(r"^(SELECT|WITH)\b", stripped, re.IGNORECASE):
        raise ValueError(f"Query must start with SELECT or WITH, got: {stripped[:60]!r}")
    if _FORBIDDEN_KEYWORDS.search(stripped):
        raise ValueError(f"Query contains a disallowed keyword: {stripped!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ask a natural-language question about the Reddit Kolkata dataset.")
    parser.add_argument("question", help="Natural-language question, e.g. 'top 10 posts by score in kolkata'")
    parser.add_argument("--effort", default="low", choices=["low", "medium", "high", "xhigh", "max"])
    parser.add_argument("--sql-only", action="store_true", help="Print the generated SQL without running it.")
    args = parser.parse_args()

    sql, notes = generate_sql(args.question, effort=args.effort)
    validate_readonly(sql)

    print("--- Generated SQL ---")
    print(sql.strip())
    if notes:
        print(f"\n--- Notes ---\n{notes}")

    if args.sql_only:
        return

    con = _connect()
    print("\n--- Result ---")
    try:
        df = con.execute(sql).fetchdf()
    except duckdb.Error as e:
        print(f"Query failed: {e}", file=sys.stderr)
        raise SystemExit(1) from e
    print(df.to_string(index=False) if not df.empty else "(0 rows)")


if __name__ == "__main__":
    main()
