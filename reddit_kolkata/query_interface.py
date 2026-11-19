"""NL -> SQL query interface: Claude writes a read-only DuckDB query against the
Parquet/CSV data using processed/DUCKDB_SCHEMA.md as schema context, then this
script validates and runs it. No local LLM, no fine-tuning -- Claude generates
the SQL on demand per question.

Usage:
    python -m reddit_kolkata.query_interface "How many posts mention metro per month?"

Requires ANTHROPIC_API_KEY in the environment (or an `ant auth login` profile).
"""
from __future__ import annotations

import argparse
import json
import re
import sys

import anthropic
import duckdb

from .collection_code import config

MODEL = "claude-opus-5"

_FORBIDDEN_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|ATTACH|DETACH|COPY|EXPORT|IMPORT"
    r"|PRAGMA|CALL|INSTALL|LOAD|VACUUM|GRANT|REVOKE|SET|BEGIN|COMMIT|ROLLBACK)\b",
    re.IGNORECASE,
)

SCHEMA_PATH = config.PROCESSED_DIR / "DUCKDB_SCHEMA.md"


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
    client = anthropic.Anthropic()
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=[
                {
                    "type": "text",
                    "text": _build_system_prompt(),
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            output_config={
                "effort": effort,
                "format": {
                    "type": "json_schema",
                    "schema": {
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
                        "additionalProperties": False,
                    },
                },
            },
            messages=[{"role": "user", "content": question}],
        )
    except anthropic.RateLimitError as e:
        raise SystemExit(f"Rate limited: {e}") from e
    except anthropic.APIConnectionError as e:
        raise SystemExit(f"Network error reaching Claude API: {e}") from e
    except anthropic.APIStatusError as e:
        raise SystemExit(f"Claude API error ({e.status_code}): {e.message}") from e

    if response.stop_reason == "refusal":
        raise SystemExit("Claude declined to generate SQL for this question.")

    text = next(block.text for block in response.content if block.type == "text")
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
