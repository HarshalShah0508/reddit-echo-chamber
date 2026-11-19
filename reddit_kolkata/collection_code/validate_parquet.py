"""Validate the Parquet conversion: row-count parity against processed/*.csv, required-field
nulls, duplicate IDs inside Parquet, invalid timestamps, and missing/incomplete collection
periods (reusing validate.py's collection_state.csv logic). Zero network calls, zero raw
rereads of raw/ -- reads only processed/*.csv and the Parquet output.
"""
from __future__ import annotations

import argparse
import json

import duckdb
import pandas as pd

from . import config, date_utils, jsonl_utils
from .convert_to_parquet import DATASET_CONFIG, dedupe, normalize
from .log_utils import get_logger
from .validate import find_collection_failures, find_missing_periods

logger = get_logger(__name__)

EARLIEST_SANE_DATE = "2025-04-01"  # the project's actual collection start date; update if backfilling earlier history


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
    return con


def _expected_csv_rows(dataset: str) -> int:
    from .convert_to_parquet import load_source

    df = load_source(dataset)
    df = normalize(df, dataset)
    df, _ = dedupe(df, DATASET_CONFIG[dataset]["id_col"])
    df = df[df["date_posted"].notna()] if not df.empty else df
    return len(df)


def check_conversion(con: duckdb.DuckDBPyConnection, dataset: str) -> dict:
    csv_rows = _expected_csv_rows(dataset)
    parquet_rows = con.execute(f"SELECT COUNT(*) FROM {dataset}").fetchone()[0]
    return {"csv_rows": csv_rows, "parquet_rows": parquet_rows, "rows_match": csv_rows == parquet_rows}


def check_integrity(con: duckdb.DuckDBPyConnection, dataset: str, id_col: str) -> dict:
    null_counts = con.execute(
        f"SELECT "
        f"SUM(CASE WHEN {id_col} IS NULL THEN 1 ELSE 0 END) AS null_id, "
        f"SUM(CASE WHEN subreddit IS NULL THEN 1 ELSE 0 END) AS null_subreddit, "
        f"SUM(CASE WHEN date_posted IS NULL THEN 1 ELSE 0 END) AS null_date_posted "
        f"FROM {dataset}"
    ).fetchdf().iloc[0].to_dict()

    duplicate_ids = con.execute(
        f"SELECT {id_col} FROM {dataset} GROUP BY {id_col} HAVING COUNT(*) > 1"
    ).fetchdf()[id_col].tolist()

    out_of_range = con.execute(
        f"SELECT {id_col}, date_posted FROM {dataset} "
        f"WHERE date_posted IS NOT NULL "
        f"AND (date_posted < TIMESTAMP '{EARLIEST_SANE_DATE}' OR date_posted > now())"
    ).fetchdf().to_dict(orient="records")

    return {
        f"null_{id_col}": int(null_counts["null_id"] or 0),
        "null_subreddit": int(null_counts["null_subreddit"] or 0),
        "null_date_posted": int(null_counts["null_date_posted"] or 0),
        f"duplicate_{id_col}s": duplicate_ids,
        "invalid_timestamps": {"out_of_range": out_of_range},
    }


def check_referential_integrity(con: duckdb.DuckDBPyConnection) -> dict:
    orphans = con.execute(
        "SELECT subreddit, COUNT(*) AS n FROM comments c "
        "WHERE NOT EXISTS (SELECT 1 FROM posts p WHERE p.post_id = c.post_id) "
        "GROUP BY subreddit"
    ).fetchdf()
    orphan_by_subreddit = dict(zip(orphans["subreddit"], orphans["n"].astype(int)))

    no_comments = con.execute(
        "SELECT subreddit, COUNT(*) AS n FROM posts p "
        "WHERE NOT EXISTS (SELECT 1 FROM comments c WHERE c.post_id = p.post_id) "
        "GROUP BY subreddit"
    ).fetchdf()
    no_comments_by_subreddit = dict(zip(no_comments["subreddit"], no_comments["n"].astype(int)))

    return {
        "orphaned_comments": {
            "total": sum(orphan_by_subreddit.values()),
            "by_subreddit": orphan_by_subreddit,
        },
        "posts_with_no_collected_comments": {
            "total": sum(no_comments_by_subreddit.values()),
            "by_subreddit": no_comments_by_subreddit,
        },
    }


def check_null_rates(con: duckdb.DuckDBPyConnection, dataset: str) -> dict:
    columns = [row[0] for row in con.execute(f"DESCRIBE {dataset}").fetchall()]
    select_parts = ", ".join(
        f'SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS "{col}__null"' for col in columns
    )
    df = con.execute(
        f'SELECT subreddit, COUNT(*) AS "__n", {select_parts} FROM {dataset} GROUP BY subreddit'
    ).fetchdf()

    result = {}
    for col in columns:
        by_subreddit = {}
        null_total = 0
        row_total = 0
        for _, row in df.iterrows():
            n = int(row["__n"])
            null_n = int(row[f"{col}__null"])
            by_subreddit[row["subreddit"]] = {
                "null_count": null_n,
                "total": n,
                "null_pct": round(null_n / n * 100, 2) if n else 0.0,
            }
            null_total += null_n
            row_total += n
        result[col] = {
            "null_count": null_total,
            "total": row_total,
            "null_pct": round(null_total / row_total * 100, 2) if row_total else 0.0,
            "by_subreddit": by_subreddit,
        }
    return result


def evaluate_report(report: dict) -> list[str]:
    failures = []
    for ds, conv in report["conversion"].items():
        if not conv["rows_match"]:
            failures.append(f"{ds}: row-count mismatch (csv={conv['csv_rows']}, parquet={conv['parquet_rows']})")

    for ds, integ in report["integrity_checks"].items():
        for key, val in integ.items():
            if key.startswith("null_") and val:
                failures.append(f"{ds}: {key} = {val} (expected 0)")
            if key.startswith("duplicate_") and val:
                failures.append(f"{ds}: {len(val)} duplicate {key}")
        if integ["invalid_timestamps"]["out_of_range"]:
            failures.append(f"{ds}: {len(integ['invalid_timestamps']['out_of_range'])} out-of-range timestamp(s)")

    orphaned = report["referential_integrity"]["orphaned_comments"]["total"]
    if orphaned:
        failures.append(f"comments: {orphaned} orphaned row(s) with no matching post_id")

    return failures


def data_quality_notes(con: duckdb.DuckDBPyConnection, missing_periods: dict) -> list[str]:
    notes = []
    null_ratio = con.execute(
        "SELECT subreddit, AVG(CASE WHEN upvote_ratio IS NULL THEN 1.0 ELSE 0.0 END) AS null_rate, COUNT(*) AS n "
        "FROM posts GROUP BY subreddit"
    ).fetchdf()
    for _, row in null_ratio.iterrows():
        if row["null_rate"] > 0:
            notes.append(
                f"{row['subreddit']}: {row['null_rate'] * 100:.1f}% of {int(row['n'])} posts have null "
                f"upvote_ratio -- expected for BrightData-sourced raw records (no upvote_ratio key); "
                f"see README 'Known gaps'."
            )
    for slug, periods in missing_periods.items():
        if periods:
            notes.append(f"{slug}: {len(periods)} (month, data_type) period(s) are not fully collected -- see missing_periods.")
    return notes


def run_validation(start_date, end_date) -> dict:
    con = _connect()
    slugs = sorted(r[0] for r in con.execute("SELECT DISTINCT subreddit FROM posts").fetchall())

    conversion = {ds: check_conversion(con, ds) for ds in ("posts", "comments")}
    integrity = {
        "posts": check_integrity(con, "posts", "post_id"),
        "comments": check_integrity(con, "comments", "comment_id"),
    }
    missing_periods = {slug: find_missing_periods(slug, start_date, end_date) for slug in slugs}

    failures = find_collection_failures()
    collection_failures = failures.to_dict(orient="records") if not failures.empty else []

    referential_integrity = check_referential_integrity(con)
    null_rate_audit = {
        "posts": check_null_rates(con, "posts"),
        "comments": check_null_rates(con, "comments"),
    }

    report = {
        "generated_at": date_utils.iso_now(),
        "conversion": conversion,
        "integrity_checks": integrity,
        "referential_integrity": referential_integrity,
        "null_rate_audit": null_rate_audit,
        "missing_periods": missing_periods,
        "collection_failures": collection_failures,
        "data_quality_notes": data_quality_notes(con, missing_periods),
    }

    jsonl_utils.write_json_atomic(config.PROCESSED_DIR / "parquet_validation_report.json", report)
    print(json.dumps(report, indent=2, default=str))
    return report


def main():
    parser = argparse.ArgumentParser(description="Validate the Parquet conversion against processed/*.csv and collection_state.csv.")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any hard-failure check fails.")
    args = parser.parse_args()

    start = date_utils.parse_date(args.start_date)
    end = date_utils.parse_date(args.end_date) if args.end_date else date_utils.today_utc()
    report = run_validation(start, end)

    if args.strict:
        failures = evaluate_report(report)
        if failures:
            for f in failures:
                print(f"STRICT FAIL: {f}")
            raise SystemExit(1)


if __name__ == "__main__":
    main()
