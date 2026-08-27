"""Basic validation per spec section 8. Reads only processed/*.csv + collection_state.csv --
zero network calls, safe to rerun standalone at any time.
"""
from __future__ import annotations

import argparse
import json

import pandas as pd

from . import config, date_utils, jsonl_utils, state
from .log_utils import get_logger

logger = get_logger(__name__)


def validate_subreddit(slug: str, posts_df: pd.DataFrame, comments_df: pd.DataFrame) -> dict:
    p = posts_df[posts_df["subreddit"] == slug] if not posts_df.empty else posts_df
    c = comments_df[comments_df["subreddit"] == slug] if not comments_df.empty else comments_df

    return {
        "subreddit": slug,
        "total_posts": int(len(p)),
        "total_comments": int(len(c)),
        "unique_authors": int(pd.concat([p.get("author", pd.Series(dtype=str)), c.get("author", pd.Series(dtype=str))]).dropna().nunique()),
        "earliest_post": p["date_posted"].min() if not p.empty else None,
        "latest_post": p["date_posted"].max() if not p.empty else None,
        "cross_posts": int((p["is_crosspost"] == True).sum()) if not p.empty else 0,
        "posts_with_external_urls": int(p["external_url"].notna().sum()) if not p.empty else 0,
        "deleted_removed_posts": int(((p["is_deleted"] == True) | (p["is_removed"] == True)).sum()) if not p.empty else 0,
        "deleted_removed_comments": int(((c["is_deleted"] == True) | (c["is_removed"] == True)).sum()) if not c.empty else 0,
    }


def find_duplicates(df: pd.DataFrame, id_col: str) -> list:
    if df.empty or id_col not in df.columns:
        return []
    dup_mask = df.duplicated(id_col, keep=False)
    return sorted(df.loc[dup_mask, id_col].dropna().unique().tolist())


def find_missing_periods(subreddit: str, start_date, end_date) -> list:
    missing = []
    for chunk_start, _ in date_utils.month_chunks(start_date, end_date):
        month = date_utils.month_key(chunk_start)
        for data_type in ("posts", "comments"):
            if not state.is_month_complete(subreddit, data_type, month):
                row = state.get_row(subreddit, data_type, month)
                status = row.get("status") if row else "never_attempted"
                missing.append(f"{month} ({data_type}: {status})")
    return missing


def find_collection_failures() -> pd.DataFrame:
    df = state.load_state()
    if df.empty:
        return df
    return df[df["status"].isin(["failed", "partial"])]


def run_validation(slugs: list[str], start_date, end_date) -> dict:
    from .process_comments import build_comments_table
    from .process_posts import build_posts_table

    posts_df = build_posts_table(slugs)
    comments_df = build_comments_table(slugs)

    report = {"generated_at": date_utils.iso_now(), "subreddits": {}}

    for slug in slugs:
        stats = validate_subreddit(slug, posts_df, comments_df)
        missing = find_missing_periods(slug, start_date, end_date)
        report["subreddits"][slug] = {**stats, "missing_periods": missing}

    report["duplicate_post_ids"] = find_duplicates(posts_df, "post_id")
    report["duplicate_comment_ids"] = find_duplicates(comments_df, "comment_id")

    failures = find_collection_failures()
    report["collection_failures"] = failures.to_dict(orient="records") if not failures.empty else []

    jsonl_utils.write_json_atomic(config.PROCESSED_DIR / "validation_report.json", report)

    print(json.dumps(report, indent=2, default=str))
    return report


def main():
    from .process_posts import discover_collected_subreddits

    parser = argparse.ArgumentParser(description="Run basic validation checks (no network calls).")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args()

    start = date_utils.parse_date(args.start_date)
    end = date_utils.parse_date(args.end_date) if args.end_date else date_utils.today_utc()
    slugs = discover_collected_subreddits()
    run_validation(slugs, start, end)


if __name__ == "__main__":
    main()
