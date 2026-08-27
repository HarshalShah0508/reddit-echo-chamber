"""Batch comments collection per post URL, gated by state and resumed via disk-content diff.

The comments dataset (BRIGHTDATA_REDDIT_COMMENTS_DATASET_ID) is a plain url_collection
dataset confirmed live on 2026-08-27: no discover_by/type needed, payload is a list of
{"url": post_url}, and it returns one record per TOP-LEVEL comment with native comment_id /
parent_comment_id / root_comment_id, plus a nested "replies" array for direct replies only
(no deeper nesting was observed -- a documented API gap, see README).

Resumability: rather than tracking "batch N of M" as a counter, every run recomputes which
post_ids already have comments on disk (already_covered_post_ids) and only re-batches the
remainder. This survives a crash mid-batch with no extra bookkeeping.
"""
from __future__ import annotations

import argparse
from datetime import date
from math import ceil

from . import brightdata_client, config, date_utils, jsonl_utils, state
from .log_utils import append_collection_log, get_logger

logger = get_logger(__name__)


def post_ids_and_urls_for_month(subreddit: str, month: str) -> list[tuple[str, str]]:
    slug = config.slugify(subreddit)
    path = config.RAW_DIR / slug / month / "posts.jsonl"
    return [(p["post_id"], p["url"]) for p in jsonl_utils.read_jsonl(path) if p.get("post_id") and p.get("url")]


def already_covered_post_ids(subreddit: str, month: str) -> set:
    slug = config.slugify(subreddit)
    path = config.RAW_DIR / slug / month / "comments.jsonl"
    return {rec.get("parent_comment_id") for rec in jsonl_utils.read_jsonl(path) if rec.get("parent_comment_id")}


def batch(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def collect_comments_for_month(
    subreddit: str,
    month_start: date,
    month_end_calendar: date,
    requested_end: date,
    batch_size: int = config.COMMENTS_BATCH_SIZE,
    force: bool = False,
) -> dict:
    slug = config.slugify(subreddit)
    month = date_utils.month_key(month_start)

    posts_row = state.get_row(subreddit, "posts", month)
    if posts_row is None:
        logger.warning("No posts collected yet for %s %s, skipping comments.", slug, month)
        return {"status": "skipped_no_posts", "subreddit": slug, "month": month, "records": 0}

    all_posts = post_ids_and_urls_for_month(subreddit, month)
    if not all_posts:
        state.upsert(subreddit, "comments", month, month_start, month_end_calendar, status="complete", records_collected=0)
        return {"status": "complete", "subreddit": slug, "month": month, "records": 0}

    covered = set() if force else already_covered_post_ids(subreddit, month)
    pending_posts = [(pid, url) for pid, url in all_posts if pid not in covered]

    if not pending_posts:
        logger.info("Comments for %s %s already cover all %d known posts, skipping.", slug, month, len(all_posts))
        return {"status": "skipped", "subreddit": slug, "month": month, "records": 0}

    raw_path = config.RAW_DIR / slug / month / "comments.jsonl"
    total_appended = 0
    failed_batches = 0
    batches = list(batch(pending_posts, batch_size))
    total_batches = len(batches)

    for i, chunk in enumerate(batches, start=1):
        urls = [u for _, u in chunk]
        payload = [{"url": u} for u in urls]
        query_filter = f"url_collection batch {i}/{total_batches}, {len(urls)} post urls"
        try:
            snapshot_id, raw_records = brightdata_client.fetch_dataset(config.COMMENTS_DATASET_ID, payload)
        except Exception as exc:
            logger.exception("Comments batch %d/%d failed for %s %s", i, total_batches, slug, month)
            failed_batches += 1
            append_collection_log(
                {
                    "collection_date": date_utils.iso_now(),
                    "subreddit": slug,
                    "period_start": month_start.isoformat(),
                    "period_end": month_end_calendar.isoformat(),
                    "data_type": "comments",
                    "api_tool": "brightdata_comments",
                    "query_filter": query_filter,
                    "records_collected": 0,
                    "status": "failed",
                    "errors_or_limitations": str(exc)[:500],
                }
            )
            continue

        appended = jsonl_utils.append_jsonl_dedup(raw_path, raw_records, id_fn=lambda r: r.get("comment_id") or r.get("url"))
        total_appended += appended

        append_collection_log(
            {
                "collection_date": date_utils.iso_now(),
                "subreddit": slug,
                "period_start": month_start.isoformat(),
                "period_end": month_end_calendar.isoformat(),
                "data_type": "comments",
                "api_tool": "brightdata_comments",
                "query_filter": query_filter,
                "records_collected": appended,
                "status": "complete",
                "errors_or_limitations": "",
            }
        )

    new_covered = already_covered_post_ids(subreddit, month)
    all_ids = {pid for pid, _ in all_posts}
    fully_covered = all_ids <= new_covered
    if fully_covered and posts_row.get("status") == "complete":
        month_status = "complete"
    elif new_covered:
        month_status = "partial"
    else:
        month_status = "failed"

    state.upsert(
        subreddit,
        "comments",
        month,
        month_start,
        month_end_calendar,
        status=month_status,
        records_collected=total_appended,
    )

    return {
        "status": month_status,
        "subreddit": slug,
        "month": month,
        "records": total_appended,
        "failed_batches": failed_batches,
    }


def collect_comments_for_range(
    subreddit: str, start_date: date, end_date: date, batch_size: int = config.COMMENTS_BATCH_SIZE, force: bool = False
) -> list[dict]:
    results = []
    for chunk_start, _ in date_utils.month_chunks(start_date, end_date):
        month_start, month_end_calendar = date_utils.month_bounds(chunk_start)
        results.append(collect_comments_for_month(subreddit, month_start, month_end_calendar, end_date, batch_size=batch_size, force=force))
    return results


def main():
    parser = argparse.ArgumentParser(description="Collect comments for a subreddit's already-collected posts.")
    parser.add_argument("--subreddit", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--batch-size", type=int, default=config.COMMENTS_BATCH_SIZE)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    start = date_utils.parse_date(args.start_date)
    end = date_utils.parse_date(args.end_date) if args.end_date else date_utils.today_utc()

    results = collect_comments_for_range(args.subreddit, start, end, batch_size=args.batch_size, force=args.force)
    for r in results:
        print(r)


if __name__ == "__main__":
    main()
