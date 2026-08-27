"""Month-by-month posts collection, gated by collection_state.csv.

Live evidence from 2026-08-27 testing (before the BrightData account went inactive)
shaped this module's strategy:
  - `end_date` in the trigger payload is rejected outright by BrightData with HTTP 400
    ("This input should not contain a end_date field") -- so only `start_date` is ever sent.
  - A `start_date`-only request against r/kolkata returned 200 OK but 0 records in ~13s.
  - `sort_by` is enum-validated server-side ("new" lowercase was rejected: "This value is
    not allowed") and the correct casing/value was never confirmed before the account went
    inactive.
  - A completely bare request (no date params at all, matching the original working POC)
    was still running after 12+ minutes for r/kolkata's full history -- clearly a real,
    working, much slower full-history crawl.
Given this, we can't be sure whether the 0-record dated response means "this subreddit
genuinely has nothing new-enough" or "our date/sort params are subtly wrong." Rather than
either trust the fast dated path blindly or always pay for a full history crawl, this module
tries the cheap dated request first and falls back to exactly ONE bare full-history request
per collection run if that returns 0 records -- and reuses that one bare response to satisfy
every pending month in the requested range at once, not just the first one. All fields the
server can't be trusted to filter correctly are re-checked client-side after download either
way.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timezone

import requests

from . import brightdata_client, config, date_utils, jsonl_utils, state
from .log_utils import append_collection_log, get_logger

logger = get_logger(__name__)


def _parse_post_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).date()
    except ValueError:
        return None


def _within_range(record: dict, start: date, end: date) -> bool:
    d = _parse_post_date(record.get("date_posted"))
    return d is not None and start <= d <= end


def fetch_posts_dated(subreddit: str, period_start: date) -> tuple[str, list[dict]]:
    """start_date-only request. end_date is never sent -- BrightData rejects it with HTTP 400."""
    payload = [{"url": config.subreddit_url(subreddit), "start_date": period_start.isoformat()}]
    return brightdata_client.fetch_dataset(config.POSTS_DATASET_ID, payload, discover_by="subreddit_url")


def fetch_posts_bare(subreddit: str) -> tuple[str, list[dict]]:
    """No date params at all -- confirmed-working full-history discovery, just slower."""
    payload = [{"url": config.subreddit_url(subreddit)}]
    return brightdata_client.fetch_dataset(config.POSTS_DATASET_ID, payload, discover_by="subreddit_url")


def _write_month(subreddit: str, month: str, month_start: date, target_end: date, records: list[dict], snapshot_id: str, query_filter: str) -> dict:
    slug = config.slugify(subreddit)
    filtered = [r for r in records if _within_range(r, month_start, target_end)]
    dropped = len(records) - len(filtered)

    raw_path = config.RAW_DIR / slug / month / "posts.jsonl"
    appended = jsonl_utils.append_jsonl_dedup(raw_path, filtered, id_fn=lambda r: r.get("post_id"))

    _, month_end_calendar = date_utils.month_bounds(month_start)
    status = "complete" if target_end >= month_end_calendar else "partial"

    state.upsert(subreddit, "posts", month, month_start, target_end, status=status, snapshot_id=snapshot_id, records_collected=appended)

    append_collection_log(
        {
            "collection_date": date_utils.iso_now(),
            "subreddit": slug,
            "period_start": month_start.isoformat(),
            "period_end": target_end.isoformat(),
            "data_type": "posts",
            "api_tool": "brightdata_posts",
            "query_filter": query_filter,
            "records_collected": appended,
            "status": status,
            "errors_or_limitations": f"{dropped} records outside requested date range dropped" if dropped else "",
        }
    )
    return {"status": status, "subreddit": slug, "month": month, "records": appended, "snapshot_id": snapshot_id}


def _log_failure(subreddit: str, month: str, month_start: date, target_end: date, query_filter: str, exc: Exception) -> dict:
    slug = config.slugify(subreddit)
    state.upsert(subreddit, "posts", month, month_start, month_start, status="failed", error_message=str(exc))
    append_collection_log(
        {
            "collection_date": date_utils.iso_now(),
            "subreddit": slug,
            "period_start": month_start.isoformat(),
            "period_end": target_end.isoformat(),
            "data_type": "posts",
            "api_tool": "brightdata_posts",
            "query_filter": query_filter,
            "records_collected": 0,
            "status": "failed",
            "errors_or_limitations": str(exc)[:500],
        }
    )
    return {"status": "failed", "subreddit": slug, "month": month, "records": 0}


def collect_posts_for_range(subreddit: str, start_date: date, end_date: date, force: bool = False) -> list[dict]:
    slug = config.slugify(subreddit)
    chunks = date_utils.month_chunks(start_date, end_date)

    pending_chunks = []
    for chunk_start, _ in chunks:
        month_start, month_end_calendar = date_utils.month_bounds(chunk_start)
        month = date_utils.month_key(month_start)
        target_end = min(month_end_calendar, end_date)
        if not force and state.pending_subrange(subreddit, "posts", month_start, month_end_calendar, end_date) is None:
            continue
        pending_chunks.append((month, month_start, target_end))

    if not pending_chunks:
        logger.info("Posts for %s already complete for %s..%s, skipping.", slug, start_date, end_date)
        return [{"status": "skipped", "subreddit": slug, "month": date_utils.month_key(c[0]), "records": 0} for c in chunks]

    results = []
    first_month, first_month_start, first_target_end = pending_chunks[0]
    query_filter = f"discover_new/subreddit_url start_date={first_month_start}"

    try:
        snapshot_id, raw_records = fetch_posts_dated(subreddit, first_month_start)
    except Exception as exc:
        logger.exception("Posts collection failed for %s %s", slug, first_month)
        results.append(_log_failure(subreddit, first_month, first_month_start, first_target_end, query_filter, exc))
        remaining = pending_chunks[1:]
    else:
        if raw_records:
            results.append(_write_month(subreddit, first_month, first_month_start, first_target_end, raw_records, snapshot_id, query_filter))
            remaining = pending_chunks[1:]
        else:
            logger.warning(
                "Dated request for %s returned 0 records (start_date=%s) -- falling back to one bare full-history request.",
                slug, first_month_start,
            )
            results.append(_run_bare_fallback(subreddit, pending_chunks))
            remaining = []

    for month, month_start, target_end in remaining:
        month_query_filter = f"discover_new/subreddit_url start_date={month_start}"
        try:
            snapshot_id, raw_records = fetch_posts_dated(subreddit, month_start)
        except Exception as exc:
            logger.exception("Posts collection failed for %s %s", slug, month)
            results.append(_log_failure(subreddit, month, month_start, target_end, month_query_filter, exc))
            continue
        results.append(_write_month(subreddit, month, month_start, target_end, raw_records, snapshot_id, month_query_filter))

    return results


def _run_bare_fallback(subreddit: str, pending_chunks: list[tuple[str, date, date]]) -> dict:
    """One bare full-history discovery call, bucketed to satisfy every pending month at once."""
    slug = config.slugify(subreddit)
    try:
        snapshot_id, raw_records = fetch_posts_bare(subreddit)
    except Exception as exc:
        logger.exception("Bare full-history fallback failed for %s", slug)
        first_month, first_month_start, first_target_end = pending_chunks[0]
        for month, month_start, target_end in pending_chunks:
            _log_failure(subreddit, month, month_start, target_end, "discover_new/subreddit_url bare full-history (fallback)", exc)
        return {"status": "failed", "subreddit": slug, "month": "ALL", "records": 0}

    by_month: dict[str, list[dict]] = defaultdict(list)
    for record in raw_records:
        d = _parse_post_date(record.get("date_posted"))
        if d:
            by_month[date_utils.month_key(d)].append(record)

    total = 0
    for month, month_start, target_end in pending_chunks:
        month_records = by_month.get(month, [])
        result = _write_month(
            subreddit, month, month_start, target_end, month_records, snapshot_id,
            "discover_new/subreddit_url bare full-history (fallback, no date params)",
        )
        total += result["records"]

    logger.info("Bare full-history fallback for %s appended %d records across %d months.", slug, total, len(pending_chunks))
    return {"status": "complete", "subreddit": slug, "month": "ALL", "records": total, "snapshot_id": snapshot_id}


def collect_posts_for_month(subreddit: str, month_start: date, month_end_calendar: date, requested_end: date, force: bool = False) -> dict:
    """Single-month convenience wrapper, kept for standalone/debugging use.

    Prefer collect_posts_for_range for real runs -- it shares one bare-fallback call across
    all pending months instead of risking one expensive full-history crawl per month.
    """
    month = date_utils.month_key(month_start)
    results = collect_posts_for_range(subreddit, month_start, min(month_end_calendar, requested_end), force=force)
    for r in results:
        if r.get("month") in (month, "ALL"):
            return r
    return {"status": "skipped", "subreddit": config.slugify(subreddit), "month": month, "records": 0}


def main():
    parser = argparse.ArgumentParser(description="Collect posts for a subreddit over a date range.")
    parser.add_argument("--subreddit", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    start = date_utils.parse_date(args.start_date)
    end = date_utils.parse_date(args.end_date) if args.end_date else date_utils.today_utc()

    results = collect_posts_for_range(args.subreddit, start, end, force=args.force)
    for r in results:
        print(r)


if __name__ == "__main__":
    main()
