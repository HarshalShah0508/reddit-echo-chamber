"""Month-by-month posts collection via the Arctic Shift API, gated by collection_state.csv.

Writes into the exact same raw/<slug>/<month>/posts.jsonl schema BrightData's collect_posts.py
produces (see normalize_post in process_posts.py) so the rest of the pipeline -- processing,
aggregation, validation -- needs no changes to consume either source. Unlike BrightData's
async snapshot jobs, Arctic Shift's /api/posts/search is a plain synchronous paginated GET,
so there's no dated-vs-bare-fallback dance here: one paginated fetch covers the whole
requested range in one pass, then gets bucketed into months client-side.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timezone

from . import arcticshift_client, config, date_utils, jsonl_utils, state
from .log_utils import append_collection_log, get_logger

logger = get_logger(__name__)


def _iso_z(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _to_raw_post(raw: dict, slug: str) -> dict:
    permalink = raw.get("permalink") or ""
    post_id_bare = raw.get("id")
    crossposts = raw.get("crosspost_parent_list") or []
    return {
        "post_id": f"t3_{post_id_bare}",
        "url": f"https://www.reddit.com{permalink}" if permalink else raw.get("url"),
        "user_posted": raw.get("author"),
        "title": raw.get("title"),
        "description": raw.get("selftext"),
        "num_comments": raw.get("num_comments"),
        "date_posted": _iso_z(raw["created_utc"]),
        "community_name": slug,
        "num_upvotes": raw.get("score"),
        "upvote_ratio": raw.get("upvote_ratio"),
        "photos": None,
        "videos": None,
        "tag": raw.get("link_flair_text"),
        "related_posts": [],
        "community_members_num": raw.get("subreddit_subscribers"),
        "post_karma": None,
        "crosspost_parent_list": crossposts,
        "stickied": raw.get("stickied"),
        "timestamp": date_utils.iso_now(),
        "source": "arcticshift",
    }


def _within_range(record: dict, start: date, end: date) -> bool:
    d = datetime.fromisoformat(record["date_posted"].replace("Z", "+00:00")).astimezone(timezone.utc).date()
    return start <= d <= end


def _write_month(subreddit: str, month: str, month_start: date, target_end: date, raw_posts: list[dict], query_filter: str) -> dict:
    slug = config.slugify(subreddit)
    filtered = [r for r in raw_posts if _within_range(r, month_start, target_end)]

    raw_path = config.RAW_DIR / slug / month / "posts.jsonl"
    appended = jsonl_utils.append_jsonl_dedup(raw_path, filtered, id_fn=lambda r: r.get("post_id"))

    _, month_end_calendar = date_utils.month_bounds(month_start)
    status = "complete" if target_end >= month_end_calendar else "partial"

    state.upsert(subreddit, "posts", month, month_start, target_end, status=status, snapshot_id="arcticshift", records_collected=appended)
    append_collection_log(
        {
            "collection_date": date_utils.iso_now(),
            "subreddit": slug,
            "period_start": month_start.isoformat(),
            "period_end": target_end.isoformat(),
            "data_type": "posts",
            "api_tool": "arcticshift_posts",
            "query_filter": query_filter,
            "records_collected": appended,
            "status": status,
            "errors_or_limitations": "",
        }
    )
    return {"status": status, "subreddit": slug, "month": month, "records": appended}


def collect_posts_for_range(subreddit: str, start_date: date, end_date: date, force: bool = False) -> list[dict]:
    slug = config.slugify(subreddit)
    chunks = date_utils.month_chunks(start_date, end_date)

    pending = []
    for chunk_start, _ in chunks:
        month_start, month_end_calendar = date_utils.month_bounds(chunk_start)
        month = date_utils.month_key(month_start)
        target_end = min(month_end_calendar, end_date)
        if not force and state.pending_subrange(subreddit, "posts", month_start, month_end_calendar, end_date) is None:
            continue
        pending.append((month, month_start, target_end))

    if not pending:
        logger.info("Posts for %s already complete for %s..%s, skipping.", slug, start_date, end_date)
        return []

    overall_start = pending[0][1]
    overall_end = pending[-1][2]
    query_filter = f"arcticshift subreddit={slug} after={overall_start} before={overall_end}"

    try:
        raw_posts_native = arcticshift_client.fetch_posts(slug, overall_start, overall_end)
    except Exception as exc:
        logger.exception("Arctic Shift posts fetch failed for %s", slug)
        results = []
        for month, month_start, target_end in pending:
            state.upsert(subreddit, "posts", month, month_start, month_start, status="failed", error_message=str(exc))
            append_collection_log(
                {
                    "collection_date": date_utils.iso_now(),
                    "subreddit": slug,
                    "period_start": month_start.isoformat(),
                    "period_end": target_end.isoformat(),
                    "data_type": "posts",
                    "api_tool": "arcticshift_posts",
                    "query_filter": query_filter,
                    "records_collected": 0,
                    "status": "failed",
                    "errors_or_limitations": str(exc)[:500],
                }
            )
            results.append({"status": "failed", "subreddit": slug, "month": month, "records": 0})
        return results

    raw_posts = [_to_raw_post(r, slug) for r in raw_posts_native]
    logger.info("Arctic Shift returned %d posts for %s in %s..%s", len(raw_posts), slug, overall_start, overall_end)

    by_month: dict[str, list[dict]] = defaultdict(list)
    for record in raw_posts:
        d = datetime.fromisoformat(record["date_posted"].replace("Z", "+00:00")).astimezone(timezone.utc).date()
        by_month[date_utils.month_key(d)].append(record)

    results = []
    for month, month_start, target_end in pending:
        results.append(_write_month(subreddit, month, month_start, target_end, by_month.get(month, []), query_filter))
    return results


def main():
    parser = argparse.ArgumentParser(description="Collect posts for a subreddit via Arctic Shift.")
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
