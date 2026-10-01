"""Comments collection via the Arctic Shift API, one thread fetch per post.

Alternative to BrightData's url_collection comments dataset (see collect_comments.py) for
subreddits/networks where BrightData itself is unreachable. Arctic Shift's
/api/comments/search returns a FLAT list of every comment under a post -- this module
reconstructs the same two-level (top-level + direct replies) tree shape BrightData's raw
comments.jsonl uses (native reply-to-reply threads deeper than that get dropped), so
process_comments.flatten_comment_tree needs no changes to consume either source. This
matches the depth<=1 limitation already documented for the BrightData-collected subreddit
(see README "Known gaps") rather than introducing a schema difference between subreddits.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

from . import arcticshift_client, config, date_utils, jsonl_utils, state
from .log_utils import append_collection_log, get_logger

logger = get_logger(__name__)

# Fetches are network-I/O-bound (Arctic Shift round-trip, not CPU), so overlapping several
# in flight multiplies throughput ~N-fold instead of paying each request's latency serially.
# File writes (append_jsonl_dedup) stay single-threaded in the main thread -- only the fetch
# itself runs concurrently -- so dedup/state bookkeeping has no race condition.
DEFAULT_WORKERS = 5


def _iso_z(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _build_comment_tree(post_id_bare: str, flat_comments: list[dict], slug: str, post_url: str) -> list[dict]:
    link_fullname = f"t3_{post_id_bare}"
    top_level = [c for c in flat_comments if c.get("parent_id") == link_fullname]

    records = []
    for top in top_level:
        direct_replies = [c for c in flat_comments if c.get("parent_id") == f"t1_{top['id']}"]
        records.append(
            {
                "url": f"https://www.reddit.com{top.get('permalink', '')}",
                "comment_id": top["id"],
                "user_posted": top.get("author"),
                "comment": top.get("body"),
                "date_posted": _iso_z(top["created_utc"]),
                "post_url": post_url,
                "post_id": post_id_bare,
                "community_name": slug,
                "num_upvotes": top.get("score"),
                "num_replies": len(direct_replies),
                "is_moderator": top.get("distinguished") == "moderator",
                "parent_comment_id": link_fullname,
                "timestamp": date_utils.iso_now(),
                "source": "arcticshift",
                "replies": [
                    {
                        "reply_id": r["id"],
                        "user_replying": r.get("author"),
                        "reply": r.get("body"),
                        "date_of_reply": _iso_z(r["created_utc"]),
                        "num_replies": None,
                        "num_upvotes": r.get("score"),
                    }
                    for r in direct_replies
                ],
            }
        )
    return records


def post_ids_and_urls_for_month(subreddit: str, month: str) -> list[tuple[str, str]]:
    slug = config.slugify(subreddit)
    path = config.RAW_DIR / slug / month / "posts.jsonl"
    return [(p["post_id"], p["url"]) for p in jsonl_utils.read_jsonl(path) if p.get("post_id") and p.get("url")]


def already_covered_post_ids_bare(subreddit: str, month: str) -> set:
    slug = config.slugify(subreddit)
    path = config.RAW_DIR / slug / month / "comments.jsonl"
    return {rec.get("post_id") for rec in jsonl_utils.read_jsonl(path) if rec.get("post_id")}


def collect_comments_for_month(subreddit: str, month_start: date, month_end_calendar: date, requested_end: date, force: bool = False, workers: int = DEFAULT_WORKERS) -> dict:
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

    covered_bare = set() if force else already_covered_post_ids_bare(subreddit, month)
    pending_posts = [
        (pid, url) for pid, url in all_posts
        if (pid[3:] if pid.startswith("t3_") else pid) not in covered_bare
    ]
    if not pending_posts:
        logger.info("Comments for %s %s already cover all %d known posts, skipping.", slug, month, len(all_posts))
        return {"status": "skipped", "subreddit": slug, "month": month, "records": 0}

    raw_path = config.RAW_DIR / slug / month / "comments.jsonl"
    total_appended = 0
    failed = 0

    def _fetch(pid_url):
        pid, url = pid_url
        post_id_bare = pid[3:] if pid.startswith("t3_") else pid
        return post_id_bare, url, arcticshift_client.fetch_comments_for_post(post_id_bare)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch, pu): pu for pu in pending_posts}
        for future in as_completed(futures):
            pid, url = futures[future]
            post_id_bare = pid[3:] if pid.startswith("t3_") else pid
            query_filter = f"arcticshift link_id=t3_{post_id_bare}"
            try:
                post_id_bare, url, flat_comments = future.result()
            except Exception as exc:
                logger.exception("Arctic Shift comments fetch failed for %s post %s", slug, post_id_bare)
                failed += 1
                append_collection_log(
                    {
                        "collection_date": date_utils.iso_now(),
                        "subreddit": slug,
                        "period_start": month_start.isoformat(),
                        "period_end": month_end_calendar.isoformat(),
                        "data_type": "comments",
                        "api_tool": "arcticshift_comments",
                        "query_filter": query_filter,
                        "records_collected": 0,
                        "status": "failed",
                        "errors_or_limitations": str(exc)[:500],
                    }
                )
                continue

            records = _build_comment_tree(post_id_bare, flat_comments, slug, url)
            appended = jsonl_utils.append_jsonl_dedup(raw_path, records, id_fn=lambda r: r.get("comment_id") or r.get("url"))
            total_appended += appended
            append_collection_log(
                {
                    "collection_date": date_utils.iso_now(),
                    "subreddit": slug,
                    "period_start": month_start.isoformat(),
                    "period_end": month_end_calendar.isoformat(),
                    "data_type": "comments",
                    "api_tool": "arcticshift_comments",
                    "query_filter": query_filter,
                    "records_collected": appended,
                    "status": "complete",
                    "errors_or_limitations": "",
                }
            )

    new_covered = already_covered_post_ids_bare(subreddit, month)
    all_ids_bare = {(pid[3:] if pid.startswith("t3_") else pid) for pid, _ in all_posts}
    fully_covered = all_ids_bare <= new_covered
    if fully_covered and posts_row.get("status") == "complete":
        month_status = "complete"
    elif new_covered:
        month_status = "partial"
    else:
        month_status = "failed"

    state.upsert(subreddit, "comments", month, month_start, month_end_calendar, status=month_status, records_collected=total_appended)
    return {"status": month_status, "subreddit": slug, "month": month, "records": total_appended, "failed_posts": failed}


def collect_comments_for_range(subreddit: str, start_date: date, end_date: date, force: bool = False, workers: int = DEFAULT_WORKERS) -> list[dict]:
    results = []
    for chunk_start, _ in date_utils.month_chunks(start_date, end_date):
        month_start, month_end_calendar = date_utils.month_bounds(chunk_start)
        results.append(collect_comments_for_month(subreddit, month_start, month_end_calendar, end_date, force=force, workers=workers))
    return results


def main():
    parser = argparse.ArgumentParser(description="Collect comments for a subreddit's already-collected posts, via Arctic Shift.")
    parser.add_argument("--subreddit", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args()

    start = date_utils.parse_date(args.start_date)
    end = date_utils.parse_date(args.end_date) if args.end_date else date_utils.today_utc()

    results = collect_comments_for_range(args.subreddit, start, end, force=args.force, workers=args.workers)
    for r in results:
        print(r)


if __name__ == "__main__":
    main()
