"""Collect a point-in-time subreddit metadata snapshot.

Primary source: Reddit's public .json endpoints (reddit_public_client). As of 2026-08-27
these return HTTP 403 from this environment (documented gap, see reddit_public_client.py
docstring and README). Secondary source: fields already embedded in previously-collected
posts/comments raw data (community_description, community_members_num, community_rank) --
this costs zero extra API requests, so it is always attempted regardless of the primary
source's success. Run this AFTER collect_posts has run at least once so the secondary
source has data to draw from.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from . import config, date_utils, jsonl_utils, reddit_public_client
from .log_utils import append_collection_log, get_logger

logger = get_logger(__name__)


def _derive_from_raw_posts(subreddit: str) -> dict:
    """Best-effort fields already present on any post/comment record we've collected."""
    slug = config.slugify(subreddit)
    subreddit_raw_dir = config.RAW_DIR / slug
    derived = {"description": None, "member_count": None, "subreddit_rank": None, "source_post_id": None}
    if not subreddit_raw_dir.exists():
        return derived

    latest_ts = None
    for posts_file in sorted(subreddit_raw_dir.glob("*/posts.jsonl")):
        for post in jsonl_utils.read_jsonl(posts_file):
            ts = post.get("timestamp")
            if latest_ts is None or (ts and ts > latest_ts):
                latest_ts = ts
                derived["description"] = post.get("community_description")
                derived["member_count"] = post.get("community_members_num")
                derived["subreddit_rank"] = post.get("community_rank")
                derived["source_post_id"] = post.get("post_id")
    return derived


def collect_subreddit_metadata(subreddit: str) -> Path:
    slug = config.slugify(subreddit)
    collected_at = date_utils.iso_now()
    notes = []

    about = reddit_public_client.get_about(subreddit)
    rules = reddit_public_client.get_rules(subreddit)
    moderators = reddit_public_client.get_moderators(subreddit)
    hot_listing = reddit_public_client.get_hot_listing(subreddit)

    if about is None:
        notes.append("Reddit public about.json unreachable (HTTP 403 from this environment as of 2026-08-27).")
    if not rules:
        notes.append("Rules unavailable (public rules.json blocked or subreddit has none).")
    if not moderators:
        notes.append("Moderator list unavailable (public moderators.json blocked or empty).")

    derived = _derive_from_raw_posts(subreddit)
    if derived["description"] is None and about is None:
        notes.append("Description/member count/rank fall back to null: no public API access and no raw posts collected yet.")

    pinned_posts = [p for p in hot_listing if p.get("stickied")]
    flairs_from_hot = sorted({p.get("link_flair_text") for p in hot_listing if p.get("link_flair_text")})

    about_data = (about or {}).get("data", {})

    metadata = {
        "subreddit": slug,
        "collected_at": collected_at,
        "source": "reddit_public_json" if about is not None else "derived_from_posts_dataset",
        "created_utc": about_data.get("created_utc"),
        "description": about_data.get("public_description") or derived["description"],
        "about_markdown": about_data.get("description"),
        "subscribers": about_data.get("subscribers") or derived["member_count"],
        "subreddit_rank": derived["subreddit_rank"],
        "rules": rules,
        "post_flairs": [{"text": t, "source": "derived_from_hot_listing"} for t in flairs_from_hot],
        "moderators": moderators,
        "pinned_posts": [
            {
                "post_id": p.get("name"),
                "title": p.get("title"),
                "author": p.get("author"),
                "url": p.get("url"),
            }
            for p in pinned_posts
        ],
        "moderator_announcements": [
            {
                "post_id": p.get("name"),
                "title": p.get("title"),
                "author": p.get("author"),
                "text": p.get("selftext"),
                "date": p.get("created_utc"),
            }
            for p in hot_listing
            if p.get("distinguished") == "moderator"
        ],
        "raw_about_response": about,
        "notes": " ".join(notes) if notes else None,
    }

    dated_path = config.METADATA_DIR / f"{slug}_{date_utils.today_utc().isoformat()}.json"
    latest_path = config.METADATA_DIR / f"{slug}_latest.json"
    jsonl_utils.write_json_atomic(dated_path, metadata)
    jsonl_utils.write_json_atomic(latest_path, metadata)

    append_collection_log(
        {
            "collection_date": collected_at,
            "subreddit": slug,
            "period_start": "",
            "period_end": "",
            "data_type": "subreddit_metadata",
            "api_tool": metadata["source"],
            "query_filter": "about/rules/moderators/hot.json + derived from raw posts",
            "records_collected": 1,
            "status": "complete" if about is not None else "partial",
            "errors_or_limitations": metadata["notes"] or "",
        }
    )

    logger.info("Wrote subreddit metadata snapshot for %s to %s", slug, dated_path)
    return dated_path


def main():
    parser = argparse.ArgumentParser(description="Collect a subreddit metadata snapshot.")
    parser.add_argument("--subreddit", required=True)
    args = parser.parse_args()
    collect_subreddit_metadata(args.subreddit)


if __name__ == "__main__":
    main()
