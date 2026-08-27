"""Raw posts.jsonl -> processed/posts.csv (+ processed/crossposts.csv side table).

Column-by-column mapping from BrightData's raw post schema is documented inline below.
Fields the raw schema simply doesn't carry (upvote_ratio, reliable crosspost linkage) are
always written as null/None -- this is a real API limitation, documented in the README,
not silently guessed at.
"""
from __future__ import annotations

import argparse
from urllib.parse import urlparse

import pandas as pd

from . import config, id_utils, jsonl_utils
from .log_utils import get_logger

logger = get_logger(__name__)

POSTS_COLUMNS = [
    "subreddit", "post_id", "date_posted", "author", "title", "post_text", "flair",
    "post_type", "score", "upvote_ratio", "num_comments", "permalink", "external_url",
    "external_domain", "is_crosspost", "crosspost_original_post_id", "crosspost_original_subreddit",
    "is_stickied", "is_deleted", "is_removed", "community_members_num", "post_karma",
    "num_related_posts", "collected_at", "raw_json_path",
]

CROSSPOST_COLUMNS = [
    "current_post_id", "current_subreddit", "original_post_id", "original_subreddit",
    "original_author", "original_post_url", "original_post_date", "detected_at",
]


def discover_collected_subreddits() -> list[str]:
    if not config.RAW_DIR.exists():
        return []
    return sorted(p.name for p in config.RAW_DIR.iterdir() if p.is_dir())


def _iter_raw_posts_with_source(slug: str):
    subreddit_dir = config.RAW_DIR / slug
    if not subreddit_dir.exists():
        return
    for posts_file in sorted(subreddit_dir.glob("*/posts.jsonl")):
        rel = posts_file.relative_to(config.ROOT)
        for i, record in enumerate(jsonl_utils.read_jsonl(posts_file)):
            yield record, f"{rel}#L{i + 1}"


def _load_pinned_post_ids(slug: str) -> set:
    latest_path = config.METADATA_DIR / f"{slug}_latest.json"
    if not latest_path.exists():
        return set()
    try:
        meta = jsonl_utils.read_json(latest_path)
    except Exception:
        return set()
    return {p.get("post_id") for p in meta.get("pinned_posts", []) if p.get("post_id")}


def infer_post_type(raw: dict) -> str:
    """Heuristic only -- the raw schema has no authoritative type flag like Apify's isSelf/isVideo."""
    if raw.get("videos"):
        return "video"
    if raw.get("photos"):
        return "image"
    if raw.get("embedded_links"):
        return "link"
    return "text"


def normalize_post(raw: dict, slug: str, raw_json_path: str, pinned_ids: set) -> dict:
    author = raw.get("user_posted")
    is_deleted_author, is_removed_author = id_utils.is_deleted_or_removed_text(author)
    is_deleted_title, is_removed_title = id_utils.is_deleted_or_removed_text(raw.get("title"))

    external_links = raw.get("embedded_links") or []
    external_url = external_links[0] if external_links else None

    post_text = raw.get("description")
    if post_text is None:
        post_text = raw.get("description_markdown")
    # a deleted/removed post on Reddit most commonly shows this in the BODY, not the title
    is_deleted_body, is_removed_body = id_utils.is_deleted_or_removed_text(post_text)

    post_id = raw.get("post_id")

    return {
        "subreddit": slug,
        "post_id": post_id,
        "date_posted": raw.get("date_posted"),
        "author": None if is_deleted_author else author,
        "title": raw.get("title"),
        "post_text": post_text,
        "flair": raw.get("tag"),
        "post_type": infer_post_type(raw),
        "score": raw.get("num_upvotes"),
        "upvote_ratio": None,  # not present anywhere in BrightData's raw post schema
        "num_comments": raw.get("num_comments"),
        "permalink": raw.get("url"),
        "external_url": external_url,
        "external_domain": urlparse(external_url).netloc if external_url else None,
        "is_crosspost": None,  # unresolved: no confirmed crosspost marker found, see README
        "crosspost_original_post_id": None,
        "crosspost_original_subreddit": None,
        "is_stickied": True if post_id in pinned_ids else None,
        "is_deleted": is_deleted_author or is_deleted_title or is_deleted_body,
        "is_removed": is_removed_author or is_removed_title or is_removed_body,
        "community_members_num": raw.get("community_members_num"),
        "post_karma": raw.get("post_karma"),
        "num_related_posts": len(raw.get("related_posts") or []),
        "collected_at": raw.get("timestamp"),
        "raw_json_path": raw_json_path,
    }


def load_raw_posts(slug: str) -> list[dict]:
    return [r for r, _ in _iter_raw_posts_with_source(slug)]


def build_posts_table(slugs: list[str] | None = None) -> pd.DataFrame:
    if slugs is None:
        slugs = discover_collected_subreddits()

    rows = []
    for slug in slugs:
        pinned_ids = _load_pinned_post_ids(slug)
        for raw, source in _iter_raw_posts_with_source(slug):
            rows.append(normalize_post(raw, slug, source, pinned_ids))

    df = pd.DataFrame(rows, columns=POSTS_COLUMNS)
    if not df.empty:
        df["collected_at"] = df["collected_at"].fillna("")
        df = df.sort_values(["collected_at"], ascending=True).drop_duplicates(subset=["post_id"], keep="last")
        df = df.sort_values(["subreddit", "date_posted"]).reset_index(drop=True)

    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(config.PROCESSED_DIR / "posts.csv", index=False)
    logger.info("Wrote %d posts to processed/posts.csv", len(df))

    _write_crossposts_table(df)
    return df


def _write_crossposts_table(posts_df: pd.DataFrame) -> None:
    crossposts = posts_df[posts_df["is_crosspost"] == True] if not posts_df.empty else pd.DataFrame()
    out = pd.DataFrame(columns=CROSSPOST_COLUMNS)
    if not crossposts.empty:
        out = pd.DataFrame(
            {
                "current_post_id": crossposts["post_id"],
                "current_subreddit": crossposts["subreddit"],
                "original_post_id": crossposts["crosspost_original_post_id"],
                "original_subreddit": crossposts["crosspost_original_subreddit"],
                "original_author": None,
                "original_post_url": None,
                "original_post_date": None,
                "detected_at": crossposts["collected_at"],
            }
        )
    out.to_csv(config.PROCESSED_DIR / "crossposts.csv", index=False)
    logger.info("Wrote %d crossposts to processed/crossposts.csv", len(out))


def main():
    parser = argparse.ArgumentParser(description="Build processed/posts.csv from raw JSONL.")
    parser.add_argument("--subreddit", action="append", default=None, help="Limit to specific subreddit(s); default is all collected.")
    args = parser.parse_args()
    slugs = [config.slugify(s) for s in args.subreddit] if args.subreddit else None
    build_posts_table(slugs)


if __name__ == "__main__":
    main()
