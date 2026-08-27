"""Raw comments.jsonl -> processed/comments.csv.

Confirmed live shape (2026-08-27) of BRIGHTDATA_REDDIT_COMMENTS_DATASET_ID: each raw record
is a TOP-LEVEL comment with native comment_id / parent_comment_id (= the post's t3_ id) /
root_comment_id, plus a nested "replies" array of DIRECT replies only. Reply objects use a
different field-naming convention (reply_id/user_replying/reply/date_of_reply) and carry no
comment_id of their own and no further "replies" nesting -- deeper reply-to-reply threads
are NOT retrievable from this dataset. This is a real, confirmed API gap, documented in the
README, not a bug in this processor: depth is capped at 1 (post -> top-level comment -> its
direct replies).
"""
from __future__ import annotations

import argparse

import pandas as pd

from . import config, id_utils, jsonl_utils
from .log_utils import get_logger

logger = get_logger(__name__)

COMMENTS_COLUMNS = [
    "subreddit", "post_id", "comment_id", "parent_id", "author", "date_posted",
    "comment_text", "score", "depth", "is_mod", "is_automod", "is_deleted", "is_removed",
    "num_replies", "collected_at", "raw_json_path",
]


def _prefixed(kind: str, raw_id) -> str | None:
    if raw_id is None:
        return None
    raw_id = str(raw_id)
    return raw_id if raw_id.startswith(f"{kind}_") else f"{kind}_{raw_id}"


def _iter_raw_comments_with_source(slug: str):
    subreddit_dir = config.RAW_DIR / slug
    if not subreddit_dir.exists():
        return
    for comments_file in sorted(subreddit_dir.glob("*/comments.jsonl")):
        rel = comments_file.relative_to(config.ROOT)
        for i, record in enumerate(jsonl_utils.read_jsonl(comments_file)):
            yield record, f"{rel}#L{i + 1}"


def flatten_comment_tree(raw_comment: dict, slug: str, raw_json_path: str) -> list[dict]:
    post_id = raw_comment.get("parent_comment_id") or _prefixed("t3", raw_comment.get("post_id"))
    top_comment_id = _prefixed("t1", raw_comment.get("comment_id"))
    author = raw_comment.get("user_posted")
    is_deleted, is_removed = id_utils.is_deleted_or_removed_text(raw_comment.get("comment"))
    author_deleted, author_removed = id_utils.is_deleted_or_removed_text(author)

    rows = [
        {
            "subreddit": slug,
            "post_id": post_id,
            "comment_id": top_comment_id,
            "parent_id": post_id,
            "author": None if author_deleted else author,
            "date_posted": raw_comment.get("date_posted"),
            "comment_text": raw_comment.get("comment"),
            "score": raw_comment.get("num_upvotes"),
            "depth": 0,
            "is_mod": raw_comment.get("is_moderator"),
            "is_automod": author == "AutoModerator",
            "is_deleted": is_deleted or author_deleted,
            "is_removed": is_removed or author_removed,
            "num_replies": raw_comment.get("num_replies"),
            "collected_at": raw_comment.get("timestamp"),
            "raw_json_path": raw_json_path,
        }
    ]

    for reply in raw_comment.get("replies") or []:
        reply_author = reply.get("user_replying")
        r_is_deleted, r_is_removed = id_utils.is_deleted_or_removed_text(reply.get("reply"))
        r_author_deleted, r_author_removed = id_utils.is_deleted_or_removed_text(reply_author)
        rows.append(
            {
                "subreddit": slug,
                "post_id": post_id,
                "comment_id": _prefixed("t1", reply.get("reply_id")),
                "parent_id": top_comment_id,
                "author": None if r_author_deleted else reply_author,
                "date_posted": reply.get("date_of_reply"),
                "comment_text": reply.get("reply"),
                "score": reply.get("num_upvotes"),
                "depth": 1,
                "is_mod": None,  # not present on reply objects in this dataset -- documented gap
                "is_automod": reply_author == "AutoModerator",
                "is_deleted": r_is_deleted or r_author_deleted,
                "is_removed": r_is_removed or r_author_removed,
                "num_replies": reply.get("num_replies"),
                "collected_at": raw_comment.get("timestamp"),
                "raw_json_path": raw_json_path,
            }
        )

    return rows


def load_raw_comments(slug: str) -> list[dict]:
    return [r for r, _ in _iter_raw_comments_with_source(slug)]


def build_comments_table(slugs: list[str] | None = None) -> pd.DataFrame:
    from .process_posts import discover_collected_subreddits

    if slugs is None:
        slugs = discover_collected_subreddits()

    rows = []
    for slug in slugs:
        for raw, source in _iter_raw_comments_with_source(slug):
            rows.extend(flatten_comment_tree(raw, slug, source))

    df = pd.DataFrame(rows, columns=COMMENTS_COLUMNS)
    if not df.empty:
        df["collected_at"] = df["collected_at"].fillna("")
        df = df.sort_values(["collected_at"]).drop_duplicates(subset=["comment_id"], keep="last")
        df = df.sort_values(["subreddit", "post_id", "depth", "date_posted"]).reset_index(drop=True)

    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(config.PROCESSED_DIR / "comments.csv", index=False)
    logger.info("Wrote %d comments to processed/comments.csv", len(df))
    return df


def main():
    parser = argparse.ArgumentParser(description="Build processed/comments.csv from raw JSONL.")
    parser.add_argument("--subreddit", action="append", default=None)
    args = parser.parse_args()
    slugs = [config.slugify(s) for s in args.subreddit] if args.subreddit else None
    build_comments_table(slugs)


if __name__ == "__main__":
    main()
