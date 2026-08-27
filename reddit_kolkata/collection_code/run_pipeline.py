"""End-to-end orchestrator. Run from the project root as:

    python -m reddit_kolkata.collection_code.run_pipeline \
        --subreddit kolkata --subreddit kolkatacity \
        --start-date 2025-04-01

Rerunning with the same or a wider date range only fetches what isn't already marked
"complete" in collection_state.csv. Adding a new subreddit later needs no code change --
just add another --subreddit flag; the processing/aggregation steps always re-scan every
subreddit ever collected under raw/, not just the ones passed on this invocation.
"""
from __future__ import annotations

import argparse

from . import date_utils
from .build_monthly_activity import build_monthly_activity_table
from .build_users import build_users_table
from .collect_comments import collect_comments_for_range
from .collect_posts import collect_posts_for_range
from .collect_subreddit_metadata import collect_subreddit_metadata
from .log_utils import get_logger
from .process_comments import build_comments_table
from .process_posts import build_posts_table, discover_collected_subreddits
from .validate import run_validation

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run the full Reddit collection pipeline.")
    parser.add_argument("--subreddit", action="append", required=True, help="Repeatable, e.g. --subreddit kolkata --subreddit kolkatacity")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--skip-comments", action="store_true")
    parser.add_argument("--skip-metadata", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--batch-size", type=int, default=None)
    args = parser.parse_args()

    start = date_utils.parse_date(args.start_date)
    end = date_utils.parse_date(args.end_date) if args.end_date else date_utils.today_utc()

    for subreddit in args.subreddit:
        logger.info("=== %s: collecting posts %s to %s ===", subreddit, start, end)
        post_results = collect_posts_for_range(subreddit, start, end, force=args.force)
        for r in post_results:
            logger.info("posts %s", r)

        if not args.skip_metadata:
            logger.info("=== %s: collecting subreddit metadata ===", subreddit)
            collect_subreddit_metadata(subreddit)

        if not args.skip_comments:
            logger.info("=== %s: collecting comments %s to %s ===", subreddit, start, end)
            kwargs = {"force": args.force}
            if args.batch_size:
                kwargs["batch_size"] = args.batch_size
            comment_results = collect_comments_for_range(subreddit, start, end, **kwargs)
            for r in comment_results:
                logger.info("comments %s", r)

    all_slugs = discover_collected_subreddits()
    logger.info("=== Processing all collected subreddits: %s ===", all_slugs)
    posts_df = build_posts_table(all_slugs)
    comments_df = build_comments_table(all_slugs)
    users_df = build_users_table(posts_df, comments_df, all_slugs)
    build_monthly_activity_table(posts_df, comments_df, users_df)

    logger.info("=== Running validation ===")
    run_validation(all_slugs, start, end)

    print(f"\nDone. posts={len(posts_df)} comments={len(comments_df)} users={len(users_df)} subreddits={all_slugs}")


if __name__ == "__main__":
    main()
