"""processed/posts.csv + processed/comments.csv + processed/users.csv -> processed/monthly_activity.csv.

new_users = users whose GLOBAL first_seen month (across all subreddits, from users.csv)
equals this row's month -- i.e. first appearance anywhere in our collected dataset, per
spec section 7, attributed to whichever subreddit(s) they were active in that month.
"""
from __future__ import annotations

import argparse

import pandas as pd

from . import config
from .log_utils import get_logger

logger = get_logger(__name__)

MONTHLY_COLUMNS = ["month", "subreddit", "posts", "comments", "unique_active_users", "new_users"]


def build_monthly_activity_table(posts_df: pd.DataFrame, comments_df: pd.DataFrame, users_df: pd.DataFrame) -> pd.DataFrame:
    def with_month(df: pd.DataFrame, author_col: str, date_col: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["username", "subreddit", "month"])
        out = df.dropna(subset=[author_col, date_col])[[author_col, "subreddit", date_col]].copy()
        out["month"] = out[date_col].str.slice(0, 7)
        return out.rename(columns={author_col: "username"})[["username", "subreddit", "month"]]

    posts_activity = with_month(posts_df, "author", "date_posted")
    comments_activity = with_month(comments_df, "author", "date_posted")

    posts_counts = posts_activity.groupby(["month", "subreddit"]).size().rename("posts") if not posts_activity.empty else pd.Series(dtype=int, name="posts")
    comments_counts = comments_activity.groupby(["month", "subreddit"]).size().rename("comments") if not comments_activity.empty else pd.Series(dtype=int, name="comments")

    combined = pd.concat([posts_activity, comments_activity], ignore_index=True)
    if combined.empty:
        empty = pd.DataFrame(columns=MONTHLY_COLUMNS)
        empty.to_csv(config.PROCESSED_DIR / "monthly_activity.csv", index=False)
        return empty

    unique_active = combined.groupby(["month", "subreddit"])["username"].nunique().rename("unique_active_users")

    first_seen_month = {}
    if not users_df.empty:
        for _, row in users_df.iterrows():
            fs = row.get("first_seen")
            if fs and isinstance(fs, str) and len(fs) >= 7:
                first_seen_month[row["username"]] = fs[:7]

    combined["first_seen_month"] = combined["username"].map(first_seen_month)
    new_users_df = combined[combined["month"] == combined["first_seen_month"]]
    new_users = new_users_df.groupby(["month", "subreddit"])["username"].nunique().rename("new_users") if not new_users_df.empty else pd.Series(dtype=int, name="new_users")

    result = pd.concat([posts_counts, comments_counts, unique_active, new_users], axis=1).fillna(0).reset_index()
    for col in ("posts", "comments", "unique_active_users", "new_users"):
        result[col] = result[col].astype(int)

    result = result[MONTHLY_COLUMNS].sort_values(["subreddit", "month"]).reset_index(drop=True)

    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    result.to_csv(config.PROCESSED_DIR / "monthly_activity.csv", index=False)
    logger.info("Wrote %d monthly_activity rows to processed/monthly_activity.csv", len(result))
    return result


def main():
    from .build_users import build_users_table
    from .process_comments import build_comments_table
    from .process_posts import build_posts_table, discover_collected_subreddits

    parser = argparse.ArgumentParser(description="Build processed/monthly_activity.csv.")
    parser.parse_args()

    slugs = discover_collected_subreddits()
    posts_df = build_posts_table(slugs)
    comments_df = build_comments_table(slugs)
    users_df = build_users_table(posts_df, comments_df, slugs)
    build_monthly_activity_table(posts_df, comments_df, users_df)


if __name__ == "__main__":
    main()
