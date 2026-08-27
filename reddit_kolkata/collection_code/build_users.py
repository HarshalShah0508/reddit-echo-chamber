"""processed/posts.csv + processed/comments.csv -> processed/users.csv.

Columns are generated dynamically per discovered subreddit slug (never hardcoded), so for
today's two subreddits this resolves to exactly the spec's named
posts_kolkata, comments_kolkata, posts_kolkatacity, comments_kolkatacity, and a third
subreddit added later grows the column set automatically with no code change.

Only uses posts/comments already collected on disk -- never fetches Reddit-wide user
histories, per spec section 6.
"""
from __future__ import annotations

import argparse

import pandas as pd

from . import config
from .log_utils import get_logger

logger = get_logger(__name__)


def build_users_table(posts_df: pd.DataFrame, comments_df: pd.DataFrame, slugs: list[str]) -> pd.DataFrame:
    activity = []

    if not posts_df.empty:
        p = posts_df.dropna(subset=["author"])[["author", "subreddit", "date_posted"]].copy()
        p["type"] = "post"
        activity.append(p.rename(columns={"date_posted": "date"}))

    if not comments_df.empty:
        c = comments_df.dropna(subset=["author"])[["author", "subreddit", "date_posted"]].copy()
        c["type"] = "comment"
        activity.append(c.rename(columns={"date_posted": "date"}))

    if not activity:
        empty_cols = ["username"] + [f"{t}_{s}" for s in slugs for t in ("posts", "comments")] + ["total_posts", "total_comments", "first_seen", "last_seen"]
        df = pd.DataFrame(columns=empty_cols)
        df.to_csv(config.PROCESSED_DIR / "users.csv", index=False)
        return df

    combined = pd.concat(activity, ignore_index=True)
    combined = combined.rename(columns={"author": "username"})

    counts = combined.groupby(["username", "subreddit", "type"]).size().unstack(fill_value=0)
    counts = counts.reset_index()
    for col in ("post", "comment"):
        if col not in counts.columns:
            counts[col] = 0

    wide = counts.pivot_table(index="username", columns="subreddit", values=["post", "comment"], fill_value=0)
    wide.columns = [f"{metric}s_{slug}" for metric, slug in wide.columns]
    wide = wide.reset_index()

    for slug in slugs:
        for metric in ("posts", "comments"):
            col = f"{metric}_{slug}"
            if col not in wide.columns:
                wide[col] = 0

    posts_cols = [f"posts_{s}" for s in slugs]
    comments_cols = [f"comments_{s}" for s in slugs]
    wide["total_posts"] = wide[posts_cols].sum(axis=1)
    wide["total_comments"] = wide[comments_cols].sum(axis=1)

    first_last = combined.groupby("username")["date"].agg(first_seen="min", last_seen="max").reset_index()
    users_df = wide.merge(first_last, on="username", how="left")

    ordered_cols = ["username"] + [c for s in slugs for c in (f"posts_{s}", f"comments_{s}")] + ["total_posts", "total_comments", "first_seen", "last_seen"]
    users_df = users_df[ordered_cols].sort_values("username").reset_index(drop=True)

    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    users_df.to_csv(config.PROCESSED_DIR / "users.csv", index=False)
    logger.info("Wrote %d users to processed/users.csv", len(users_df))
    return users_df


def summarize_user_overlap(users_df: pd.DataFrame, slugs: list[str]) -> dict:
    if users_df.empty:
        return {"unique_per_subreddit": {s: 0 for s in slugs}, "in_multiple": 0, "only_in": {s: 0 for s in slugs}}

    active_in = {}
    for s in slugs:
        mask = (users_df[f"posts_{s}"] > 0) | (users_df[f"comments_{s}"] > 0)
        active_in[s] = set(users_df.loc[mask, "username"])

    unique_per_subreddit = {s: len(active_in[s]) for s in slugs}
    all_active = set().union(*active_in.values()) if active_in else set()
    in_multiple = sum(1 for u in all_active if sum(u in active_in[s] for s in slugs) > 1)
    only_in = {
        s: len(active_in[s] - set().union(*(active_in[o] for o in slugs if o != s))) if len(slugs) > 1 else len(active_in[s])
        for s in slugs
    }

    return {"unique_per_subreddit": unique_per_subreddit, "in_multiple": in_multiple, "only_in": only_in}


def main():
    from .process_comments import build_comments_table
    from .process_posts import build_posts_table, discover_collected_subreddits

    parser = argparse.ArgumentParser(description="Build processed/users.csv from processed posts/comments.")
    parser.parse_args()

    slugs = discover_collected_subreddits()
    posts_df = build_posts_table(slugs)
    comments_df = build_comments_table(slugs)
    users_df = build_users_table(posts_df, comments_df, slugs)
    print(summarize_user_overlap(users_df, slugs))


if __name__ == "__main__":
    main()
