"""processed/posts.csv + processed/comments.csv -> partitioned Parquet.

Reads the already-built, already-deduplicated processed CSVs (NOT the raw JSONL, and NOT
process_posts.build_posts_table()/process_comments.build_comments_table(), which rebuild
those CSVs from raw as a side effect every call -- this script only consumes what's already
on disk). Normalizes dtypes/timestamps, re-applies ID-based dedup defensively, and writes
one Parquet file per (subreddit, month) partition in Hive layout
(parquet/<dataset>/subreddit=<slug>/month=<YYYY-MM>/part-0000.parquet) so DuckDB's
hive_partitioning=1 can reconstruct subreddit/month as queryable columns without us storing
them in the file body.

Only partitions present in the current input are ever written; partitions for other
subreddits/months already on disk are never touched or deleted, and a partition whose rows
are unchanged since the last run is skipped (tracked via parquet/_manifest.json), so adding
a new subreddit or extending the collection period never reprocesses unrelated data.
"""
from __future__ import annotations

import argparse
import hashlib
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import config, jsonl_utils
from .log_utils import get_logger

logger = get_logger(__name__)

POSTS_INT_COLUMNS = ["score", "num_comments", "community_members_num", "post_karma", "num_related_posts"]
POSTS_BOOL_COLUMNS = ["is_crosspost", "is_stickied", "is_deleted", "is_removed"]
POSTS_TIMESTAMP_COLUMNS = ["date_posted", "collected_at"]
POSTS_ID_COL = "post_id"

COMMENTS_INT_COLUMNS = ["score", "depth", "num_replies"]
COMMENTS_BOOL_COLUMNS = ["is_mod", "is_automod", "is_deleted", "is_removed"]
COMMENTS_TIMESTAMP_COLUMNS = ["date_posted", "collected_at"]
COMMENTS_ID_COL = "comment_id"

DATASET_CONFIG = {
    "posts": {
        "csv_path": lambda: config.PROCESSED_DIR / "posts.csv",
        "parquet_dir": lambda: config.POSTS_PARQUET_DIR,
        "id_col": POSTS_ID_COL,
        "int_columns": POSTS_INT_COLUMNS,
        "bool_columns": POSTS_BOOL_COLUMNS,
        "timestamp_columns": POSTS_TIMESTAMP_COLUMNS,
    },
    "comments": {
        "csv_path": lambda: config.PROCESSED_DIR / "comments.csv",
        "parquet_dir": lambda: config.COMMENTS_PARQUET_DIR,
        "id_col": COMMENTS_ID_COL,
        "int_columns": COMMENTS_INT_COLUMNS,
        "bool_columns": COMMENTS_BOOL_COLUMNS,
        "timestamp_columns": COMMENTS_TIMESTAMP_COLUMNS,
    },
}


def load_source(dataset: str) -> pd.DataFrame:
    path = DATASET_CONFIG[dataset]["csv_path"]()
    if not path.exists():
        logger.warning("%s does not exist; nothing to convert for %r", path, dataset)
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def _parse_timestamp_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _to_nullable_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _to_nullable_bool(series: pd.Series) -> pd.Series:
    mapping = {"True": True, "False": False, "true": True, "false": False, "1": True, "0": False}
    return series.map(mapping).astype("boolean")


def normalize(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    if df.empty:
        return df
    cfg = DATASET_CONFIG[dataset]
    df = df.copy()
    for col in cfg["timestamp_columns"]:
        df[col] = _parse_timestamp_utc(df[col])
    for col in cfg["int_columns"]:
        df[col] = _to_nullable_int(df[col])
    for col in cfg["bool_columns"]:
        df[col] = _to_nullable_bool(df[col])
    # Cast every remaining column to pandas' nullable "string" dtype -- otherwise a column
    # that's entirely null within a single (subreddit, month) partition (e.g. external_domain
    # for a month with no link posts) gets written as Arrow's "null" type by
    # pa.Table.from_pandas's type inference, which conflicts with the "string" type the same
    # column gets in other partitions when DuckDB reads across the whole Hive-partitioned glob.
    typed_columns = set(cfg["timestamp_columns"]) | set(cfg["int_columns"]) | set(cfg["bool_columns"])
    for col in df.columns:
        if col not in typed_columns and col not in ("subreddit", "month"):
            df[col] = df[col].astype("string")
    return df


def dedupe(df: pd.DataFrame, id_col: str, sort_col: str = "collected_at") -> tuple[pd.DataFrame, int]:
    if df.empty:
        return df, 0
    before = len(df)
    df = df.sort_values(sort_col).drop_duplicates(subset=[id_col], keep="last")
    return df, before - len(df)


def add_month_column(df: pd.DataFrame, source_col: str = "date_posted") -> pd.DataFrame:
    if df.empty:
        df["month"] = pd.Series(dtype=str)
        return df
    df = df.copy()
    df["month"] = df[source_col].dt.strftime("%Y-%m")
    return df


def partition_fingerprint(df: pd.DataFrame, id_col: str) -> tuple[str, int]:
    keys = sorted(f"{row[id_col]}:{row['collected_at']}" for _, row in df[[id_col, "collected_at"]].iterrows())
    digest = hashlib.sha256("\n".join(keys).encode("utf-8")).hexdigest()
    return digest, len(df)


def load_manifest() -> dict:
    if not config.PARQUET_MANIFEST_PATH.exists():
        return {}
    return jsonl_utils.read_json(config.PARQUET_MANIFEST_PATH)


def save_manifest(manifest: dict) -> None:
    jsonl_utils.write_json_atomic(config.PARQUET_MANIFEST_PATH, manifest)


def write_partition(
    df: pd.DataFrame, dataset: str, subreddit: str, month: str, manifest: dict, force: bool,
) -> str:
    cfg = DATASET_CONFIG[dataset]
    manifest_key = f"{dataset}/{subreddit}/{month}"
    partition_dir = cfg["parquet_dir"]() / f"subreddit={subreddit}" / f"month={month}"
    final_path = partition_dir / "part-0000.parquet"

    fingerprint, row_count = partition_fingerprint(df, cfg["id_col"])
    existing = manifest.get(manifest_key)
    if (
        not force
        and existing is not None
        and existing.get("row_count") == row_count
        and existing.get("fingerprint") == fingerprint
        and final_path.exists()
    ):
        logger.info("%s: skipped (unchanged, %d rows)", manifest_key, row_count)
        return "skipped"

    partition_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df.drop(columns=["subreddit", "month"]), preserve_index=False)
    tmp_path = final_path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp_path)
    tmp_path.replace(final_path)

    manifest[manifest_key] = {
        "row_count": row_count,
        "fingerprint": fingerprint,
        "written_at": pd.Timestamp.utcnow().isoformat(),
    }
    logger.info("%s: written (%d rows)", manifest_key, row_count)
    return "written"


def convert_dataset(dataset: str, force: bool) -> dict:
    cfg = DATASET_CONFIG[dataset]
    df = load_source(dataset)
    rows_total = len(df)
    df = normalize(df, dataset)
    df, duplicates_dropped = dedupe(df, cfg["id_col"])
    df = add_month_column(df)

    unpartitionable = int(df["month"].isna().sum()) if not df.empty else 0
    if unpartitionable:
        logger.warning(
            "%s: %d rows have an unparseable date_posted and cannot be partitioned; excluded from Parquet output",
            dataset, unpartitionable,
        )
        df = df[df["month"].notna()]

    manifest = load_manifest()
    partitions_written = 0
    partitions_skipped = 0

    if not df.empty:
        for (subreddit, month), group in df.groupby(["subreddit", "month"]):
            result = write_partition(group, dataset, subreddit, month, manifest, force)
            if result == "written":
                partitions_written += 1
            else:
                partitions_skipped += 1
        save_manifest(manifest)

    summary = {
        "rows_total": rows_total,
        "duplicates_dropped": duplicates_dropped,
        "unpartitionable_rows": unpartitionable,
        "partitions_written": partitions_written,
        "partitions_skipped": partitions_skipped,
    }
    logger.info("%s conversion summary: %s", dataset, summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description="Convert processed/*.csv to partitioned Parquet.")
    parser.add_argument("--dataset", choices=["posts", "comments", "both"], default="both")
    parser.add_argument("--force", action="store_true", help="Rewrite every present partition, ignoring the unchanged-skip check.")
    args = parser.parse_args()

    datasets = ["posts", "comments"] if args.dataset == "both" else [args.dataset]
    summary = {ds: convert_dataset(ds, args.force) for ds in datasets}
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
