"""Logging: console/file logger plus the collection_log.csv audit trail."""
from __future__ import annotations

import csv
import logging
from pathlib import Path

from . import config

LOG_COLUMNS = [
    "collection_date",
    "subreddit",
    "period_start",
    "period_end",
    "data_type",
    "api_tool",
    "query_filter",
    "records_collected",
    "status",
    "errors_or_limitations",
]

def get_logger(name: str = "reddit_kolkata") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(config.LOGS_DIR / "pipeline.log")
        ch = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)
        logger.propagate = False
    return logger


def append_collection_log(row: dict) -> None:
    """Append one row to collection_log.csv, creating the header if the file is new.

    `row` keys must be a subset of LOG_COLUMNS; missing keys are written blank.
    """
    path: Path = config.LOG_CSV
    is_new = not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_COLUMNS)
        if is_new:
            writer.writeheader()
        writer.writerow({col: row.get(col, "") for col in LOG_COLUMNS})
