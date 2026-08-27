"""collection_state.csv: tracks which (subreddit, data_type, month) windows are done.

Keyed by (subreddit, data_type, month) rather than the literal (subreddit,start_date,end_date)
tuple shown as an example in the task spec. A literal-tuple key would force a full month
re-fetch every time `end_date` advances mid-month on a later run; keying by month lets us
track exactly how much of a given month has been covered so far and fetch only the delta.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from . import config, date_utils

STATE_COLUMNS = [
    "subreddit",
    "data_type",
    "month",
    "period_start",
    "period_end",
    "status",
    "snapshot_id",
    "records_collected",
    "last_attempt_at",
    "error_message",
]


def _empty_state() -> pd.DataFrame:
    return pd.DataFrame(columns=STATE_COLUMNS)


def load_state() -> pd.DataFrame:
    path: Path = config.STATE_CSV
    if not path.exists():
        return _empty_state()
    df = pd.read_csv(path, dtype=str)
    for col in STATE_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[STATE_COLUMNS]


def _save_state(df: pd.DataFrame) -> None:
    config.STATE_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(config.STATE_CSV, index=False)


def get_row(subreddit: str, data_type: str, month: str) -> dict | None:
    slug = config.slugify(subreddit)
    df = load_state()
    match = df[(df["subreddit"] == slug) & (df["data_type"] == data_type) & (df["month"] == month)]
    if match.empty:
        return None
    return match.iloc[0].to_dict()


def is_month_complete(subreddit: str, data_type: str, month: str) -> bool:
    row = get_row(subreddit, data_type, month)
    return bool(row) and row.get("status") == "complete"


def pending_subrange(
    subreddit: str,
    data_type: str,
    month_start: date,
    month_end_calendar: date,
    requested_end: date,
) -> tuple[date, date] | None:
    """Return the (start, end) delta that still needs fetching for this month, or None to skip.

    - No existing row: fetch (month_start, min(month_end_calendar, requested_end)).
    - status == complete: skip.
    - status in (partial, pending): fetch only the uncovered delta after the existing period_end.
    - status == failed: retry the whole requested window for this month.
    """
    target_end = min(month_end_calendar, requested_end)
    month = date_utils.month_key(month_start)
    row = get_row(subreddit, data_type, month)

    if row is None:
        return month_start, target_end

    status = row.get("status")
    if status == "complete":
        return None
    if status == "failed":
        return month_start, target_end
    # partial / pending: fetch the delta beyond what's already covered
    existing_end_raw = row.get("period_end")
    if not existing_end_raw or pd.isna(existing_end_raw):
        return month_start, target_end
    existing_end = date_utils.parse_date(str(existing_end_raw))
    if target_end <= existing_end:
        return None
    new_start = existing_end + timedelta(days=1)
    return new_start, target_end


def upsert(
    subreddit: str,
    data_type: str,
    month: str,
    period_start: date,
    period_end: date,
    status: str,
    snapshot_id: str | None = None,
    records_collected: int | None = None,
    error_message: str | None = None,
) -> None:
    slug = config.slugify(subreddit)
    df = load_state()
    mask = (df["subreddit"] == slug) & (df["data_type"] == data_type) & (df["month"] == month)

    new_row = {
        "subreddit": slug,
        "data_type": data_type,
        "month": month,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "status": status,
        "snapshot_id": snapshot_id or "",
        "records_collected": records_collected if records_collected is not None else "",
        "last_attempt_at": date_utils.iso_now(),
        "error_message": error_message or "",
    }

    if mask.any():
        for col, val in new_row.items():
            df.loc[mask, col] = val
    else:
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    _save_state(df)
