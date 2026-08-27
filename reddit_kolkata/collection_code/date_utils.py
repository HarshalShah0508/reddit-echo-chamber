"""Date parsing and calendar-month chunking helpers."""
from __future__ import annotations

from datetime import date, datetime, timezone

from dateutil.relativedelta import relativedelta


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def today_utc() -> date:
    return datetime.now(timezone.utc).date()


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def month_key(d: date) -> str:
    return d.strftime("%Y-%m")


def month_bounds(d: date) -> tuple[date, date]:
    """First and last calendar day of d's month."""
    first = d.replace(day=1)
    last = first + relativedelta(months=1, days=-1)
    return first, last


def month_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into calendar-month-aligned (chunk_start, chunk_end) tuples.

    The first chunk starts at `start` (may be mid-month), the last chunk ends at `end`
    (may be mid-month), every interior chunk is a full calendar month.
    """
    if start > end:
        return []
    chunks = []
    cursor = start
    while cursor <= end:
        _, month_end = month_bounds(cursor)
        chunk_end = min(month_end, end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + relativedelta(days=1)
    return chunks
