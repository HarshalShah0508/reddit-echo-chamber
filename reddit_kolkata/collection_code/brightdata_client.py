"""Thin wrapper around BrightData's Datasets API v3 trigger/poll/download flow.

This is the ONLY module allowed to call api.brightdata.com. Every collection script
routes through fetch_dataset() so request patterns (and any future rate-limit changes)
live in exactly one place.

Two trigger modes are used by this pipeline, confirmed by live probing on 2026-08-27:
  - "discover_new" + discover_by="subreddit_url": used for posts (gd_lvz8ah06191smkebj4).
    Accepts a per-item payload of {"url", "sort_by", "sort_by_time", "keyword", "start_date"}.
  - plain url_collection (no "type"/"discover_by" needed): used for comments
    (BRIGHTDATA_REDDIT_COMMENTS_DATASET_ID). Payload is a list of {"url": post_url}.
    Attempting discover_new on this dataset returns HTTP 400:
    "This dataset does not support discovery. Supported types: ['url_collection']".
"""
from __future__ import annotations

import time

import requests

from . import config
from .log_utils import get_logger

logger = get_logger(__name__)


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {config.BRIGHTDATA_API_KEY}",
        "Content-Type": "application/json",
        "Connection": "close",
    }


def _request_with_retry(method: str, url: str, max_attempts: int = 4, **kwargs) -> requests.Response:
    """requests/urllib3 pools keep-alive connections per host; BrightData's side has been
    observed to reset those connections after a short idle gap (e.g. between 10s poll
    ticks), which surfaces as ConnectionResetError on an otherwise-healthy network path
    (plain curl calls to the same host never reproduce it). Retrying a fresh connection
    clears it every time, so treat it as transient rather than a real failure.
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return requests.request(method, url, **kwargs)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            delay = 2 * attempt
            logger.warning(
                "%s %s connection error (attempt %d/%d), retrying in %ds: %s",
                method, url, attempt, max_attempts, delay, exc,
            )
            time.sleep(delay)
    raise last_exc


def trigger_snapshot(dataset_id: str, payload: list[dict], discover_by: str | None = None) -> str:
    params = {"dataset_id": dataset_id, "format": "json"}
    if discover_by:
        params["type"] = "discover_new"
        params["discover_by"] = discover_by

    response = _request_with_retry(
        "post",
        f"{config.BASE_URL}/trigger",
        params=params,
        headers=_headers(),
        json=payload,
        timeout=120,
    )
    if response.status_code not in (200, 201):
        logger.error("BrightData trigger failed (%s): %s", response.status_code, response.text[:500])
    response.raise_for_status()

    data = response.json()
    snapshot_id = data.get("snapshot_id")
    if not snapshot_id:
        raise ValueError(f"No snapshot_id returned by BrightData: {data}")
    return snapshot_id


def poll_snapshot(
    snapshot_id: str,
    poll_interval_s: int = config.POLL_INTERVAL_S,
    timeout_s: int = config.POLL_TIMEOUT_S,
) -> str:
    elapsed = 0
    while elapsed < timeout_s:
        response = _request_with_retry(
            "get",
            f"{config.BASE_URL}/progress/{snapshot_id}",
            headers={"Authorization": f"Bearer {config.BRIGHTDATA_API_KEY}", "Connection": "close"},
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        status = data.get("status")

        if status == "ready":
            return status
        if status in ("failed", "error"):
            raise RuntimeError(f"BrightData snapshot {snapshot_id} failed: {data}")

        time.sleep(poll_interval_s)
        elapsed += poll_interval_s

    raise TimeoutError(f"BrightData snapshot {snapshot_id} did not become ready within {timeout_s}s")


def download_snapshot(snapshot_id: str, max_attempts: int = 5, retry_delay_s: int = 15) -> list[dict]:
    """Download a ready snapshot, guarding against a race condition observed live on
    2026-08-27: BrightData's progress endpoint reported status="ready" before the snapshot's
    data was fully materialized, and an immediate download returned 2 malformed placeholder
    items instead of the real ~1000-record dataset (confirmed by re-querying the same
    snapshot_id moments later and getting the full, well-formed result). We treat a
    non-dict item anywhere in the response as evidence the snapshot isn't truly ready yet
    and retry after a short delay rather than trusting HTTP 200 + "ready" status alone.
    """
    last_data = []
    for attempt in range(1, max_attempts + 1):
        response = _request_with_retry(
            "get",
            f"{config.BASE_URL}/snapshot/{snapshot_id}",
            params={"format": "json"},
            headers={"Authorization": f"Bearer {config.BRIGHTDATA_API_KEY}", "Connection": "close"},
            timeout=120,
        )
        response.raise_for_status()
        data = response.json()
        last_data = data

        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            return data

        logger.warning(
            "Snapshot %s attempt %d/%d returned malformed content (not a list-of-dicts, len=%s) -- "
            "retrying in %ds, likely not fully materialized yet.",
            snapshot_id, attempt, max_attempts, len(data) if hasattr(data, "__len__") else "n/a", retry_delay_s,
        )
        time.sleep(retry_delay_s)

    raise RuntimeError(f"Snapshot {snapshot_id} still returned malformed content after {max_attempts} attempts: {last_data!r:.500}")


def fetch_dataset(dataset_id: str, payload: list[dict], discover_by: str | None = None) -> tuple[str, list[dict]]:
    """Trigger + poll + download in one call. Returns (snapshot_id, records)."""
    snapshot_id = trigger_snapshot(dataset_id, payload, discover_by=discover_by)
    logger.info("Triggered snapshot %s on dataset %s", snapshot_id, dataset_id)
    poll_snapshot(snapshot_id)
    data = download_snapshot(snapshot_id)
    logger.info("Downloaded %d records from snapshot %s", len(data), snapshot_id)
    return snapshot_id, data
