"""HTTP client for the Arctic Shift Reddit archive API (github.com/ArthurHeitmann/arctic_shift).

No API key required -- a plain paginated REST GET per page, unlike BrightData's async
trigger/poll/download flow. Added as an alternative source because BrightData's
discover_new/subreddit_url mode against kolkatacity fails on this network with a TLS
interception error (SSLCertVerificationError: self signed certificate in certificate chain),
while arctic-shift.photon-reddit.com is unaffected by whatever is intercepting
api.brightdata.com traffic here.
"""
from __future__ import annotations

import time
from datetime import date, datetime, timezone

import requests

BASE_URL = "https://arctic-shift.photon-reddit.com/api"
PAGE_LIMIT = 100
REQUEST_DELAY_S = 0.5  # "a couple requests per second" is the API's own stated safe ceiling

# This network has repeatedly dropped outbound HTTPS for tens of seconds to minutes at a time
# (SSL cert-chain errors, connection resets, read timeouts -- see README/collection_log.csv).
# Without a retry, one such window turns every in-flight post into a permanent "failed" --
# racing through the rest of a multi-hour run's backlog in seconds with nothing collected.
# Retrying with backoff lets a transient outage cost minutes instead of the whole run.
#
# Arctic Shift's own 422 "Unprocessable Entity" is ALSO transient, confirmed empirically:
# identical requests (same params, same cursor) that 422'd were retried seconds later with no
# change and returned 200 with valid data. It is not a real "this data/cursor is invalid"
# rejection -- treat it the same as a dropped connection, not as a permanent per-item failure.
MAX_RETRIES = 6
# Two distinct failure modes need different pacing: a dropped-connection network outage takes
# tens of seconds to minutes to clear (needs the long tail), but Arctic Shift's own quick 503s
# (confirmed empirically: sub-second responses, ~30% instantaneous error rate under load) often
# clear on an immediate retry. Starting the backoff near-instant instead of at 5s recovers the
# common case fast while still escalating to the long tail for a real outage.
RETRY_BACKOFF_S = (1, 2, 5, 15, 30, 60)  # total worst case ~1.9 min before giving up on one call

_RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


def _get(path: str, params: dict) -> list[dict]:
    last_exc = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.get(f"{BASE_URL}{path}", params=params, timeout=60)
        except _RETRYABLE_EXCEPTIONS as exc:
            last_exc = exc
        else:
            if resp.status_code == 422 or resp.status_code == 429 or resp.status_code >= 500:
                last_exc = requests.exceptions.HTTPError(f"{resp.status_code} from Arctic Shift", response=resp)
            else:
                resp.raise_for_status()
                body = resp.json()
                if body.get("error"):
                    raise RuntimeError(f"Arctic Shift API error: {body['error']}")
                return body.get("data") or []

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_S[attempt])

    raise last_exc


def _epoch(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def _paginate(path: str, params: dict) -> list[dict]:
    """Page through a search endpoint to exhaustion. _get already retries transient failures
    (connection errors, 422/429/5xx) per page, so a page that still raises after retries is a
    real failure and should propagate -- the caller's resumable state machine handles it."""
    results: list[dict] = []
    while True:
        page = _get(path, params)
        if not page:
            break
        results.extend(page)
        if len(page) < PAGE_LIMIT:
            break
        params = {**params, "after": page[-1]["created_utc"] + 1}
        time.sleep(REQUEST_DELAY_S)
    return results


def fetch_posts(subreddit: str, after: date, before: date) -> list[dict]:
    """All posts in [after, before] (inclusive calendar days), paginated to exhaustion."""
    params = {
        "subreddit": subreddit,
        "after": after.isoformat(),
        "before": _epoch(before) + 86400,
        "limit": PAGE_LIMIT,
        "sort": "asc",
    }
    return _paginate("/posts/search", params)


def fetch_comments_for_post(post_id_bare: str) -> list[dict]:
    """All comments (flat, unthreaded) under one post. post_id_bare has no t3_ prefix."""
    params = {"link_id": f"t3_{post_id_bare}", "limit": PAGE_LIMIT, "sort": "asc"}
    return _paginate("/comments/search", params)


def fetch_subreddit_about(subreddit: str) -> dict | None:
    """The subreddit's own "about" object (created_utc, description, subscribers, title, ...).

    Does NOT carry rules, moderator list, pinned posts, or mod announcements -- Reddit never
    puts those on the subreddit object itself, and Arctic Shift doesn't mirror the separate
    endpoints that do (about/rules.json, about/moderators.json, the hot listing).
    """
    results = _get("/subreddits/search", {"subreddit": subreddit, "limit": 1})
    return results[0] if results else None
