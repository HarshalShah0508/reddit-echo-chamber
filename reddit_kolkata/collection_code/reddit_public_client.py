"""Best-effort client for Reddit's public unauthenticated .json endpoints.

Live-tested on 2026-08-27 from this environment: www.reddit.com/*.json returns HTTP 403
(Reddit's anti-bot wall) regardless of User-Agent, and old.reddit.com/*.json returns a 200
login-wall HTML page instead of JSON. Both are documented as a known API gap in the README.
These functions are kept so the pipeline auto-upgrades to live rules/moderators/flair data
without code changes if run from an environment where Reddit's endpoints aren't blocked, or
once proper OAuth app credentials are added.
"""
from __future__ import annotations

import time

import requests

from . import config
from .log_utils import get_logger

logger = get_logger(__name__)

_HEADERS = {"User-Agent": config.REDDIT_PUBLIC_USER_AGENT}


def _get_json(url: str) -> dict | None:
    try:
        response = requests.get(url, headers=_HEADERS, timeout=15)
        time.sleep(config.REDDIT_PUBLIC_REQUEST_DELAY_S)
        if response.status_code != 200:
            logger.warning("Reddit public endpoint %s returned %s", url, response.status_code)
            return None
        return response.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("Reddit public endpoint %s failed: %s", url, exc)
        return None


def get_about(subreddit: str) -> dict | None:
    slug = config.slugify(subreddit)
    return _get_json(f"https://www.reddit.com/r/{slug}/about.json")


def get_rules(subreddit: str) -> list[dict]:
    slug = config.slugify(subreddit)
    data = _get_json(f"https://www.reddit.com/r/{slug}/about/rules.json")
    return (data or {}).get("rules", [])


def get_moderators(subreddit: str) -> list[dict]:
    slug = config.slugify(subreddit)
    data = _get_json(f"https://www.reddit.com/r/{slug}/about/moderators.json")
    if not data:
        return []
    return data.get("data", {}).get("children", [])


def get_hot_listing(subreddit: str, limit: int = 25) -> list[dict]:
    slug = config.slugify(subreddit)
    data = _get_json(f"https://www.reddit.com/r/{slug}/hot.json?limit={limit}")
    if not data:
        return []
    return [child.get("data", {}) for child in data.get("data", {}).get("children", [])]
