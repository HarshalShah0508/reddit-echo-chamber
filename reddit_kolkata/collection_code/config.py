"""Central configuration: paths, env vars, and shared constants for the pipeline."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# .env lives at the project root, one level above reddit_kolkata/
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / ".env")

ROOT = Path(__file__).resolve().parents[1]  # reddit_kolkata/
RAW_DIR = ROOT / "raw"
PROCESSED_DIR = ROOT / "processed"
METADATA_DIR = ROOT / "subreddit_metadata"
LOGS_DIR = ROOT / "logs"
STATE_CSV = ROOT / "collection_state.csv"
LOG_CSV = ROOT / "collection_log.csv"

BRIGHTDATA_API_KEY = os.getenv("BRIGHTDATA_API_KEY")
POSTS_DATASET_ID = os.getenv("BRIGHTDATA_REDDIT_POSTS_DATASET_ID")
COMMENTS_DATASET_ID = os.getenv("BRIGHTDATA_REDDIT_COMMENTS_DATASET_ID")
BASE_URL = os.getenv("BRIGHTDATA_DATASETS_BASE_URL", "https://api.brightdata.com/datasets/v3")

COMMENTS_BATCH_SIZE = 50
POLL_INTERVAL_S = 10
POLL_TIMEOUT_S = 3600

REDDIT_PUBLIC_USER_AGENT = "kolkata-research-pipeline/0.1 (contact: harshal05shah@gmail.com)"
REDDIT_PUBLIC_REQUEST_DELAY_S = 2


def slugify(subreddit: str) -> str:
    """Canonical folder-name form of a subreddit, e.g. 'r/KolkataCity' -> 'kolkatacity'."""
    return subreddit.strip().lower().lstrip("r/").strip("/")


def subreddit_url(subreddit: str) -> str:
    """Reddit's own display-cased URL isn't recoverable from the slug, so callers pass
    the display name they want to hit; this just normalizes trailing/leading slashes."""
    name = subreddit.strip().strip("/")
    if name.lower().startswith("r/"):
        name = name[2:]
    return f"https://www.reddit.com/r/{name}/"


for _dir in (RAW_DIR, PROCESSED_DIR, METADATA_DIR, LOGS_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

if not BRIGHTDATA_API_KEY:
    raise ValueError("BRIGHTDATA_API_KEY is missing from .env")
if not POSTS_DATASET_ID:
    raise ValueError("BRIGHTDATA_REDDIT_POSTS_DATASET_ID is missing from .env")
if not COMMENTS_DATASET_ID:
    raise ValueError("BRIGHTDATA_REDDIT_COMMENTS_DATASET_ID is missing from .env")
