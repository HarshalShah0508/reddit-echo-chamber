import json
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv


load_dotenv()


BRIGHTDATA_API_KEY = os.getenv("BRIGHTDATA_API_KEY")

if not BRIGHTDATA_API_KEY:
    raise ValueError("BRIGHTDATA_API_KEY is missing from .env")


DATASET_ID = os.getenv(
    "BRIGHTDATA_REDDIT_POSTS_DATASET_ID",
    "gd_lvz8ah06191smkebj4"
)

BASE_URL = os.getenv(
    "BRIGHTDATA_DATASETS_BASE_URL",
    "https://api.brightdata.com/datasets/v3"
)


TRIGGER_URL = f"{BASE_URL}/trigger"
PROGRESS_URL = f"{BASE_URL}/progress"
SNAPSHOT_URL = f"{BASE_URL}/snapshot"


# ---------------------------------------------------------
# Reddit target
# ---------------------------------------------------------

SUBREDDIT_URL = "https://www.reddit.com/r/technology/"

# We will only keep the first 5 records locally.
MAX_POSTS = 5


# ---------------------------------------------------------
# Bright Data request payload
# ---------------------------------------------------------

payload = [
    {
        "url": SUBREDDIT_URL
    }
]


headers = {
    "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
    "Content-Type": "application/json"
}


def scrape_reddit():

    print("Starting Bright Data Reddit scraper...")
    print("Dataset:", DATASET_ID)
    print("Target: r/technology")
    print("Maximum posts to save:", MAX_POSTS)

    # ---------------------------------------------------------
    # STEP 1: Trigger Bright Data scraping job
    # ---------------------------------------------------------

    response = requests.post(
        TRIGGER_URL,
        params={
            "dataset_id": DATASET_ID,
            "format": "json",
            "type": "discover_new",
            "discover_by": "subreddit_url"
        },
        headers=headers,
        json=payload,
        timeout=120
    )

    print("Trigger HTTP Status:", response.status_code)

    if response.status_code not in (200, 201):
        print("\nBright Data response:")
        print(response.text)

    response.raise_for_status()

    trigger_data = response.json()

    snapshot_id = trigger_data.get("snapshot_id")

    if not snapshot_id:
        raise ValueError(
            f"No snapshot_id returned by Bright Data:\n{trigger_data}"
        )

    print("Snapshot ID:", snapshot_id)

    # ---------------------------------------------------------
    # STEP 2: Monitor snapshot
    # ---------------------------------------------------------

    print("\nChecking snapshot status...")

    while True:

        progress_response = requests.get(
            f"{PROGRESS_URL}/{snapshot_id}",
            headers={
                "Authorization": f"Bearer {BRIGHTDATA_API_KEY}"
            },
            timeout=60
        )

        progress_response.raise_for_status()

        progress_data = progress_response.json()

        status = progress_data.get("status")

        print("Status:", status)

        if status == "ready":
            print("Snapshot is ready.")
            break

        if status in ("failed", "error"):
            raise RuntimeError(
                f"Bright Data scraping failed:\n{progress_data}"
            )

        time.sleep(10)

    # ---------------------------------------------------------
    # STEP 3: Download snapshot
    # ---------------------------------------------------------

    print("\nDownloading Reddit data...")

    snapshot_response = requests.get(
        f"{SNAPSHOT_URL}/{snapshot_id}",
        params={
            "format": "json"
        },
        headers={
            "Authorization": f"Bearer {BRIGHTDATA_API_KEY}"
        },
        timeout=120
    )

    print(
        "Download HTTP Status:",
        snapshot_response.status_code
    )

    if snapshot_response.status_code != 200:
        print("\nBright Data response:")
        print(snapshot_response.text)

    snapshot_response.raise_for_status()

    data = snapshot_response.json()

    print("Total records returned by Bright Data:", len(data))

    # ---------------------------------------------------------
    # STEP 4: Keep only first 5 posts locally
    # ---------------------------------------------------------

    limited_data = data[:MAX_POSTS]

    print(
        "Records kept for this test:",
        len(limited_data)
    )

    return limited_data


def save_raw_data(data):

    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "reddit_test.json"

    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            indent=2,
            ensure_ascii=False
        )

    print("Saved:", output_file)


if __name__ == "__main__":

    reddit_data = scrape_reddit()

    if not reddit_data:

        print(
            "WARNING: Bright Data returned zero items."
        )

    else:

        print("\nFirst Reddit item:")

        print(
            json.dumps(
                reddit_data[0],
                indent=2,
                ensure_ascii=False
            )
        )

    save_raw_data(reddit_data)