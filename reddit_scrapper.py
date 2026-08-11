import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


load_dotenv()

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")

if not APIFY_API_TOKEN:
    raise ValueError("APIFY_API_TOKEN is missing from .env")


ACTOR_ID = "prodiger~reddit-scraper"

API_URL = (
    f"https://api.apify.com/v2/actors/"
    f"{ACTOR_ID}/run-sync-get-dataset-items"
)


payload = {
    "includeComments": False,
    "proxyConfiguration": {
        "useApifyProxy": True,
        "apifyProxyGroups": [
            "RESIDENTIAL"
        ]
    },
    "urls": [
        {
            "url": "https://www.reddit.com/r/technology/"
        }
    ]
}


headers = {
    "Content-Type": "application/json"
}


def scrape_reddit():

    print("Starting Reddit scraper...")
    print("Actor:", ACTOR_ID)
    print("Target: r/technology")

    response = requests.post(
        API_URL,
        params={
            "token": APIFY_API_TOKEN
        },
        headers=headers,
        json=payload,
        timeout=600
    )

    print("HTTP Status:", response.status_code)

    if response.status_code not in (200, 201):
        print("\nApify response:")
        print(response.text)

    response.raise_for_status()

    data = response.json()

    print("Items received:", len(data))

    return data


def save_raw_data(data):

    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "technology.json"

    with open(output_file, "w", encoding="utf-8") as file:
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
        print("WARNING: Apify returned zero items.")
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