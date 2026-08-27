# Reddit Data Collection Pipeline — r/kolkata & r/KolkataCity

Resumable, parameterized collection pipeline for Reddit posts, comments, and subreddit
metadata. Built for r/kolkata and r/KolkataCity, 2025-04-01 to present, but the collection
code never hardcodes those subreddit names — adding a new subreddit is a rerun with a
different `--subreddit` flag, not a code change.

## API / tool / data source used

- **Posts**: [BrightData Datasets API v3](https://docs.brightdata.com/api-reference/marketplace-dataset-api/trigger-a-collection-or-discovery), dataset `gd_lvz8ah06191smkebj4` (env `BRIGHTDATA_REDDIT_POSTS_DATASET_ID`). Discovery mode: `discover_new` / `discover_by=subreddit_url`.
- **Comments**: BrightData Datasets API v3, dataset id in env `BRIGHTDATA_REDDIT_COMMENTS_DATASET_ID`. This dataset only supports `url_collection` mode (a plain list of known post URLs) — it explicitly rejects `discover_new` with HTTP 400 `"This dataset does not support discovery. Supported types: ['url_collection']"`.
- **Subreddit metadata**: Reddit's public `.json` endpoints (`reddit.com/r/<sub>/about.json`, `about/rules.json`, `about/moderators.json`, `hot.json`) as the primary attempt, falling back to fields already embedded in already-collected posts/comments (`community_description`, `community_members_num`, `community_rank`) so this costs zero extra requests. See **Known gaps** below — the public endpoints are currently blocked from our environment.
- A discarded Apify proof of concept (`reddit_scrapper_apify.py`, actor `prodiger~reddit-scraper`) exists at the project root from earlier testing; it is **not used** by this pipeline (comments were disabled in that POC and it has no comments-dataset equivalent).

## Authentication

- BrightData: `Authorization: Bearer {BRIGHTDATA_API_KEY}` header, all requests. Key and dataset IDs live in a project-root `.env` (not committed): `BRIGHTDATA_API_KEY`, `BRIGHTDATA_REDDIT_POSTS_DATASET_ID`, `BRIGHTDATA_REDDIT_COMMENTS_DATASET_ID`, `BRIGHTDATA_DATASETS_BASE_URL`.
- Reddit public endpoints: no auth, only a descriptive `User-Agent` (see `config.REDDIT_PUBLIC_USER_AGENT`).

## Exact filters / query parameters

- **Posts trigger, dated attempt**: `{"url": "https://www.reddit.com/r/<sub>/", "start_date": "YYYY-MM-DD"}`, sent with `type=discover_new&discover_by=subreddit_url`. Two things were confirmed live on 2026-08-27, before the BrightData account went inactive:
  - `end_date` is **rejected outright** with HTTP 400 (`"This input should not contain a end_date field"`) — it is never sent.
  - `sort_by` is enum-validated server-side; `"new"` (lowercase) was rejected (`"This value is not allowed"`) and the correct value/casing was never confirmed before the outage, so `sort_by` is currently **not sent at all**.
  - A `start_date`-only request against r/kolkata returned HTTP 200 with **0 records** in ~13 seconds. It's unresolved whether that means "genuinely nothing found by this discovery mode" or a subtly wrong parameter — **retest this once the account reactivates** (see Known gaps).
- **Posts trigger, bare fallback**: `{"url": "https://www.reddit.com/r/<sub>/"}` with no date params at all — this is the same call the original working proof-of-concept (`brightdata_reddit_test.py`) used successfully, and it was still running (not erroring) after 12+ minutes against r/kolkata's full history when last observed. `collect_posts.collect_posts_for_range` tries the cheap dated request first; if it returns 0 records, it automatically falls back to exactly **one** bare full-history request per run, and reuses that single response to satisfy every pending month in the requested range at once (not just the first one) — so a bad dated response never turns into a bare-fallback-per-month cost blowup.
- **Comments trigger** payload: `[{"url": "<post_permalink>"}, ...]` (no `type`/`discover_by` — this dataset only supports `url_collection`), batched at `config.COMMENTS_BATCH_SIZE = 50` post URLs per request.
- **Date filtering**: regardless of what BrightData honors server-side, every downloaded post is re-checked client-side against the requested date window (`collect_posts._within_range`) before being written to disk — this is a hard backstop, not an assumption.

## Pagination procedure

BrightData's Datasets API is asynchronous, not page-based: `POST /trigger` returns a `snapshot_id`; `GET /progress/{snapshot_id}` is polled every 10s (`config.POLL_INTERVAL_S`) until `status == "ready"` (or `failed`/`error`, or a 1-hour timeout); then `GET /snapshot/{snapshot_id}?format=json` returns the full result array in one response. There is no further pagination on our side — one trigger call is one logical page. Large date ranges are instead split into calendar-month chunks (`date_utils.month_chunks`) purely to bound the blast radius of any single failed/retried request, not because the API itself paginates.

## Date filtering

Handled two ways, layered:
1. Server-side, best-effort: `start_date`/`end_date` sent in the trigger payload (see above).
2. Client-side, guaranteed: every post's `date_posted` is parsed and compared against the requested window after download; anything outside the window is dropped before being written to `raw/`.

## Comment / reply retrieval procedure

1. `collect_posts` must run first for a given month — comment collection reads the list of `(post_id, url)` pairs from that month's already-saved `raw/<slug>/<month>/posts.jsonl`.
2. Post URLs are batched (50 at a time) into `url_collection` trigger calls against the comments dataset.
3. Each call returns one record per **top-level comment**, with a nested `replies` array of **direct replies only**. See **Known gaps** — deeper reply-to-reply threads are not retrievable from this dataset.
4. `process_comments.flatten_comment_tree` turns each raw record into 1 row (the top-level comment, `depth=0`, `parent_id` = the post's id) plus one row per direct reply (`depth=1`, `parent_id` = the top-level comment's id), preserving full parent linkage.

## Rate limits encountered

No explicit rate-limit error was hit during development; BrightData's snapshot jobs for a single post's comments took roughly 5–8 minutes to become `ready` in testing. Comment batches are processed **sequentially**, not concurrently, since BrightData's concurrent-snapshot ceiling for this account has not been characterized — this is a deliberately conservative default (`config.COMMENTS_BATCH_SIZE`), not a discovered hard limit.

## Fields unavailable through the API (confirmed gaps)

- **`upvote_ratio`**: not present anywhere in BrightData's raw post schema. Always `null` in `posts.csv`.
- **True crosspost linkage**: the only candidate field, `related_posts`, reads like a "related reading" recommendation list, not genuine crosspost parentage (no `crosspost_parent_list`-equivalent field was found). `is_crosspost` / `crosspost_original_post_id` / `crosspost_original_subreddit` are always `null` pending a confirmed marker; `processed/crossposts.csv` will legitimately be empty until one is found.
- **Comment depth beyond 1**: the comments dataset returns top-level comments plus their *direct* replies only — no further nesting. `comments.csv` therefore only contains `depth` values 0 and 1, even where a real Reddit thread goes deeper. Each reply's `num_replies` field is also `null` in the raw data, so we cannot even detect how much is missing beyond depth 1.
- **`is_mod` on replies**: the raw comments dataset marks `is_moderator` on top-level comment records only; reply objects carry no such field. `is_mod` is `null` for all `depth=1` rows.
- **Subreddit rules / moderator list / creation date / flair list / pinned posts / mod announcements**: intended to come from Reddit's public `.json` endpoints. As of 2026-08-27, `www.reddit.com/r/<sub>/*.json` returns HTTP 403 from this environment regardless of `User-Agent`, and `old.reddit.com/*.json` returns a login-wall HTML page instead of JSON. The pipeline still records whatever it can passively derive from already-collected posts/comments (`community_description`, `community_members_num`, `community_rank`) at zero extra request cost, and writes `null` + an explanatory note for everything else. **To close this gap**: either (a) set up a Reddit OAuth "script" app (`reddit.com/prefs/apps`) and add `REDDIT_CLIENT_ID`/`REDDIT_CLIENT_SECRET` so `reddit_public_client.py` can be upgraded to authenticated calls, or (b) supply a BrightData "subreddit info" dataset ID if one exists on the account (checked at build time — none was found among the currently subscribed datasets).

## Known gaps / failed collection periods

Check `processed/validation_report.json` (written by `validate.py`) for the live, current list of `missing_periods` per subreddit and any `collection_failures` — this is generated from `collection_state.csv`, not hardcoded here, since it changes every run.

**Collection status as of 2026-08-27**: the BrightData account used for this project went inactive mid-development (`"Customer is not active"` on every trigger call) and is expected to reactivate next month. No real collection has run yet — `collection_state.csv`/`collection_log.csv` are empty and `raw/` is empty. Every module was validated against a synthetic offline dry-run (mirroring the exact schemas confirmed live: real posts/comments field names, the 0-record dated-request behavior, and the bare-fallback path) to confirm the state machine, dedup, CSV generation, and resumability all work correctly end to end. **Once the account reactivates**: run `python -m reddit_kolkata.collection_code.run_pipeline --subreddit kolkata --subreddit kolkatacity --start-date 2025-04-01` and additionally re-verify the two open items above (whether `start_date`-only genuinely returns 0 for r/kolkata or was a parameter issue, and the correct `sort_by` value) since confirming either could make posts collection meaningfully cheaper than the bare full-history fallback this pipeline currently relies on.

## How completed periods are tracked

`collection_state.csv`, one row per `(subreddit, data_type, month)`:

| column | meaning |
|---|---|
| `subreddit` | slug, e.g. `kolkata` |
| `data_type` | `posts`, `comments`, or `subreddit_metadata` |
| `month` | `YYYY-MM` |
| `period_start` / `period_end` | cumulative date range actually covered so far for this month |
| `status` | `pending`, `partial`, `complete`, or `failed` |
| `snapshot_id` | last BrightData snapshot id used |
| `records_collected` | records appended in the most recent successful run for this row |
| `last_attempt_at` | ISO timestamp of the last write to this row |
| `error_message` | last error, if `status == failed` |

This is keyed by month rather than the literal `(subreddit, start_date, end_date)` tuple sketched in the original task spec, deliberately: an exact-tuple key would force a full month re-fetch every time a later run's `end_date` extends past a previously-partial month (e.g. today's run covers August through the 27th; a run in October would otherwise see `(2025-08-01, 2025-08-31)` as a brand-new key and re-fetch all of August). Keying by month lets `state.pending_subrange` fetch only the uncovered delta.

Before any API call, every `collect_*` function checks this file. Comments additionally self-heal from disk content: `collect_comments.already_covered_post_ids` reads whatever is already in that month's `comments.jsonl` and only re-batches posts not yet present there, rather than trusting a batch counter — this means a crash mid-batch loses no bookkeeping.

Every actual API call (one per posts-month-chunk, one per comments-batch, one per metadata snapshot) is also logged as its own row in `collection_log.csv`.

## How to resume an interrupted collection

Just rerun the same command:

```
python -m reddit_kolkata.collection_code.run_pipeline --subreddit kolkata --subreddit kolkatacity --start-date 2025-04-01
```

Any month already `complete` in `collection_state.csv` is skipped without any network call. Any `partial` month resumes from where it left off (posts: the uncovered date delta; comments: the posts not yet covered). Any `failed` month is retried from scratch for that month only. Use `--force` to bypass all of this and re-fetch everything (useful for a full manual refresh).

## How to add another subreddit later

```
python -m reddit_kolkata.collection_code.run_pipeline --subreddit <newsubreddit> --start-date 2025-04-01
```

No code changes needed. Raw data lands in its own `raw/<newslug>/` tree, untouched existing subreddits' raw data is never re-read from the network, and the processing step (`process_posts` / `process_comments` / `build_users` / `build_monthly_activity`) always re-scans **every** subreddit folder under `raw/`, so `users.csv` gains new `posts_<newslug>`/`comments_<newslug>` columns automatically and `monthly_activity.csv`/`posts.csv`/`comments.csv` simply include the new subreddit's rows.

## File layout

```
reddit_kolkata/
├── raw/
│   ├── kolkata/2025-04/{posts.jsonl, comments.jsonl}, 2025-05/, ...
│   └── kolkatacity/2025-04/, ...
├── processed/
│   ├── posts.csv
│   ├── comments.csv
│   ├── crossposts.csv
│   ├── users.csv
│   ├── monthly_activity.csv
│   └── validation_report.json
├── subreddit_metadata/
│   ├── kolkata_<date>.json (one per collection run)
│   └── kolkata_latest.json
├── collection_code/       (all pipeline scripts, see below)
├── collection_state.csv
├── collection_log.csv
└── README.md (this file)
```

## Running it

All commands run from the project root (`4-1 SEM/Project/`), using the venv (`pandas`/`python-dateutil`/`requests`/`python-dotenv` must be installed — see root `requirements.txt`):

```bash
source venv/bin/activate

# full pipeline, both subreddits
python -m reddit_kolkata.collection_code.run_pipeline \
  --subreddit kolkata --subreddit kolkatacity --start-date 2025-04-01

# individual steps (useful for debugging / re-running just one stage)
python -m reddit_kolkata.collection_code.collect_posts --subreddit kolkata --start-date 2025-04-01
python -m reddit_kolkata.collection_code.collect_comments --subreddit kolkata --start-date 2025-04-01
python -m reddit_kolkata.collection_code.collect_subreddit_metadata --subreddit kolkata
python -m reddit_kolkata.collection_code.validate --start-date 2025-04-01
```

## Collection scripts (`collection_code/`)

| file | responsibility |
|---|---|
| `config.py` | env vars, path constants, `slugify()` |
| `date_utils.py` | calendar-month chunking |
| `id_utils.py` | derive IDs from URLs, deleted/removed text sentinels |
| `jsonl_utils.py` | JSONL read/append-with-dedup, atomic JSON writes |
| `log_utils.py` | logger + `collection_log.csv` writer |
| `state.py` | `collection_state.csv` read/upsert, resumability logic |
| `brightdata_client.py` | trigger/poll/download wrapper — only module that calls BrightData |
| `reddit_public_client.py` | best-effort Reddit `.json` client (see Known gaps) |
| `collect_subreddit_metadata.py` | subreddit metadata snapshot |
| `collect_posts.py` | month-by-month posts collection |
| `collect_comments.py` | batched comments collection per post |
| `process_posts.py` | raw → `posts.csv` + `crossposts.csv` |
| `process_comments.py` | raw → `comments.csv` |
| `build_users.py` | → `users.csv` + overlap summary |
| `build_monthly_activity.py` | → `monthly_activity.csv` |
| `validate.py` | → `validation_report.json` (spec section 8) |
| `run_pipeline.py` | orchestrates all of the above end to end |

## Links

- BrightData Datasets API (trigger/progress/snapshot): https://docs.brightdata.com/api-reference/marketplace-dataset-api/trigger-a-collection-or-discovery
- Reddit's public JSON API (subject to the access issue documented above): https://www.reddit.com/dev/api/
