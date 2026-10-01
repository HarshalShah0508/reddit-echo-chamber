# Reddit Data Collection Pipeline — r/kolkata & r/KolkataCity

Resumable, parameterized collection pipeline for Reddit posts, comments, and subreddit
metadata. Built for r/kolkata and r/KolkataCity, 2025-04-01 to present, but the collection
code never hardcodes those subreddit names — adding a new subreddit is a rerun with a
different `--subreddit` flag, not a code change.

## API / tool / data source used

Two sources exist in this codebase; **Arctic Shift is the one actually used for all current
data** (`--source arcticshift`, the default in practice). BrightData was the original plan
but became unusable partway through the project (see "Known gaps" below) and is kept only as
dead-but-working code (`collect_posts.py`, `collect_comments.py`, `brightdata_client.py`).

- **Arctic Shift** (`collection_code/arcticshift_client.py`): a third-party, continuously-updated
  Reddit archive (successor to Pushshift), `https://arctic-shift.photon-reddit.com/api`. No API
  key. Endpoints used:
  - `/posts/search?subreddit=&after=&before=&limit=&sort=` — paginated, cursor by `created_utc`.
  - `/comments/search?link_id=t3_<id>&limit=&sort=` — returns a **flat** list of every comment
    under one post (not nested); we reconstruct a 2-level tree client-side (see below).
  - `/subreddits/search?subreddit=&limit=` — the subreddit's own "about" object.
  - Repo: https://github.com/ArthurHeitmann/arctic_shift
- **BrightData Datasets API v3** (`collection_code/brightdata_client.py`, `collect_posts.py`,
  `collect_comments.py`): dataset `gd_lvz8ah06191smkebj4` for posts, a separate dataset ID (env
  `BRIGHTDATA_REDDIT_COMMENTS_DATASET_ID`) for comments. Still wired into `run_pipeline.py` via
  `--source brightdata` (the default) but unused in practice since the account went inactive and
  this network also intermittently blocks `api.brightdata.com` with TLS interception errors.
  Docs: https://docs.brightdata.com/api-reference/marketplace-dataset-api/trigger-a-collection-or-discovery
- **Subreddit metadata**: tried in order — Reddit's own public `.json` endpoints first
  (`reddit_public_client.py`), then Arctic Shift's `/subreddits/search` (closes most of the gap
  the public endpoint leaves), then fields already embedded in collected posts/comments (zero
  extra cost). See **Known gaps** — rules, moderator list, pinned posts, and mod announcements
  remain unavailable from *any* of these sources (Reddit doesn't expose them on the subreddit
  object itself, and Arctic Shift doesn't mirror the separate `about/rules.json` /
  `about/moderators.json` endpoints).
- A discarded Apify proof of concept (`reddit_scrapper_apify.py`) exists at the project root
  from earlier testing; not used by this pipeline.

## Authentication

- **Arctic Shift**: none. Only a sensible default `User-Agent`/no special headers needed.
- **BrightData**: `Authorization: Bearer {BRIGHTDATA_API_KEY}` header, all requests. Key and
  dataset IDs live in a project-root `.env` (not committed).
- **Reddit public endpoints**: no auth, only a descriptive `User-Agent` (see
  `config.REDDIT_PUBLIC_USER_AGENT`).

## Exact filters / query parameters

**Arctic Shift posts** (`collect_posts_arcticshift.py` → `arcticshift_client.fetch_posts`):
one call per pending date range, `after=<ISO date>`, `before=<epoch of end-date + 1 day>`,
`limit=100`, `sort=asc`; paginated by re-issuing with `after=<last page's created_utc + 1>`
until a short page is returned. Every downloaded post is still re-checked client-side against
the requested date window before being written to disk (`_within_range`), as a hard backstop.

**Arctic Shift comments** (`collect_comments_arcticshift.py` →
`arcticshift_client.fetch_comments_for_post`): one call per post, `link_id=t3_<post_id>`,
`limit=100`, `sort=asc`, paginated the same way. Fetches run **concurrently**
(`ThreadPoolExecutor`, `--workers`, default 5) — file writes stay single-threaded in the main
thread so dedup/state bookkeeping has no race condition; only the network fetch overlaps.

**Arctic Shift subreddit metadata**: `subreddit=<slug>&limit=1`, single call, no pagination.

**BrightData** (legacy path, documented for completeness): posts trigger tries
`{"url": "...", "start_date": "YYYY-MM-DD"}` first (cheap), falls back to one bare
full-history request per run if that returns 0 records; comments trigger is
`[{"url": "<post_permalink>"}, ...]`, batched 50 at a time. See git history / inline
docstrings in `collect_posts.py` / `collect_comments.py` for the full detail — this path is
not currently exercised.

## Pagination procedure

Arctic Shift is a plain synchronous paginated GET (no async job): one page is up to 100
records; the next page's cursor is the last record's `created_utc + 1`. Loop until a page
comes back shorter than the limit (or empty). BrightData, by contrast, is asynchronous:
`POST /trigger` → poll `GET /progress/{snapshot_id}` every 10s → `GET /snapshot/{snapshot_id}`.

## Date filtering

Two layers: server-side `after`/`before` params on the Arctic Shift request, and a client-side
re-check of every downloaded post's `date_posted` against the requested window before writing
to `raw/` (`collect_posts_arcticshift._within_range`) — the server-side filter is trusted but
never solely relied on.

## Comment / reply retrieval procedure

1. `collect_posts_arcticshift` must run first for a given month — comment collection reads the
   list of `(post_id, url)` pairs from that month's already-saved
   `raw/<slug>/<month>/posts.jsonl`.
2. For each post, `arcticshift_client.fetch_comments_for_post` returns a **flat** list of every
   comment under the post (no native tree structure from this endpoint).
3. `collect_comments_arcticshift._build_comment_tree` reconstructs a 2-level tree: top-level
   comments are those whose `parent_id` equals the post's own `t3_` fullname; direct replies are
   comments whose `parent_id` equals a top-level comment's `t1_` fullname. **Deeper
   reply-to-reply threads are collapsed/dropped** — this matches the same depth-1 cap the
   original BrightData comments dataset had, kept deliberately so both subreddits share one
   schema rather than introducing a difference between sources.
4. `process_comments.flatten_comment_tree` turns each reconstructed record into 1 row per
   top-level comment (`depth=0`) plus 1 row per direct reply (`depth=1`), preserving
   `parent_id` linkage so comment trees (to depth 1) can be reconstructed from `comments.csv`.

## Rate limits encountered

- **Arctic Shift**: no official hard limit; their own guidance is "a couple requests per
  second" is safe. In practice, their backend has shown two failure modes under load, both
  handled by `arcticshift_client._get`'s retry loop (6 attempts, backoff `1, 2, 5, 15, 30, 60`
  seconds): (a) genuine connection-level failures (SSL cert-chain errors, connection resets,
  read timeouts — these also hit this network independent of Arctic Shift, see below), and
  (b) quick `422`/`503` responses that are **transient, not semantic** — confirmed empirically
  by retrying an identical failed request seconds later and getting a normal `200`. Both are
  retried identically; only genuine non-retryable 4xx (malformed request) propagate immediately.
- **This network's own reliability** is the dominant real-world constraint, independent of
  either API: outbound HTTPS has repeatedly failed for tens of seconds to minutes at a time
  with `SSLCertVerificationError: self signed certificate in certificate chain` (looks like
  local TLS interception — VPN/proxy/security software, not a code bug) — the same signature
  that made BrightData unusable in the first place. Two further operational issues were found
  and fixed during multi-hour unattended runs: (1) **the machine going to sleep** silently
  stalls a background collection process for however long it's asleep — fixed by wrapping
  long runs in `caffeinate -i -s`; (2) the per-post sequential loop made throughput collapse
  during backend flakiness (each retry-laden post costing minutes) — fixed by (a) a much
  shorter initial backoff (quick transient errors recover in ~1s, not 5s) and (b) running
  several posts' comment-fetches concurrently instead of one at a time.
- **BrightData**: single-post comment snapshot jobs took roughly 5–8 minutes to become `ready`
  in earlier testing; not currently exercised.

## Fields unavailable through the API (confirmed gaps)

- **Comment depth beyond 1**: both sources return/are reconstructed into top-level comments
  plus their *direct* replies only — `comments.csv` has `depth` values 0 and 1 only.
- **Subreddit rules / moderator list / pinned posts / mod announcements**: unavailable from
  every source tried — Reddit's public `.json` endpoints are blocked (HTTP 403) from this
  environment; Arctic Shift's `/subreddits/search` returns the subreddit's "about" object
  (which *does* now supply `created_utc`, `description`/`public_description`, `subscribers`,
  `title`) but not rules/moderators/pinned-posts/announcements, because Reddit itself doesn't
  put those on the subreddit object — they're separate endpoints Arctic Shift doesn't mirror.
  `subreddit_metadata/*.json` records `null` + an explanatory note for these fields, not a
  guess. To close this gap: a Reddit OAuth "script" app (`reddit.com/prefs/apps`) with
  `REDDIT_CLIENT_ID`/`REDDIT_CLIENT_SECRET` would let `reddit_public_client.py` use
  authenticated calls instead of the blocked public ones.
- **`is_stickied`**: populated from Arctic Shift's own per-post `stickied` field for any post
  fetched *after* this field was wired in; posts collected earlier in the project (most of the
  current dataset) fall back to the subreddit-metadata `pinned_posts` list, which is itself
  empty (see above) — so `is_stickied` is mostly `null` for already-collected data. Re-fetching
  posts (`--force` on the posts step) would backfill it; not done by default to avoid an
  otherwise-unnecessary full re-download.
- **`upvote_ratio` / true crosspost linkage**: present for any post sourced via Arctic Shift
  (`upvote_ratio`, `crosspost_parent_list` with real original author/URL/date — see
  `processed/crossposts.csv`); still `null`/absent for the handful of posts originally sourced
  via BrightData, whose raw schema never carried these fields.

## Known gaps / failed collection periods

Check `processed/validation_report.json` (written by `validate.py`) for the live, current list
of `missing_periods` per subreddit and any `collection_failures` — generated from
`collection_state.csv`, not hardcoded here, since it changes every run. As of this writing:

- **Both subreddits**: full real post history collected, 2025-04-01 through present.
- **r/KolkataCity**: comments collected (≥1 pass) for essentially the whole date range.
- **r/kolkata**: comments collected for 2025-04, 2025-05 (mostly), 2026-01 through 2026-09
  (the active-collection range), and 2026-08/2026-09 (most recent months). **2025-06 through
  2025-12 comments were deliberately never attempted** — a scope decision, not a failure: this
  subreddit turned out to have ~90,000 posts total (vs. ~13,500 for KolkataCity), several times
  larger than originally scoped, and comment-by-comment collection at that volume is a
  multi-day job; Jan–Sep 2026 was prioritized as the actually-needed range. Posts for those
  months *are* fully collected; only comments are missing. Rerunning
  `collect_comments_arcticshift.py --subreddit kolkata --start-date 2025-06-01 --end-date 2025-12-31`
  picks this up with no re-work of anything already done.
- A handful of individual posts per month (visible as `failed_posts` in run output / the
  `failed` status rows in `collection_log.csv`) never got comments after exhausting retries —
  these self-heal on any future rerun since a never-written post is never marked "covered."

## How completed periods are tracked

`collection_state.csv`, one row per `(subreddit, data_type, month)`:

| column | meaning |
|---|---|
| `subreddit` | slug, e.g. `kolkata` |
| `data_type` | `posts`, `comments`, or `subreddit_metadata` |
| `month` | `YYYY-MM` |
| `period_start` / `period_end` | cumulative date range actually covered so far for this month |
| `status` | `pending`, `partial`, `complete`, or `failed` |
| `snapshot_id` | last BrightData snapshot id, or `arcticshift` for that source |
| `records_collected` | records appended in the most recent successful run for this row |
| `last_attempt_at` | ISO timestamp of the last write to this row |
| `error_message` | last error, if `status == failed` |

This is keyed by month rather than the literal `(subreddit, start_date, end_date)` tuple
sketched in the original task spec, deliberately: an exact-tuple key would force a full month
re-fetch every time a later run's `end_date` extends past a previously-partial month. Keying by
month lets `state.pending_subrange` fetch only the uncovered delta.

Before any API call, every `collect_*` function checks this file. Comments additionally
self-heal from disk content: `already_covered_post_ids_bare` reads whatever is already in that
month's `comments.jsonl` and only re-batches posts not yet present there, rather than trusting a
counter — a crash mid-run loses no bookkeeping, it just resumes.

Every actual API call is also logged as its own row in `collection_log.csv`.

## How to resume an interrupted collection

Rerun the same command — any `complete` month/data-type is skipped without a network call, any
`partial` resumes from the uncovered delta, any `failed` retries from scratch for that
month/post only:

```bash
# full pipeline (posts + metadata + comments), Arctic Shift source
python -m reddit_kolkata.collection_code.run_pipeline \
  --subreddit kolkata --subreddit kolkatacity --start-date 2025-04-01 --source arcticshift

# comments only, scoped to a date range, with concurrency
python -m reddit_kolkata.collection_code.collect_comments_arcticshift \
  --subreddit kolkata --start-date 2026-01-01 --end-date 2026-09-30 --workers 6

# posts only
python -m reddit_kolkata.collection_code.collect_posts_arcticshift \
  --subreddit kolkata --start-date 2025-04-01
```

Long unattended runs on a laptop should be wrapped in `caffeinate -i -s <command>` so the
process doesn't silently stall if the machine sleeps (see "Rate limits encountered" above).
Use `--force` to bypass all state-based skipping (full manual refresh).

## How to add another subreddit later

```bash
python -m reddit_kolkata.collection_code.run_pipeline \
  --subreddit <newsubreddit> --start-date 2025-04-01 --source arcticshift
```

No code changes needed. Raw data lands in its own `raw/<newslug>/` tree, untouched existing
subreddits' raw data is never re-read from the network, and the processing step always
re-scans **every** subreddit folder under `raw/`, so `users.csv` gains new
`posts_<newslug>`/`comments_<newslug>` columns automatically and `monthly_activity.csv` /
`posts.csv` / `comments.csv` simply include the new subreddit's rows.

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

All commands run from the project root (`4-1 SEM/Project/`), using the venv:

```bash
source venv/bin/activate

python -m reddit_kolkata.collection_code.run_pipeline \
  --subreddit kolkata --subreddit kolkatacity --start-date 2025-04-01 --source arcticshift

# individual steps
python -m reddit_kolkata.collection_code.collect_posts_arcticshift --subreddit kolkata --start-date 2025-04-01
python -m reddit_kolkata.collection_code.collect_comments_arcticshift --subreddit kolkata --start-date 2025-04-01 --workers 6
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
| `arcticshift_client.py` | Arctic Shift HTTP client — retry/backoff, pagination; only module that calls arctic-shift.photon-reddit.com |
| `collect_posts_arcticshift.py` | month-by-month posts collection via Arctic Shift |
| `collect_comments_arcticshift.py` | concurrent per-post comments collection via Arctic Shift |
| `brightdata_client.py` | BrightData trigger/poll/download wrapper (legacy, unused in practice) |
| `collect_posts.py` / `collect_comments.py` | BrightData-sourced collectors (legacy, unused in practice) |
| `reddit_public_client.py` | best-effort Reddit `.json` client (see Known gaps) |
| `collect_subreddit_metadata.py` | subreddit metadata snapshot — Reddit public → Arctic Shift → derived-from-posts, in that order |
| `process_posts.py` | raw → `posts.csv` + `crossposts.csv` |
| `process_comments.py` | raw → `comments.csv` |
| `build_users.py` | → `users.csv` + overlap summary |
| `build_monthly_activity.py` | → `monthly_activity.csv` |
| `validate.py` | → `validation_report.json` (spec section 8) |
| `run_pipeline.py` | orchestrates all of the above end to end, `--source {brightdata,arcticshift}` |

## Links

- Arctic Shift (API used for all current data): https://github.com/ArthurHeitmann/arctic_shift
- BrightData Datasets API (legacy, unused in practice): https://docs.brightdata.com/api-reference/marketplace-dataset-api/trigger-a-collection-or-discovery
- Reddit's public JSON API (subject to the 403 block documented above): https://www.reddit.com/dev/api/
