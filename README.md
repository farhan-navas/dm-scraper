# DM Recommender Scraper

Lightweight Python scraper that collects thread posts and public user metadata from PersonalityCafe. Writes directly to Postgres by default, with CSV output available as a debug mode.

## DB Schema

Data is written directly into a Postgres database. Tables are created automatically on first run.

### `Thread`

| column       | type        | notes                                                               |
| ------------ | ----------- | ------------------------------------------------------------------- |
| `thread_id`  | bigint      | Numeric ID extracted from the thread URL. Primary key.              |
| `thread_url` | text        | Canonical PersonalityCafe URL.                                      |
| `forum_url`  | text        | Parent forum listing used when crawling.                            |
| `first_seen` | timestamptz | When the scraper first encountered the thread.                      |
| `last_seen`  | timestamptz | When the thread was last re-scraped.                                |
| `scraped_at` | timestamptz | Timestamp for the current extraction batch.                         |

### `Post`

| column       | type        | notes                                                                        |
| ------------ | ----------- | ---------------------------------------------------------------------------- |
| `post_id`    | text        | XenForo identifier, always `post-<digits>` format. Primary key.              |
| `thread_id`  | bigint      | FK → `thread.thread_id`.                                                     |
| `thread_url` | text        | Thread URL for convenience.                                                  |
| `page_url`   | text        | Concrete page that was scraped (thread pagination aware).                    |
| `user_id`    | bigint      | FK → `user.user_id`.                                                         |
| `username`   | text        | Username displayed on the post (overwritten by tooltip version when cached). |
| `timestamp`  | timestamptz | Raw value from `<time datetime>`; ISO 8601 string when stored in CSV.        |
| `text`       | text        | Post body flattened to newline-separated plain text.                         |
| `scraped_at` | timestamptz | When this particular post row was emitted by the scraper.                    |

### `User`

| column                | type        | notes                                                  |
| --------------------- | ----------- | ------------------------------------------------------ |
| `user_id`             | bigint      | Stable numeric identifier parsed from the member slug. |
| `username`            | text        | Canonical username from tooltip header.                |
| `profile_url`         | text        | Fully-qualified profile link without `/tooltip`.       |
| `join_date`           | timestamptz | Tooltip-provided ISO datetime.                         |
| `role`                | text        | Tooltip "user title" (e.g., Banned, Member).           |
| `gender`              | text/null   | Pulled from the `About` tab when present.              |
| `country_of_birth`    | text/null   | Pulled from the `About` tab when present.              |
| `location`            | text/null   | "From …" location from the profile header/`About` tab. |
| `mbti_type`           | text/null   | Myers-Briggs type string (`About` tab).                |
| `enneagram_type`      | text/null   | Enneagram string (`About` tab).                        |
| `socionics`           | text/null   | Socionics designation, when users fill it out.         |
| `occupation`          | text/null   | Free-form occupation field from the `About` tab.       |
| `replies`             | integer     | Tooltip "Replies" count.                               |
| `discussions_created` | integer     | Tooltip "Discussions created" count.                   |
| `reaction_score`      | integer     | Tooltip "Reaction score".                              |
| `points`              | integer     | Tooltip "Points".                                      |
| `media_count`         | integer     | Tooltip "Media" uploads.                               |
| `showcase_count`      | integer     | Tooltip "Showcase items".                              |
| `scraped_at`          | timestamptz | When this profile snapshot was saved.                  |

### `Follows`

Directed follow edges between users. Only the "following" direction is scraped per user — if user A follows user B, the row is `(follower_id=A, followed_id=B)`.

| column        | type        | notes                                            |
| ------------- | ----------- | ------------------------------------------------ |
| `follower_id` | bigint      | FK → `user.user_id`, the person who follows.     |
| `followed_id` | bigint      | FK → `user.user_id`, the person being followed.  |
| `scraped_at`  | timestamptz | When this edge was scraped.                      |

Composite primary key: `(follower_id, followed_id)`.

### `Interaction` (derived)

To capture edges between users, we derive an interaction row any time a post is a direct reply/quote/mention of another post. The first pass can infer interactions by:

1. Matching quoted blocks (they include the original username + post id in markup).
2. Falling back to explicit `@username` mentions when a quote is absent.

Schema:

| column             | type        | notes                                                                                        |
| ------------------ | ----------- | -------------------------------------------------------------------------------------------- |
| `interaction_id`   | uuid        | Synthetic PK                                                                                 |
| `replying_post_id` | text        | FK → `post.post_id` (the reply), source post id                                              |
| `target_post_id`   | text        | FK → `post.post_id` (the quoted/mentioned post). Nullable when we only know the target user. |
| `source_user_id`   | bigint      | FK → `user.user_id`, the person replying                                                     |
| `target_user_id`   | bigint      | FK → `user.user_id`, the person that is being replied to                                     |
| `thread_id`        | bigint      | Convenience FK for filtering.                                                                |
| `interaction_type` | text        | Enum (`quote`, `mention`, `reply`).                                                          |
| `scraped_at`       | timestamptz | Timestamp applied when emitting the derived edge.                                            |

## Running scraper

Orchestrator lives in `run_forum_scrape.py`. Default mode writes directly to Postgres (requires `DATABASE_URL` in `.env`).

```bash
uv run run_forum_scrape.py --forum-index <N>              # scrape forum N to Postgres, skip already-scraped threads
uv run run_forum_scrape.py --forum-index <N> --no-skip    # re-scrape all threads in forum N
uv run run_forum_scrape.py --forum-index <N> --csv        # write to CSV files instead (debug mode)
```

Forum indices correspond to rows in `forums.csv` (0-indexed).

Rate limit: 1 request per 2 seconds. Already-scraped threads and users are skipped automatically via DB lookups.

### Authentication

Some forums and user profiles require authentication. The scraper reads auth cookies from `.env`:

```
XF_USER="<your xf_user cookie>"     # persistent login cookie, lasts 30 days
CDNCSRF="<your cdncsrf cookie>"     # CDN-level CSRF token
```

To get these: log into PersonalityCafe in your browser (check "Stay logged in"), then copy the `xf_user` and `cdncsrf` cookie values from DevTools → Application → Cookies.

### Utility scripts

```bash
uv run scrape_user_follows.py                    # scrape /following for all users in DB
uv run scrape_user_follows.py --max-users 10     # limit for testing
uv run db/check_counts.py                        # show row counts per table + DB size
uv run db/retry_failed_interactions.py            # retry FK-failed interactions from db_logs/
uv run db/normalize_post_ids.py --apply          # normalize bare-digit post IDs in CSVs
uv run db/load_csv_to_postgres.py                # bulk load CSVs into Postgres
```

More testing scripts in `test/` directory.

## Run Configurations (up till now)

New Update -> forum_scraper now takes in a int arg for the forum index that it will scrape. This way we can scrape multiple forums on the same server.

COM1 (LTP):

COM2 (TM):

- 10 -> What's my personality type?
- 13 -> Cognitive Functions
- 14 -> Socionomics Forum, Subs: 15
- 16 -> Enneagram Personality Theory Forum, Subs: 17 to 29
- 30 -> The Generations, Subs: 31 to 34
- 75 -> General Psychology, Subs: 76, 77
- 38 -> Member Polls
- 81 -> The Art Museum
- 82 -> Book, Music & Movie Reviews, Subs: 83
- 89 -> History Buffs
- 84 -> Education & Career Talk
- 85 -> Science and Technology, Subs: 86
- 87 -> Health and Fitness
- 98 -> Advice Center

- TOTAL ROWS BEF: 8392223
- TOTAL ROWS AFT: 12221480

- Also training conv-transformer

COM3 (SMP):

- 0 -> Announcements, Subforums: 1, 2, 3, 4, 5, 6
- 7 -> Intro, Subs: 8
- 11 -> Guess the type
- 12 -> Myers Briggs Forum
- 35 -> Other Personality Theories, Subs: 36, 37
- 39 -> SJ's Temparement Forums, Subs: 40 to 47
- 48 -> SP's Temparement Forums, Subs: 49 to 56
- 57 -> NT's Temparement Forums, Subs: 58 to 65
- 66 -> NF's Temparement Forum, Subs: 67 to 74
- 110 -> Blog

- TOTAL ROWS BEF: 7057039
- TOTAL ROWS AFT: 10712504

0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40
41,42,43,44,45,46,47,48,49,50,51,52,53,54,55
