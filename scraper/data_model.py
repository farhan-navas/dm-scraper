"""Shared schema metadata for DB and CSV outputs."""

POSTS_FIELDNAMES: list[str] = [
    "thread_id",
    "thread_url",
    "page_url",
    "post_id",
    "user_id",
    "username",
    "timestamp",
    "text",
    "scraped_at",
]

USERS_FIELDNAMES: list[str] = [
    "user_id",
    "username",
    "profile_url",
    "join_date",
    "role",
    "gender",
    "country_of_birth",
    "location",
    "mbti_type",
    "enneagram_type",
    "socionics",
    "occupation",
    "replies",
    "discussions_created",
    "reaction_score",
    "points",
    "media_count",
    "showcase_count",
    "scraped_at",
]

INTERACTIONS_FIELDNAMES: list[str] = [
    "interaction_id",
    "replying_post_id",
    "target_post_id",
    "source_user_id",
    "target_user_id",
    "thread_id",
    "interaction_type",
    "scraped_at",
]

THREADS_FIELDNAMES: list[str] = [
    "thread_id",
    "thread_url",
    "thread_title",
    "forum_url",
    "first_seen",
    "last_seen",
    "scraped_at",
]
