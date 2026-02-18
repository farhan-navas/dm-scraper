import psycopg2
import os

# Load DB credentials from environment variables
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST
)
cur = conn.cursor()

cur.execute("SELECT post_id FROM posts")
post_format_counts = {"post-": 0, "digits": 0, "unknown": 0}

for row in cur.fetchall():
    post_id = str(row[0])
    if post_id.startswith("post-"):
        post_format_counts["post-"] += 1
    elif post_id.isdigit():
        post_format_counts["digits"] += 1
    else:
        post_format_counts["unknown"] += 1

print("Count of post-... format:", post_format_counts["post-"])
print("Count of digits only:", post_format_counts["digits"])
print("Count of unknown format:", post_format_counts["unknown"])

cur.close()
conn.close()