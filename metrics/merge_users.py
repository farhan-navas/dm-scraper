import json, glob
from pathlib import Path

INPUT_FILES = Path("metrics").glob("userids*")
OUT_FILE = "metrics/unique_userids.json"

def main():
    ids = set()
    for f in INPUT_FILES:
        with open(f, "r", encoding="utf-8") as fp:
            ids.update(json.load(fp)["user_ids"])

        
    with open(OUT_FILE, "w", encoding="utf-8") as fp:
        json.dump({"user_ids": sorted(ids)}, fp, ensure_ascii=False)

if __name__ == "__main__":
    main()
    