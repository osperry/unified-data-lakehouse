import os, json, requests, time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BRONZE = os.getenv("NYC311_BRONZE_PATH", "data/bronze/nyc311")
WATERMARK_FILE = os.path.join(BRONZE, "_watermark.json")

BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 125000
MAX_RETRIES = 5
RETRY_BACKOFF = [10, 30, 60, 90, 120]


def get_watermark():
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE) as f:
            return json.load(f)["last_created_date"]
    return "2020-07-29T19:21:00.000"


def set_watermark(dt_str):
    with open(WATERMARK_FILE, "w") as f:
        json.dump(
            {
                "last_created_date": dt_str,
                "updated_at": datetime.utcnow().isoformat(),
            },
            f,
        )


def fetch_page(since, offset):
    params = {
        "$where": f"created_date > '{since}'",
        "$order": "created_date ASC",
        "$limit": PAGE_SIZE,
        "$offset": offset,
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(BASE_URL, params=params, timeout=300)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF[attempt]
                print(f" Timeout attempt {attempt + 1}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF[attempt]
                print(f" Error attempt {attempt + 1}: {e}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def main():
    os.makedirs(BRONZE, exist_ok=True)

    since = get_watermark()
    print(f"Pulling complaints from watermark: {since}")

    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    offset = 0
    max_date = since
    total = 0

    while True:
        rows = fetch_page(since, offset)

        if not rows:
            break

        path = f"{BRONZE}/nyc311_{stamp}_p{offset}.json"

        with open(path, "w") as f:
            json.dump(rows, f)

        count = len(rows)
        total += count

        page_max = max(r.get("created_date", "") for r in rows)

        if page_max > max_date:
            max_date = page_max

        print(f" wrote {path} ({count} rows, running total: {total})")

        set_watermark(max_date)

        if count < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(3)

    if total > 0:
        print(f"Done. {total} rows. Final watermark: {max_date}")
    else:
        print("No new rows since last run.")


if __name__ == "__main__":
    main()
