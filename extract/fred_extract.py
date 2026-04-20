import os, json, requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("FRED_API_KEY")
SERIES = ["UNRATE", "CPIAUCSL", "FEDFUNDS", "GDP"]
BRONZE = os.getenv("FRED_BRONZE_PATH", "data/bronze/fred")

def fetch(series_id):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {"series_id": series_id, "api_key": API_KEY, "file_type": "json"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    os.makedirs(BRONZE, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    for sid in SERIES:
        data = fetch(sid)
        path = f"{BRONZE}/{sid}_{stamp}.json"
        with open(path, "w") as f:
            json.dump(data, f)
        print(f"wrote {path}")

if __name__ == "__main__":
    main()
