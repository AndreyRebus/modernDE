# bot/prefetch.py
import logging
from dotenv import load_dotenv
from .data_cache import fetch_and_cache

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

if __name__ == "__main__":
    load_dotenv()
    df = fetch_and_cache()
    print(f"Prefetched {len(df)} rows")
