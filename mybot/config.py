import os
import logging
from datetime import timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
SPLASH_DIR = Path(os.getenv("SPLASH_DIR", "data/splashes"))
TRINO_TABLE = os.getenv("RECORDS_TABLE", "iceberg.dbt_model.concat_record")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_FILE = DATA_DIR / "concat_record.parquet"
STALE_AFTER = timedelta(hours=int(os.getenv("STALE_HOURS", "6")))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

NICKNAMES = [
    "Monty Gard#RU1",
    "Breaksthesilence#RU1",
    "2pilka#RU1",
    "Gruntq#RU1",
    "Шaзам#RU1",
    "Prooaknor#RU1",
]