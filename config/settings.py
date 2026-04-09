from pathlib import Path

# Project root:
# config/settings.py -> parent.parent = project root
BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG_DIR = BASE_DIR / "config"
SRC_DIR = BASE_DIR / "src"
SCRIPTS_DIR = BASE_DIR / "scripts"
SQL_DIR = BASE_DIR / "sql"
TESTS_DIR = BASE_DIR / "tests"

SMARD_FILTERS_FILE = CONFIG_DIR / "smard_filters.yml"

SMARD_BASE_URL = "https://www.smard.de/app/chart_data"
DEFAULT_TIMEOUT_SECONDS = 30

# Later you can move these to .env if needed
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")

SQLALCHEMY_DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
