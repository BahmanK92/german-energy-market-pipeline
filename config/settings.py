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