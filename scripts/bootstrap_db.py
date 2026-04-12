import logging
from pathlib import Path

from config.settings import SQL_DIR
from src.load.postgres import execute_sql, get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


SQL_FILES_IN_ORDER = [
    "001_create_schemas.sql",
    "002_create_raw_tables.sql",
    "003_create_core_tables.sql",
    "004_create_mart_tables.sql",
    "005_indexes_constraints.sql",
]


def bootstrap_db() -> None:
    """
    Create schemas, tables, and indexes by executing SQL files in order.
    """
    engine = get_engine()

    for filename in SQL_FILES_IN_ORDER:
        sql_path = SQL_DIR / filename

        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")

        logger.info("Executing SQL file: %s", filename)

        sql = sql_path.read_text(encoding="utf-8")
        execute_sql(engine, sql)

    logger.info("Database bootstrap completed successfully.")


if __name__ == "__main__":
    bootstrap_db()
    