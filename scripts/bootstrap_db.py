import logging

from config.settings import SQL_DIR
from src.load.postgres import execute_sql, get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def bootstrap_db() -> None:
    """
    Create schemas and database objects needed for the pipeline.
    """
    engine = get_engine()

    schema_sql = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS core;
    CREATE SCHEMA IF NOT EXISTS mart;
    """
    execute_sql(engine, schema_sql)
    logger.info("Schemas ensured successfully")

    raw_table_sql_path = SQL_DIR / "002_create_raw_tables.sql"
    raw_table_sql = raw_table_sql_path.read_text(encoding="utf-8")
    execute_sql(engine, raw_table_sql)
    logger.info("Raw table ensured successfully")


if __name__ == "__main__":
    bootstrap_db()
    print("Schemas and raw table created or already existed")
     