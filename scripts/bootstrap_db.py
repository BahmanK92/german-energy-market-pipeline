import logging

from src.load.postgres import execute_sql, get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def bootstrap_db() -> None:
    """
    Create the database schemas needed for the pipeline.
    """
    engine = get_engine()

    sql = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS core;
    CREATE SCHEMA IF NOT EXISTS mart;
    """

    execute_sql(engine, sql)
    logger.info("Database bootstrap completed successfully")


if __name__ == "__main__":
    bootstrap_db()
    print("Schemas created or already existed: raw, core, mart")
    