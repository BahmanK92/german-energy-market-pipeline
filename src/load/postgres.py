import logging

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import SQLALCHEMY_DATABASE_URL

logger = logging.getLogger(__name__)


def get_engine() -> Engine:
    """
    Create and return a SQLAlchemy engine for PostgreSQL.
    """
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    return engine


def test_connection(engine: Engine) -> None:
    """
    Test that the PostgreSQL connection works.
    """
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 AS ok"))
        row = result.fetchone()

    logger.info("Postgres connection test successful: %s", row[0])


def execute_sql(engine: Engine, sql: str) -> None:
    """
    Execute a raw SQL statement.
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

    logger.info("Executed SQL successfully")


def write_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    schema: str | None = None,
    if_exists: str = "append",
    index: bool = False,
) -> None:
    """
    Write a pandas DataFrame into PostgreSQL using to_sql.
    """
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=index,
        method="multi",
    )

    logger.info(
        "Wrote %s rows to %s%s",
        len(df),
        f"{schema}." if schema else "",
        table_name,
    )
    