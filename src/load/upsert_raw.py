import logging
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def upsert_raw_dataframe(
    df: pd.DataFrame,
    engine: Engine,
    table_name: str = "smard_timeseries_long",
    schema: str = "raw",
) -> None:
    """
    Upsert normalized SMARD data into raw.smard_timeseries_long.

    Uses (series_key, datetime_utc) as the conflict key.
    """

    if df.empty:
        logger.warning("Received empty DataFrame for raw upsert — skipping")
        return

    required_columns = {
        "series_key",
        "value_name",
        "region",
        "resolution",
        "datetime_utc",
        "datetime_berlin",
        "value",
        "source_batch_timestamp",
    }

    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for raw upsert: {missing}")

    # Keep only needed columns
    insert_df = df[
        [
            "series_key",
            "value_name",
            "region",
            "resolution",
            "datetime_utc",
            "datetime_berlin",
            "value",
            "source_batch_timestamp",
        ]
    ].copy()

    # Convert to list of dicts for execution
    records = insert_df.to_dict(orient="records")

    if not records:
        logger.warning("No records to insert after processing — skipping")
        return

    upsert_sql = f"""
    INSERT INTO {schema}.{table_name} (
        series_key,
        value_name,
        region,
        resolution,
        datetime_utc,
        datetime_berlin,
        value,
        source_batch_timestamp
    )
    VALUES (
        :series_key,
        :value_name,
        :region,
        :resolution,
        :datetime_utc,
        :datetime_berlin,
        :value,
        :source_batch_timestamp
    )
    ON CONFLICT (series_key, datetime_utc)
    DO UPDATE SET
        value = EXCLUDED.value,
        source_batch_timestamp = EXCLUDED.source_batch_timestamp,
        datetime_berlin = EXCLUDED.datetime_berlin;
    """

    with engine.begin() as conn:
        conn.execute(text(upsert_sql), records)

    logger.info(
        "Upserted %s rows into %s.%s",
        len(records),
        schema,
        table_name,
    )