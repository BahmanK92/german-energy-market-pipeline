import logging
from functools import reduce
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


def build_core_hourly_table(
    normalized_batches: List[pd.DataFrame],
    how: str = "outer",
) -> pd.DataFrame:
    """
    Merge multiple normalized SMARD DataFrames into one hourly wide table.

    Best practice:
    - use datetime_utc as the canonical merge key
    - derive/local time columns only once after merge
    """
    if not normalized_batches:
        raise ValueError("normalized_batches cannot be empty")

    prepared_dfs = []

    for df in normalized_batches:
        if df.empty:
            logger.warning("Skipping empty normalized DataFrame")
            continue

        required_columns = {
            "datetime_utc",
            "datetime_berlin",
            "date_berlin",
            "hour_of_day",
            "value_name",
            "value",
        }
        missing = required_columns - set(df.columns)
        if missing:
            raise ValueError(f"Normalized DataFrame missing required columns: {missing}")

        value_name = df["value_name"].iloc[0]

        slim_df = df[
            [
                "datetime_utc",
                "datetime_berlin",
                "date_berlin",
                "hour_of_day",
                "value",
            ]
        ].copy()

        slim_df = slim_df.rename(columns={"value": value_name})
        slim_df = slim_df.drop_duplicates(subset=["datetime_utc"])
        slim_df = slim_df.sort_values("datetime_utc").reset_index(drop=True)

        prepared_dfs.append(slim_df)

    if not prepared_dfs:
        raise ValueError("No non-empty DataFrames available to merge")

    # Keep the full time columns only from the first dataframe
    base_df = prepared_dfs[0].copy()

    # For all others, keep only datetime_utc + metric column
    metric_dfs = [base_df]
    for df in prepared_dfs[1:]:
        metric_cols = [col for col in df.columns if col not in ["datetime_berlin", "date_berlin", "hour_of_day"]]
        metric_dfs.append(df[metric_cols].copy())

    core_df = reduce(
        lambda left, right: pd.merge(left, right, on="datetime_utc", how=how),
        metric_dfs,
    )

    # Rebuild helper time columns from datetime_utc to keep them consistent
    core_df["datetime_berlin"] = core_df["datetime_utc"].dt.tz_convert("Europe/Berlin")
    core_df["date_berlin"] = core_df["datetime_berlin"].dt.date
    core_df["hour_of_day"] = core_df["datetime_berlin"].dt.hour

    # Put time columns first
    metric_columns = [
        col for col in core_df.columns
        if col not in ["datetime_utc", "datetime_berlin", "date_berlin", "hour_of_day"]
    ]

    core_df = core_df[
        ["datetime_utc", "datetime_berlin", "date_berlin", "hour_of_day"] + metric_columns
    ]

    core_df = core_df.sort_values("datetime_utc").reset_index(drop=True)

    logger.info(
        "Built core hourly table with %s rows and %s columns",
        len(core_df),
        len(core_df.columns),
    )

    return core_df
    