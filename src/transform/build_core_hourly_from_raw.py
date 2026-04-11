import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def build_core_hourly_from_raw(
    engine: Engine,
    series_keys: list[str] | None = None,
) -> pd.DataFrame:
    """
    Read raw.smard_timeseries_long from Postgres and pivot it into
    the wide hourly core table shape.
    """
    if series_keys is None:
        series_keys = [
            "price",
            "load",
            "residual_load_smard",
            "solar",
            "wind_onshore",
            "wind_offshore",
            "biomass",
            "hydro",
            "lignite",
            "hard_coal",
            "gas",
            "other_conventional",]

    query = text(
        """
        SELECT
            series_key,
            value_name,
            datetime_utc,
            datetime_berlin,
            value
        FROM raw.smard_timeseries_long
        WHERE series_key = ANY(:series_keys)
        ORDER BY datetime_utc, series_key
        """
    )

    with engine.connect() as conn:
        raw_df = pd.read_sql(query, conn, params={"series_keys": series_keys})

    if raw_df.empty:
        logger.warning("No raw data found for requested series")
        return raw_df

    core_df = (
        raw_df.pivot_table(
            index="datetime_utc",
            columns="value_name",
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )

    core_df.columns.name = None

    core_df["datetime_utc"] = pd.to_datetime(core_df["datetime_utc"], utc=True)
    core_df["datetime_berlin"] = (
        core_df["datetime_utc"]
        .dt.tz_convert("Europe/Berlin")
        .dt.tz_localize(None)
    )
    core_df["date_berlin"] = core_df["datetime_berlin"].dt.date
    core_df["hour_of_day"] = core_df["datetime_berlin"].dt.hour

    ordered_cols = [
        "datetime_utc",
        "datetime_berlin",
        "date_berlin",
        "hour_of_day",
    ]

    metric_cols = [c for c in core_df.columns if c not in ordered_cols]
    core_df = core_df[ordered_cols + metric_cols]
    core_df = core_df.sort_values("datetime_utc").reset_index(drop=True)

    logger.info(
        "Built DB-backed core hourly DataFrame with %s rows and %s columns",
        len(core_df),
        len(core_df.columns),
    )

    return core_df