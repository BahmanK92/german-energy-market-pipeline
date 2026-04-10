import logging

import pandas as pd

logger = logging.getLogger(__name__)


def build_features(core_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build Phase 1 feature columns from the core hourly table.

    Expected core columns may include:
    - datetime_utc
    - datetime_berlin
    - date_berlin
    - hour_of_day
    - price_eur_mwh
    - load_mw
    - solar_mw
    - wind_onshore_mw
    - wind_offshore_mw
    - biomass_mw
    - hydro_mw
    - lignite_mw
    - hard_coal_mw
    - gas_mw
    - other_conventional_mw
    - residual_load_smard_mw
    """
    if core_df.empty:
        logger.warning("Received empty core_df in build_features")
        return core_df.copy()

    required_time_columns = {
        "datetime_utc",
        "datetime_berlin",
        "date_berlin",
        "hour_of_day",
    }
    missing_time = required_time_columns - set(core_df.columns)
    if missing_time:
        raise ValueError(f"core_df missing required time columns: {missing_time}")

    df = core_df.copy()

    def series_or_zero(column_name: str) -> pd.Series:
        if column_name in df.columns:
            return pd.to_numeric(df[column_name], errors="coerce").fillna(0)
        return pd.Series(0.0, index=df.index)

    def series_or_na(column_name: str) -> pd.Series:
        if column_name in df.columns:
            return pd.to_numeric(df[column_name], errors="coerce")
        return pd.Series(pd.NA, index=df.index, dtype="float64")

    solar = series_or_zero("solar_mw")
    wind_onshore = series_or_zero("wind_onshore_mw")
    wind_offshore = series_or_zero("wind_offshore_mw")
    biomass = series_or_zero("biomass_mw")
    hydro = series_or_zero("hydro_mw")

    lignite = series_or_zero("lignite_mw")
    hard_coal = series_or_zero("hard_coal_mw")
    gas = series_or_zero("gas_mw")
    other_conventional = series_or_zero("other_conventional_mw")

    load = series_or_na("load_mw")

    df["coal_mw"] = lignite + hard_coal

    df["renewable_generation_mw"] = (
        solar + wind_onshore + wind_offshore + biomass + hydro
    )

    df["fossil_generation_mw"] = (
        df["coal_mw"] + gas + other_conventional
    )

    df["total_generation_selected_mw"] = (
        df["renewable_generation_mw"] + df["fossil_generation_mw"]
    )

    denominator = df["total_generation_selected_mw"].replace(0, pd.NA)

    df["renewable_share"] = df["renewable_generation_mw"] / denominator
    df["fossil_share"] = df["fossil_generation_mw"] / denominator

    df["residual_load_mw"] = load - df["renewable_generation_mw"]

    df["day_of_week"] = df["datetime_berlin"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6])

    logger.info(
        "Built features for %s rows; output has %s columns",
        len(df),
        len(df.columns),
    )

    return df
    