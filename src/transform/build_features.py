import logging

import pandas as pd

logger = logging.getLogger(__name__)


def build_features(core_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build Phase 1 feature columns from the core hourly table.

    Expected columns may include:
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

    The function is defensive:
    if a source column is missing, it is treated as 0 for aggregate features.
    """
    if core_df.empty:
        logger.warning("Received empty core_df in build_features")
        return core_df.copy()

    df = core_df.copy()

    def series_or_zero(column_name: str) -> pd.Series:
        if column_name in df.columns:
            return pd.to_numeric(df[column_name], errors="coerce").fillna(0)
        return pd.Series(0, index=df.index, dtype="float64")

    # Base components
    solar = series_or_zero("solar_mw")
    wind_onshore = series_or_zero("wind_onshore_mw")
    wind_offshore = series_or_zero("wind_offshore_mw")
    biomass = series_or_zero("biomass_mw")
    hydro = series_or_zero("hydro_mw")

    lignite = series_or_zero("lignite_mw")
    hard_coal = series_or_zero("hard_coal_mw")
    gas = series_or_zero("gas_mw")
    other_conventional = series_or_zero("other_conventional_mw")

    load = pd.to_numeric(df["load_mw"], errors="coerce") if "load_mw" in df.columns else pd.Series(
        pd.NA, index=df.index, dtype="float64"
    )

    # Feature engineering
    df["coal_mw"] = lignite + hard_coal

    df["renewable_generation_mw"] = (
        solar + wind_onshore + wind_offshore + biomass + hydro
    )

    df["fossil_generation_mw"] = (
        lignite + hard_coal + gas + other_conventional
    )

    df["total_generation_selected_mw"] = (
        df["renewable_generation_mw"] + df["fossil_generation_mw"]
    )

    # Shares: avoid division by zero
    denominator = df["total_generation_selected_mw"].replace(0, pd.NA)

    df["renewable_share"] = df["renewable_generation_mw"] / denominator
    df["fossil_share"] = df["fossil_generation_mw"] / denominator

    # Residual load using engineered renewable generation
    if "load_mw" in df.columns:
        df["residual_load_mw"] = load - df["renewable_generation_mw"]
    else:
        df["residual_load_mw"] = pd.NA

    # Time features
    if "datetime_berlin" in df.columns:
        df["day_of_week"] = df["datetime_berlin"].dt.dayofweek
        df["is_weekend"] = df["day_of_week"].isin([5, 6])
    else:
        raise ValueError("core_df must contain 'datetime_berlin'")

    logger.info(
        "Built features for %s rows; output has %s columns",
        len(df),
        len(df.columns),
    )

    return df
    