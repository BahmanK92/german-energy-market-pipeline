import logging

import pandas as pd

logger = logging.getLogger(__name__)


def build_daily_summary(features_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate hourly feature data into one daily summary row per Berlin date.
    """
    if features_df.empty:
        logger.warning("Received empty features_df in build_daily_summary")
        return features_df.copy()

    required_columns = {
        "date_berlin",
        "price_eur_mwh",
        "load_mw",
        "renewable_generation_mw",
        "fossil_generation_mw",
        "renewable_share",
        "fossil_share",
        "residual_load_mw",
        "residual_load_smard_mw",
        "coal_mw",
        "gas_mw",
    }
    missing = required_columns - set(features_df.columns)
    if missing:
        raise ValueError(f"features_df missing required columns: {missing}")

    daily_df = (
        features_df
        .groupby("date_berlin", as_index=False)
        .agg(
            avg_price_eur_mwh=("price_eur_mwh", "mean"),
            min_price_eur_mwh=("price_eur_mwh", "min"),
            max_price_eur_mwh=("price_eur_mwh", "max"),
            avg_load_mw=("load_mw", "mean"),
            total_load_mwh_est=("load_mw", "sum"),
            avg_residual_load_mw=("residual_load_mw", "mean"),
            avg_residual_load_smard_mw=("residual_load_smard_mw", "mean"),
            avg_renewable_generation_mw=("renewable_generation_mw", "mean"),
            avg_fossil_generation_mw=("fossil_generation_mw", "mean"),
            avg_renewable_share=("renewable_share", "mean"),
            avg_fossil_share=("fossil_share", "mean"),
            avg_coal_mw=("coal_mw", "mean"),
            avg_gas_mw=("gas_mw", "mean"),
            row_count_hours=("date_berlin", "size"),
        )
        .sort_values("date_berlin")
        .reset_index(drop=True)
    )

    logger.info(
        "Built daily summary with %s rows and %s columns",
        len(daily_df),
        len(daily_df.columns),
    )

    return daily_df
    