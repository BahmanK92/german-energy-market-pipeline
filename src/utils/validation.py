import logging
from typing import Iterable

import pandas as pd

logger = logging.getLogger(__name__)


def require_columns(df: pd.DataFrame, required_columns: Iterable[str], df_name: str) -> None:
    required_columns = set(required_columns)
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {sorted(missing)}")


def assert_not_empty(df: pd.DataFrame, df_name: str) -> None:
    if df.empty:
        raise ValueError(f"{df_name} is empty")


def assert_no_duplicate_keys(df: pd.DataFrame, key_columns: list[str], df_name: str) -> None:
    duplicate_mask = df.duplicated(subset=key_columns, keep=False)
    duplicate_count = int(duplicate_mask.sum())

    if duplicate_count > 0:
        raise ValueError(
            f"{df_name} has {duplicate_count} duplicate rows for key columns {key_columns}"
        )


def assert_non_null_fraction_below(
    df: pd.DataFrame,
    column_name: str,
    max_null_fraction: float,
    df_name: str,
) -> None:
    if column_name not in df.columns:
        raise ValueError(f"{df_name} does not contain column '{column_name}'")

    null_fraction = float(df[column_name].isna().mean())
    if null_fraction > max_null_fraction:
        raise ValueError(
            f"{df_name}.{column_name} null fraction {null_fraction:.4f} exceeds "
            f"allowed maximum {max_null_fraction:.4f}"
        )


def validate_share_columns(features_df: pd.DataFrame, tolerance: float = 1e-6) -> None:
    require_columns(
        features_df,
        ["renewable_share", "fossil_share", "total_generation_selected_mw"],
        "features_df",
    )

    mask = features_df["total_generation_selected_mw"].fillna(0) > 0
    checked_df = features_df.loc[mask].copy()

    if checked_df.empty:
        logger.warning("No rows with positive total_generation_selected_mw to validate share columns")
        return

    share_sum = checked_df["renewable_share"].fillna(0) + checked_df["fossil_share"].fillna(0)
    max_deviation = float((share_sum - 1.0).abs().max())

    if max_deviation > tolerance:
        raise ValueError(
            f"renewable_share + fossil_share deviates from 1.0 by up to {max_deviation:.8f}, "
            f"which exceeds tolerance {tolerance}"
        )

    logger.info("Validated renewable_share + fossil_share ≈ 1.0")


def validate_residual_load_difference(
    features_df: pd.DataFrame,
    mean_abs_diff_threshold: float = 10000.0,
) -> None:
    require_columns(
        features_df,
        ["residual_load_mw", "residual_load_smard_mw"],
        "features_df",
    )

    comparable_df = features_df[
        ["residual_load_mw", "residual_load_smard_mw"]
    ].dropna()

    if comparable_df.empty:
        logger.warning("No comparable rows available for residual load validation")
        return

    abs_diff = (
        comparable_df["residual_load_mw"] - comparable_df["residual_load_smard_mw"]
    ).abs()

    mean_abs_diff = float(abs_diff.mean())
    max_abs_diff = float(abs_diff.max())

    logger.info(
        "Residual load comparison: mean_abs_diff=%.2f, max_abs_diff=%.2f",
        mean_abs_diff,
        max_abs_diff,
    )

    if mean_abs_diff > mean_abs_diff_threshold:
        raise ValueError(
            f"Mean absolute difference between residual_load_mw and residual_load_smard_mw "
            f"is {mean_abs_diff:.2f}, exceeding threshold {mean_abs_diff_threshold:.2f}"
        )


def run_phase1_dataframe_validations(
    raw_df: pd.DataFrame,
    core_df: pd.DataFrame,
    features_df: pd.DataFrame,
    daily_df: pd.DataFrame,
) -> None:
    assert_not_empty(raw_df, "raw_df")
    assert_not_empty(core_df, "core_df")
    assert_not_empty(features_df, "features_df")
    assert_not_empty(daily_df, "daily_df")

    require_columns(
        raw_df,
        ["series_key", "datetime_utc", "value"],
        "raw_df",
    )
    require_columns(
        core_df,
        ["datetime_utc", "datetime_berlin", "date_berlin", "hour_of_day"],
        "core_df",
    )
    require_columns(
        features_df,
        [
            "datetime_utc",
            "renewable_generation_mw",
            "fossil_generation_mw",
            "renewable_share",
            "fossil_share",
            "residual_load_mw",
            "residual_load_smard_mw",
        ],
        "features_df",
    )
    require_columns(
        daily_df,
        ["date_berlin", "avg_price_eur_mwh", "row_count_hours"],
        "daily_df",
    )

    assert_no_duplicate_keys(raw_df, ["series_key", "datetime_utc"], "raw_df")
    assert_no_duplicate_keys(core_df, ["datetime_utc"], "core_df")
    assert_no_duplicate_keys(features_df, ["datetime_utc"], "features_df")
    assert_no_duplicate_keys(daily_df, ["date_berlin"], "daily_df")

    assert_non_null_fraction_below(core_df, "datetime_utc", 0.0, "core_df")
    assert_non_null_fraction_below(features_df, "datetime_utc", 0.0, "features_df")
    assert_non_null_fraction_below(daily_df, "date_berlin", 0.0, "daily_df")

    validate_share_columns(features_df)
    validate_residual_load_difference(features_df)

    logger.info("All Phase 1 dataframe validations passed successfully")