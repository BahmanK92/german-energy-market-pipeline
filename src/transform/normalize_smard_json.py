import logging
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def normalize_smard_payload(raw_batch: Dict) -> pd.DataFrame:
    """
    Convert one raw SMARD batch payload into a normalized hourly DataFrame.

    Expected input:
    {
        "series_key": ...,
        "filter": ...,
        "region": ...,
        "resolution": ...,
        "value_column": ...,
        "source_batch_timestamp": ...,
        "payload": {
            "meta_data": {...},
            "series": [[timestamp_ms, value], ...]
        }
    }
    """
    required_keys = [
        "series_key",
        "region",
        "resolution",
        "value_column",
        "source_batch_timestamp",
        "payload",
    ]
    for key in required_keys:
        if key not in raw_batch:
            raise KeyError(f"Missing required key in raw_batch: {key}")

    payload = raw_batch["payload"]
    series = payload.get("series", [])

    if not isinstance(series, list):
        raise ValueError("Expected payload['series'] to be a list")

    df = pd.DataFrame(series, columns=["timestamp_ms", "value"])

    if df.empty:
        logger.warning(
            "Empty series payload for series '%s' at batch timestamp %s",
            raw_batch["series_key"],
            raw_batch["source_batch_timestamp"],
        )
        return pd.DataFrame(
            columns=[
                "series_key",
                "value_name",
                "region",
                "resolution",
                "source_batch_timestamp",
                "datetime_utc",
                "datetime_berlin",
                "date_berlin",
                "hour_of_day",
                "value",
            ]
        )

    # Convert timestamp/value types
    df["timestamp_ms"] = pd.to_numeric(df["timestamp_ms"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Parse datetime in UTC, then convert to Europe/Berlin
    df["datetime_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True, errors="coerce")
    df["datetime_berlin"] = df["datetime_utc"].dt.tz_convert("Europe/Berlin")

    # Add date/time helper columns
    df["date_berlin"] = df["datetime_berlin"].dt.date
    df["hour_of_day"] = df["datetime_berlin"].dt.hour

    # Add metadata columns
    df["series_key"] = raw_batch["series_key"]
    df["value_name"] = raw_batch["value_column"]
    df["region"] = raw_batch["region"]
    df["resolution"] = raw_batch["resolution"]
    df["source_batch_timestamp"] = raw_batch["source_batch_timestamp"]

    # Reorder columns
    df = df[
        [
            "series_key",
            "value_name",
            "region",
            "resolution",
            "source_batch_timestamp",
            "timestamp_ms",
            "datetime_utc",
            "datetime_berlin",
            "date_berlin",
            "hour_of_day",
            "value",
        ]
    ]

    # Basic cleaning
    df = df.dropna(subset=["datetime_utc"])
    df = df.drop_duplicates(subset=["series_key", "datetime_utc"])
    df = df.sort_values("datetime_utc").reset_index(drop=True)

    logger.info(
        "Normalized %s rows for series '%s' at batch timestamp %s",
        len(df),
        raw_batch["series_key"],
        raw_batch["source_batch_timestamp"],
    )

    return df