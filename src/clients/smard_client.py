import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.settings import (
    DEFAULT_HEADERS,
    DEFAULT_TIMEOUT_SECONDS,
    SMARD_BASE_URL,
    SMARD_FILTERS_FILE,
)

logger = logging.getLogger(__name__)


class SmardClient:
    """
    Lightweight client for fetching SMARD chart/index/data payloads.

    Responsibilities:
    - read series config from YAML
    - build SMARD URLs
    - handle retries / HTTP requests
    - return raw API JSON cleanly

    Not responsible for:
    - dataframe normalization
    - feature engineering
    - database loading
    """

    def __init__(
        self,
        filters_file: Path = SMARD_FILTERS_FILE,
        base_url: str = SMARD_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.headers = headers or DEFAULT_HEADERS
        self.series_config = self._load_filters(filters_file)
        self.session = self._build_session()

    def _load_filters(self, filters_file: Path) -> Dict[str, Dict[str, Any]]:
        if not Path(filters_file).exists():
            raise FileNotFoundError(f"SMARD filters file not found: {filters_file}")

        with open(filters_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        if not config or "series" not in config:
            raise ValueError("Invalid SMARD filters config: missing top-level 'series' key")

        return config["series"]

    def _build_session(self) -> requests.Session:
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(self.headers)

        return session

    def _get_json(self, url: str) -> Any:
        logger.info("Requesting URL: %s", url)

        response = self.session.get(url, timeout=self.timeout)

        if not response.ok:
            logger.error("HTTP %s for URL %s", response.status_code, url)
            logger.error("Response preview: %s", response.text[:500])
            response.raise_for_status()

        return response.json()

    def _get_series_meta(self, series_key: str) -> Dict[str, Any]:
        if series_key not in self.series_config:
            valid_keys = ", ".join(sorted(self.series_config.keys()))
            raise KeyError(f"Unknown series_key='{series_key}'. Valid keys: {valid_keys}")
        return self.series_config[series_key]

    def _build_index_url(self, filter_id: int, region: str, resolution: str) -> str:
        """
        Example pattern:
        https://www.smard.de/app/chart_data/<filter>/<region>/index_<resolution>.json
        """
        return f"{self.base_url}/{filter_id}/{region}/index_{resolution}.json"

    def _build_data_url(self, filter_id: int, region: str, resolution: str, timestamp: int) -> str:
        """
        Example pattern:
        https://www.smard.de/app/chart_data/<filter>/<region>/<filter>_<region>_<resolution>_<timestamp>.json
        """
        return f"{self.base_url}/{filter_id}/{region}/{filter_id}_{region}_{resolution}_{timestamp}.json"

    def get_available_timestamps(self, series_key: str) -> List[int]:
        """
        Fetch available batch timestamps for one SMARD series.
        """
        meta = self._get_series_meta(series_key)
        filter_id = meta["filter"]
        region = meta["region"]
        resolution = meta["resolution"]

        url = self._build_index_url(
            filter_id=filter_id,
            region=region,
            resolution=resolution,
        )
        payload = self._get_json(url)

        if not isinstance(payload, dict) or "timestamps" not in payload:
            raise ValueError(
                f"Unexpected index payload for '{series_key}': expected dict with 'timestamps'"
            )

        timestamps = [int(x) for x in payload["timestamps"] if isinstance(x, (int, float, str))]
        timestamps = sorted(set(timestamps))

        logger.info(
            "Found %s timestamps for series '%s'",
            len(timestamps),
            series_key,
        )
        return timestamps

    def get_raw_series(self, series_key: str, timestamp: int) -> Dict[str, Any]:
        """
        Fetch one raw timestamped JSON payload from SMARD.
        """
        meta = self._get_series_meta(series_key)
        filter_id = meta["filter"]
        region = meta["region"]
        resolution = meta["resolution"]

        url = self._build_data_url(
            filter_id=filter_id,
            region=region,
            resolution=resolution,
            timestamp=timestamp,
        )
        payload = self._get_json(url)

        if not isinstance(payload, dict):
            raise ValueError(
                f"Unexpected data payload for '{series_key}' at timestamp {timestamp}: expected dict"
            )

        return {
            "series_key": series_key,
            "filter": filter_id,
            "region": region,
            "resolution": resolution,
            "value_column": meta.get("value_column"),
            "source_batch_timestamp": timestamp,
            "payload": payload,
        }

    def get_latest_raw_series(self, series_key: str) -> Dict[str, Any]:
        """
        Convenience method: fetch the latest available raw payload for one series.
        """
        timestamps = self.get_available_timestamps(series_key)
        if not timestamps:
            raise ValueError(f"No timestamps found for series '{series_key}'")

        latest_timestamp = timestamps[-1]
        logger.info(
            "Fetching latest raw series for '%s' using timestamp %s",
            series_key,
            latest_timestamp,
        )
        return self.get_raw_series(series_key, latest_timestamp)

    def get_named_series(self, series_keys: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Fetch latest raw payload for multiple named series.
        Returns dict keyed by series_key.
        """
        results = {}
        for series_key in series_keys:
            results[series_key] = self.get_latest_raw_series(series_key)
        return results

    def fetch_many(self, series_keys: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Fetch latest raw payloads for many series.
        If series_keys is None, fetch all configured series.
        """
        if series_keys is None:
            series_keys = list(self.series_config.keys())

        logger.info("Fetching %s series from SMARD", len(series_keys))
        return self.get_named_series(series_keys)