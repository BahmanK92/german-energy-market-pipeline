import logging

from scripts.backfill_smard import backfill_smard
from scripts.build_core_from_raw import build_and_load_core_from_raw
from scripts.build_features_from_core import build_and_load_features_from_core
from scripts.build_daily_summary_from_features import (
    build_and_load_daily_summary_from_features,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def run_phase1_local() -> None:
    """
    Run the DB-backed Phase 1 pipeline locally:

    1. backfill raw SMARD data
    2. build core.energy_hourly from raw
    3. build mart.energy_features_hourly from core
    4. build mart.energy_summary_daily from features
    """
    logger.info("Starting DB-backed local Phase 1 pipeline run")

    backfill_smard()
    build_and_load_core_from_raw()
    build_and_load_features_from_core()
    build_and_load_daily_summary_from_features()

    logger.info("DB-backed local Phase 1 pipeline run finished successfully")


if __name__ == "__main__":
    run_phase1_local()
    