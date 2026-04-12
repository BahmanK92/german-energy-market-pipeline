import logging

from src.load.postgres import get_engine, truncate_table
from src.transform.build_core_hourly_from_raw import build_core_hourly_from_raw

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def build_and_load_core_from_raw() -> None:
    engine = get_engine()

    core_df = build_core_hourly_from_raw(engine=engine)

    logger.info("Truncating core.energy_hourly ...")
    truncate_table(engine=engine, table_name="energy_hourly", schema="core")

    logger.info("Appending rows into core.energy_hourly ...")
    core_df.to_sql(
        name="energy_hourly",
        con=engine,
        schema="core",
        if_exists="append",
        index=False,
        method="multi",
    )

    logger.info(
        "core.energy_hourly written successfully with %s rows and %s columns",
        len(core_df),
        len(core_df.columns),
    )


if __name__ == "__main__":
    build_and_load_core_from_raw()