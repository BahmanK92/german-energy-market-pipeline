import logging

from src.load.postgres import get_engine
from src.transform.build_core_hourly_from_raw import build_core_hourly_from_raw

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def test_build_core_from_raw():
    engine = get_engine()

    core_df = build_core_hourly_from_raw(
        engine=engine,
        series_keys=["price", "load", "solar"],
    )

    print("\n=== CORE FROM RAW ===")
    print(core_df.head(10).to_string())
    print("\nshape:", core_df.shape)
    print("\ncolumns:", list(core_df.columns))


if __name__ == "__main__":
    test_build_core_from_raw()