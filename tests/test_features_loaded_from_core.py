import logging

from sqlalchemy import text

from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def test_features_loaded_from_core():
    engine = get_engine()

    queries = {
        "count": """
            SELECT COUNT(*) FROM mart.energy_features_hourly
        """,
        "preview": """
            SELECT
                datetime_utc,
                datetime_berlin,
                load_mw,
                renewable_generation_mw,
                fossil_generation_mw,
                renewable_share,
                fossil_share,
                residual_load_mw,
                day_of_week,
                is_weekend
            FROM mart.energy_features_hourly
            ORDER BY datetime_utc
            LIMIT 10
        """,
    }

    with engine.connect() as conn:
        count = conn.execute(text(queries["count"])).scalar()
        rows = conn.execute(text(queries["preview"])).fetchall()

    print(f"\nRows in mart.energy_features_hourly: {count}")
    print("\nPreview:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    test_features_loaded_from_core()