import logging

from sqlalchemy import text

from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def test_core_loaded_from_raw():
    engine = get_engine()

    queries = {
        "count": """
            SELECT COUNT(*) FROM core.energy_hourly
        """,
        "preview": """
            SELECT *
            FROM core.energy_hourly
            ORDER BY datetime_utc
            LIMIT 10
        """,
    }

    with engine.connect() as conn:
        count = conn.execute(text(queries["count"])).scalar()
        rows = conn.execute(text(queries["preview"])).fetchall()

    print(f"\nRows in core.energy_hourly: {count}")
    print("\nPreview:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    test_core_loaded_from_raw()
    