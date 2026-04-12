import logging

from sqlalchemy import text

from scripts.build_features_from_core import build_and_load_features_from_core
from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def get_features_table_metadata(engine):
    query = text(
        """
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'mart'
          AND table_name = 'energy_features_hourly'
        ORDER BY ordinal_position
        """
    )

    pk_query = text(
        """
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class c
            ON c.oid = con.conrelid
        JOIN pg_namespace n
            ON n.oid = c.relnamespace
        WHERE con.contype = 'p'
          AND n.nspname = 'mart'
          AND c.relname = 'energy_features_hourly'
        """
    )

    count_query = text("SELECT COUNT(*) FROM mart.energy_features_hourly")

    with engine.connect() as conn:
        columns = conn.execute(query).fetchall()
        pk = conn.execute(pk_query).fetchall()
        count = conn.execute(count_query).scalar()

    return {
        "columns": columns,
        "pk": pk,
        "count": count,
    }


def test_features_table_schema_stable():
    engine = get_engine()

    build_and_load_features_from_core()
    first = get_features_table_metadata(engine)

    build_and_load_features_from_core()
    second = get_features_table_metadata(engine)

    assert first["count"] > 0, "mart.energy_features_hourly should contain rows after load"
    assert second["count"] > 0, "mart.energy_features_hourly should contain rows after second load"

    assert first["columns"] == second["columns"], (
        "mart.energy_features_hourly schema changed between runs"
    )

    assert first["pk"], "mart.energy_features_hourly should have a primary key"
    assert second["pk"], "mart.energy_features_hourly should keep its primary key after rerun"

    print("\nFeatures table schema stability test passed successfully.")
    print(f"Row count after first run: {first['count']}")
    print(f"Row count after second run: {second['count']}")
    print(f"Primary key info: {second['pk']}")


if __name__ == "__main__":
    test_features_table_schema_stable()
    