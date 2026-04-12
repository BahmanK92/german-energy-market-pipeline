import logging

from sqlalchemy import text

from scripts.build_daily_summary_from_features import (
    build_and_load_daily_summary_from_features,
)
from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def get_daily_table_metadata(engine):
    query = text(
        """
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'mart'
          AND table_name = 'energy_summary_daily'
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
          AND c.relname = 'energy_summary_daily'
        """
    )

    count_query = text("SELECT COUNT(*) FROM mart.energy_summary_daily")

    with engine.connect() as conn:
        columns = conn.execute(query).fetchall()
        pk = conn.execute(pk_query).fetchall()
        count = conn.execute(count_query).scalar()

    return {
        "columns": columns,
        "pk": pk,
        "count": count,
    }


def test_daily_summary_schema_stable():
    engine = get_engine()

    build_and_load_daily_summary_from_features()
    first = get_daily_table_metadata(engine)

    build_and_load_daily_summary_from_features()
    second = get_daily_table_metadata(engine)

    assert first["count"] > 0, "mart.energy_summary_daily should contain rows after load"
    assert second["count"] > 0, "mart.energy_summary_daily should contain rows after second load"

    assert first["columns"] == second["columns"], (
        "mart.energy_summary_daily schema changed between runs"
    )

    assert first["pk"], "mart.energy_summary_daily should have a primary key"
    assert second["pk"], "mart.energy_summary_daily should keep its primary key after rerun"

    print("\nDaily summary schema stability test passed successfully.")
    print(f"Row count after first run: {first['count']}")
    print(f"Row count after second run: {second['count']}")
    print(f"Primary key info: {second['pk']}")


if __name__ == "__main__":
    test_daily_summary_schema_stable()