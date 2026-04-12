import logging

from sqlalchemy import text

from scripts.build_core_from_raw import build_and_load_core_from_raw
from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def get_core_table_metadata(engine):
    query = text(
        """
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'core'
          AND table_name = 'energy_hourly'
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
          AND n.nspname = 'core'
          AND c.relname = 'energy_hourly'
        """
    )

    count_query = text("SELECT COUNT(*) FROM core.energy_hourly")

    with engine.connect() as conn:
        columns = conn.execute(query).fetchall()
        pk = conn.execute(pk_query).fetchall()
        count = conn.execute(count_query).scalar()

    return {
        "columns": columns,
        "pk": pk,
        "count": count,
    }


def test_core_table_schema_stable():
    engine = get_engine()

    build_and_load_core_from_raw()
    first = get_core_table_metadata(engine)

    build_and_load_core_from_raw()
    second = get_core_table_metadata(engine)

    assert first["count"] > 0, "core.energy_hourly should contain rows after load"
    assert second["count"] > 0, "core.energy_hourly should contain rows after second load"

    assert first["columns"] == second["columns"], (
        "core.energy_hourly schema changed between runs"
    )

    assert first["pk"], "core.energy_hourly should have a primary key"
    assert second["pk"], "core.energy_hourly should keep its primary key after rerun"

    print("\nCore table schema stability test passed successfully.")
    print(f"Row count after first run: {first['count']}")
    print(f"Row count after second run: {second['count']}")
    print(f"Primary key info: {second['pk']}")


if __name__ == "__main__":
    test_core_table_schema_stable()
    