import logging

from sqlalchemy import text

from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def test_bootstrap_db():
    engine = get_engine()

    schema_query = text(
        """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name IN ('raw', 'core', 'mart')
        ORDER BY schema_name
        """
    )

    table_query = text(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE (table_schema, table_name) IN (
            ('raw', 'smard_timeseries_long'),
            ('core', 'energy_hourly'),
            ('mart', 'energy_features_hourly'),
            ('mart', 'energy_summary_daily')
        )
        ORDER BY table_schema, table_name
        """
    )

    pk_query = text(
        """
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            con.conname AS constraint_name
        FROM pg_constraint con
        JOIN pg_class c
            ON c.oid = con.conrelid
        JOIN pg_namespace n
            ON n.oid = c.relnamespace
        WHERE con.contype = 'p'
          AND (n.nspname, c.relname) IN (
            ('raw', 'smard_timeseries_long'),
            ('core', 'energy_hourly'),
            ('mart', 'energy_features_hourly'),
            ('mart', 'energy_summary_daily')
          )
        ORDER BY n.nspname, c.relname
        """
    )

    with engine.connect() as conn:
        schemas = conn.execute(schema_query).fetchall()
        tables = conn.execute(table_query).fetchall()
        pks = conn.execute(pk_query).fetchall()

    schema_names = {row[0] for row in schemas}
    expected_schemas = {"raw", "core", "mart"}
    assert schema_names == expected_schemas, (
        f"Expected schemas {expected_schemas}, got {schema_names}"
    )

    table_names = {(row[0], row[1]) for row in tables}
    expected_tables = {
        ("raw", "smard_timeseries_long"),
        ("core", "energy_hourly"),
        ("mart", "energy_features_hourly"),
        ("mart", "energy_summary_daily"),
    }
    assert table_names == expected_tables, (
        f"Expected tables {expected_tables}, got {table_names}"
    )

    pk_tables = {(row[0], row[1]) for row in pks}
    expected_pk_tables = {
        ("raw", "smard_timeseries_long"),
        ("core", "energy_hourly"),
        ("mart", "energy_features_hourly"),
        ("mart", "energy_summary_daily"),
    }
    assert pk_tables == expected_pk_tables, (
        f"Expected primary keys on {expected_pk_tables}, got {pk_tables}"
    )

    print("\\nBootstrap DB test passed successfully.")
    print(f"Schemas found: {sorted(schema_names)}")
    print(f"Tables found: {sorted(table_names)}")
    print(f"PK tables found: {sorted(pk_tables)}")


if __name__ == "__main__":
    test_bootstrap_db()