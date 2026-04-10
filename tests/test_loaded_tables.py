import logging

import pandas as pd
from sqlalchemy import text

from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

engine = get_engine()

tables = [
    ("core", "energy_hourly"),
    ("mart", "energy_features_hourly"),
    ("mart", "energy_summary_daily"),
]

with engine.connect() as conn:
    for schema, table in tables:
        print(f"\n=== {schema}.{table} ===")

        count_query = text(f"SELECT COUNT(*) AS row_count FROM {schema}.{table}")
        row_count = conn.execute(count_query).scalar()
        print("row_count:", row_count)

        sample_df = pd.read_sql(
            text(f"SELECT * FROM {schema}.{table} ORDER BY 1 LIMIT 5"),
            conn,
        )
        print("\npreview:")
        print(sample_df.to_string(index=False))

        columns_query = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
            ORDER BY ordinal_position
            """
        )
        columns = conn.execute(
            columns_query,
            {"schema": schema, "table": table}
        ).fetchall()

        print("\ncolumns:")
        for col in columns:
            print("-", col[0])
            