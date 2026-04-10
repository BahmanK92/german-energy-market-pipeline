import logging

from sqlalchemy import text

from src.load.postgres import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

engine = get_engine()

query = """
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('raw', 'core', 'mart')
ORDER BY schema_name;
"""

with engine.connect() as conn:
    rows = conn.execute(text(query)).fetchall()

print("Found schemas:")
for row in rows:
    print("-", row[0])
    