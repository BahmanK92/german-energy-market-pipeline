import logging

from src.load.postgres import get_engine, test_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

engine = get_engine()
test_connection(engine)

print("Postgres connection OK")
