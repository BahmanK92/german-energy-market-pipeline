import logging

from scripts.smoke_check_phase1 import smoke_check_phase1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def test_smoke_check_phase1():
    smoke_check_phase1()
    print("\nSmoke check test passed successfully.")


if __name__ == "__main__":
    test_smoke_check_phase1()