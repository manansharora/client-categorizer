from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.database import initialize_database


def main() -> None:
    conn = initialize_database()
    conn.close()
    print("Database initialized and seeded.")


if __name__ == "__main__":
    main()
