from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import shutil
import sys
import time


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.constants import DEFAULT_DB_PATH
from core.database import initialize_database
from core.logging_utils import get_logger
from scripts.ingest_pm_csv import ingest_pm_csv
from scripts.ingest_rfq_csv import ingest_rfq_csv
from scripts.ingest_trade_csv import ingest_trade_csv
logger = get_logger("reset_and_rebuild")


def _backup_db(db_path: Path) -> Path | None:
    if not db_path.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_name(f"{db_path.stem}.backup_{stamp}{db_path.suffix}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def _remove_sqlite_files(db_path: Path) -> None:
    for suffix in ["", "-wal", "-shm"]:
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset DB and rebuild using RFQ canonical names, then optional trade/PM ingestion."
    )
    parser.add_argument("--rfq-csv", required=True, help="Path to RFQ CSV file.")
    parser.add_argument("--trade-csv", default=None, help="Optional path to trade CSV file.")
    parser.add_argument("--pm-csv", default=None, help="Optional path to PM CSV file.")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite DB path.")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup of existing DB file.")
    args = parser.parse_args()

    db_path = Path(args.db)
    start = time.time()
    logger.info(
        "reset_rebuild_start db=%s rfq=%s trade=%s pm=%s",
        db_path,
        args.rfq_csv,
        args.trade_csv,
        args.pm_csv,
    )

    backup_path = None
    if not args.no_backup:
        backup_path = _backup_db(db_path)

    _remove_sqlite_files(db_path)
    conn = initialize_database(str(db_path))
    conn.close()

    rfq_stats = ingest_rfq_csv(Path(args.rfq_csv), db_path=str(db_path))
    trade_stats = None
    pm_stats = None

    if args.trade_csv:
        trade_stats = ingest_trade_csv(Path(args.trade_csv), db_path=str(db_path))
    if args.pm_csv:
        pm_stats = ingest_pm_csv(Path(args.pm_csv), db_path=str(db_path), clear_existing_pm_features=True)

    elapsed = time.time() - start
    print("Reset and rebuild complete.")
    if backup_path:
        print(f"Backup created: {backup_path}")
    print(f"RFQ stats: {rfq_stats}")
    if trade_stats:
        print(f"Trade stats: {trade_stats}")
    if pm_stats:
        print(f"PM stats: {pm_stats}")
    print(f"Elapsed seconds: {elapsed:.2f}")
    logger.info(
        "reset_rebuild_done elapsed=%.2f rfq=%s trade=%s pm=%s backup=%s",
        elapsed,
        rfq_stats,
        trade_stats,
        pm_stats,
        backup_path,
    )


if __name__ == "__main__":
    main()
