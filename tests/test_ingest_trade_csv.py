import csv
from pathlib import Path

from core.database import get_connection
from core.repository import Repository
from scripts.ingest_trade_csv import ingest_trade_csv, parse_client_list


def test_parse_client_list_variants() -> None:
    assert parse_client_list('["Client A","Client B"]') == ["Client A", "Client B"]
    assert parse_client_list("['Client A', 'Client B']") == ["Client A", "Client B"]
    assert parse_client_list("Client A, Client B") == ["Client A", "Client B"]
    assert parse_client_list("[]") == []


def test_ingest_trade_csv_end_to_end(tmp_path: Path) -> None:
    db_path = tmp_path / "ingest.db"
    csv_path = tmp_path / "trades.csv"

    rows = [
        {
            "Trade": "3m KO Hedge Idea",
            "Sales Feedback": "Strong interest for central bank event hedge.",
            "Client": '["Client A","Client B"]',
            "Created By": "Alice",
            "Other Column": "ignored",
        },
        {
            "Trade": "EM Carry Basket",
            "Sales Feedback": "HFs asked for carry with defined risk.",
            "Client": '["Client B","Client C"]',
            "Created By": "Bob",
            "Other Column": "ignored",
        },
    ]

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    stats_first = ingest_trade_csv(csv_path=csv_path, db_path=str(db_path))
    assert stats_first["rows_processed"] == 2
    assert stats_first["ideas_created"] == 2
    assert stats_first["clients_created"] == 3
    assert stats_first["observations_created"] == 4

    stats_second = ingest_trade_csv(csv_path=csv_path, db_path=str(db_path))
    assert stats_second["rows_processed"] == 2
    assert stats_second["ideas_created"] == 0
    assert stats_second["ideas_updated"] == 2
    assert stats_second["clients_created"] == 0
    assert stats_second["observations_created"] == 0

    conn = get_connection(str(db_path))
    repo = Repository(conn)
    assert len(repo.list_ideas()) == 2
    assert len(repo.list_clients()) == 3
    assert len(repo.list_observations()) == 4
    conn.close()

