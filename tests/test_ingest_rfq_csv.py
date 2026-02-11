import csv
from pathlib import Path

from core.database import get_connection
from core.repository import Repository
from scripts.ingest_rfq_csv import ingest_rfq_csv


def test_ingest_rfq_csv_builds_aggregates_and_profiles(tmp_path: Path) -> None:
    db_path = tmp_path / "rfq.db"
    csv_path = tmp_path / "rfq.csv"

    rows = [
        {
            "aspen_l2_client_name": "BREVAN HOWARD ASSET MANAGEMENT",
            "CcyPair": "EURUSD",
            "Client Region": "EUROPE",
            "Client Sector": "HF / Real Money",
            "clientregion": "UNITED KINGDOM",
            "date": "1/28/2026",
            "NotionalBucket": "5-25M eur",
            "productType": "DIGKNO",
            "Sales": "Alice",
            "Sales_Region": "Europe",
            "salesdesk": "Investor - London",
            "Tenor": "7",
            "tenor_bucket": "1W",
            "Hit Notional": "15.00M",
        },
        {
            "aspen_l2_client_name": "CITADEL ADVISORS LLC",
            "CcyPair": "USDINR",
            "Client Region": "CEEMA",
            "Client Sector": "HF / Real Money",
            "clientregion": "TURKEY",
            "date": "02-04-2026",
            "NotionalBucket": "5-25M eur",
            "productType": "KNO",
            "Sales": "Bob",
            "Sales_Region": "Europe",
            "salesdesk": "EM - London",
            "Tenor": "14",
            "tenor_bucket": "2W-1M",
            "Hit Notional": "10.00M",
        },
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    stats = ingest_rfq_csv(csv_path=csv_path, db_path=str(db_path), clear_existing=True)
    assert stats["rows_read"] == 2
    assert stats["rows_valid"] == 2
    assert stats["features_upserted"] > 0

    conn = get_connection(str(db_path))
    repo = Repository(conn)
    clients = repo.list_clients()
    assert len(clients) == 2
    profiles = repo.list_entity_profile_cache_bulk("CLIENT", [int(c["client_id"]) for c in clients])
    assert len(profiles) == 2
    feature_count = conn.execute("SELECT COUNT(*) AS n FROM rfq_entity_feature_agg WHERE entity_type = 'CLIENT'").fetchone()["n"]
    assert feature_count > 0
    ceemea_count = conn.execute(
        "SELECT COUNT(*) AS n FROM rfq_entity_feature_agg WHERE region = 'CEEMEA'"
    ).fetchone()["n"]
    assert ceemea_count > 0
    conn.close()

