import csv
from pathlib import Path

from core.database import initialize_database
from core.repository import Repository
from scripts.ingest_pm_csv import ingest_pm_csv


def test_ingest_pm_csv_creates_pm_and_features(tmp_path: Path) -> None:
    db_path = tmp_path / "pm.db"
    conn = initialize_database(str(db_path))
    repo = Repository(conn)
    client_id = repo.create_client("BREVAN HOWARD ASSET MANAGEMENT", "HF_MACRO", 1)
    repo.add_client_alias(client_id, "Brevan", source="MANUAL")
    conn.close()

    csv_path = tmp_path / "pm.csv"
    rows = [
        {
            "Client": "Brevan",
            "PM": "PM A",
            "PreferenceText": "Likes EURUSD DIGKNO ideas around central bank events.",
            "CcyPair": "EURUSD",
            "productType": "DIGKNO",
            "Client Region": "EUROPE",
            "clientregion": "UNITED KINGDOM",
            "date": "1/31/2026",
        }
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    stats = ingest_pm_csv(csv_path=csv_path, db_path=str(db_path), clear_existing_pm_features=True)
    assert stats["rows_read"] == 1
    assert stats["rows_valid"] == 1
    assert stats["pms_created"] == 1
    assert stats["pm_features_upserted"] > 0

    conn = initialize_database(str(db_path))
    repo = Repository(conn)
    pms = repo.list_pms()
    assert len(pms) == 1
    pm_id = int(pms[0]["pm_id"])
    pm_obs = repo.list_pm_observations(pm_id)
    assert len(pm_obs) == 1
    pm_feat = repo.list_rfq_features_for_entity("PM", pm_id)
    assert len(pm_feat) > 0
    conn.close()

