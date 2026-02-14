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
            "portfolioManager": "Manu Kapoor",
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
            "portfolioManager": "Kevin Chen",
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

    stats = ingest_rfq_csv(csv_path=csv_path, db_path=str(db_path))
    assert stats["rows_read"] == 2
    assert stats["rows_valid"] == 2
    assert stats["features_upserted"] > 0
    assert stats["pms_created"] == 2

    conn = get_connection(str(db_path))
    repo = Repository(conn)
    clients = repo.list_clients()
    assert len(clients) == 2
    profiles = repo.list_entity_profile_cache_bulk("CLIENT", [int(c["client_id"]) for c in clients])
    assert len(profiles) == 2
    pms = repo.list_pms()
    assert len(pms) == 2
    pm_features = conn.execute(
        "SELECT COUNT(*) AS n FROM rfq_entity_feature_agg WHERE entity_type = 'PM'"
    ).fetchone()["n"]
    assert pm_features > 0
    pm_profiles = conn.execute(
        "SELECT COUNT(*) AS n FROM entity_profile_cache WHERE entity_type = 'PM'"
    ).fetchone()["n"]
    assert pm_profiles == 2
    feature_count = conn.execute("SELECT COUNT(*) AS n FROM rfq_entity_feature_agg WHERE entity_type = 'CLIENT'").fetchone()["n"]
    assert feature_count > 0
    ceemea_count = conn.execute(
        "SELECT COUNT(*) AS n FROM rfq_entity_feature_agg WHERE region = 'CEEMEA'"
    ).fetchone()["n"]
    assert ceemea_count > 0
    conn.close()


def test_ingest_rfq_csv_accumulates_overlapping_keys(tmp_path: Path) -> None:
    db_path = tmp_path / "rfq_additive.db"

    first_csv = tmp_path / "rfq_first.csv"
    second_csv = tmp_path / "rfq_second.csv"

    row_first = {
        "aspen_l2_client_name": "CITADEL ADVISORS LLC",
        "portfolioManager": "Kevin Chen",
        "CcyPair": "USDJPY",
        "Client Region": "AMERICA",
        "Client Sector": "HF / Real Money",
        "clientregion": "UNITED STATES",
        "date": "1/29/2026",
        "NotionalBucket": "50-100M eur",
        "productType": "RKO",
        "Sales": "Max Formato",
        "Sales_Region": "Americas",
        "salesdesk": "FX IG AND EM SALES",
        "Tenor": "7",
        "tenor_bucket": "1W",
        "Hit Notional": "67.00M",
    }
    row_second = {
        "aspen_l2_client_name": "CITADEL ADVISORS LLC",
        "portfolioManager": "Kevin Chen",
        "CcyPair": "USDJPY",
        "Client Region": "AMERICA",
        "Client Sector": "HF / Real Money",
        "clientregion": "UNITED STATES",
        "date": "2/02/2026",
        "NotionalBucket": "25-50M eur",
        "productType": "RKO",
        "Sales": "Nicole Tay",
        "Sales_Region": "Americas",
        "salesdesk": "FX IG AND EM SALES",
        "Tenor": "7",
        "tenor_bucket": "1W",
        "Hit Notional": "33.00M",
    }

    with first_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row_first.keys()))
        writer.writeheader()
        writer.writerow(row_first)
    with second_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row_second.keys()))
        writer.writeheader()
        writer.writerow(row_second)

    stats_first = ingest_rfq_csv(csv_path=first_csv, db_path=str(db_path))
    assert stats_first["rows_valid"] == 1
    assert stats_first["pms_created"] == 1

    stats_second = ingest_rfq_csv(csv_path=second_csv, db_path=str(db_path))
    assert stats_second["rows_valid"] == 1

    conn = get_connection(str(db_path))
    client_pair_product = conn.execute(
        """
        SELECT trade_count, hit_notional_sum_m
        FROM rfq_entity_feature_agg
        WHERE entity_type = 'CLIENT'
          AND region = 'AMERICA'
          AND feature_kind = 'PAIR_PRODUCT'
          AND ccy_pair = 'USDJPY'
          AND product_type = 'RKO'
        """
    ).fetchone()
    assert client_pair_product is not None
    assert int(client_pair_product["trade_count"]) == 2
    assert float(client_pair_product["hit_notional_sum_m"]) == 100.0

    pm_pair_product = conn.execute(
        """
        SELECT trade_count, hit_notional_sum_m
        FROM rfq_entity_feature_agg
        WHERE entity_type = 'PM'
          AND region = 'AMERICA'
          AND feature_kind = 'PAIR_PRODUCT'
          AND ccy_pair = 'USDJPY'
          AND product_type = 'RKO'
        """
    ).fetchone()
    assert pm_pair_product is not None
    assert int(pm_pair_product["trade_count"]) == 2
    assert float(pm_pair_product["hit_notional_sum_m"]) == 100.0
    conn.close()
