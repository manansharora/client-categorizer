from __future__ import annotations

import argparse
import csv
import math
from datetime import date
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.database import initialize_database
from core.repository import Repository
from core.rfq import (
    normalize_ccy_pair,
    normalize_country,
    normalize_product_type,
    normalize_region,
    normalize_tenor_bucket,
    parse_trade_date,
)


def _normalize_key(value: str) -> str:
    return value.strip().lower()


def _build_row_getter(row: dict[str, str]):
    keyed = {_normalize_key(k): (v or "") for k, v in row.items()}

    def get_field(*names: str) -> str:
        for name in names:
            value = keyed.get(_normalize_key(name), "")
            if value.strip():
                return value.strip()
        return ""

    return get_field


def _decay(days_old: int, window: float) -> float:
    return math.exp(-max(0, days_old) / window)


def ingest_pm_csv(csv_path: Path, db_path: str | None = None, clear_existing_pm_features: bool = True) -> dict[str, int]:
    conn = initialize_database(db_path)
    repo = Repository(conn)

    rows_read = 0
    rows_valid = 0
    rows_skipped = 0
    pms_created = 0
    obs_created = 0
    feature_rows: list[tuple[Any, ...]] = []

    agg_map: dict[tuple[str, int, str, str, str, str | None, str | None, str | None], dict[str, Any]] = {}
    profile_texts: dict[int, list[str]] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row.")

        for row in reader:
            rows_read += 1
            get = _build_row_getter(row)
            client_name = get("Client", "Client Name")
            pm_name = get("PM", "Portfolio Manager")
            pref_text = get("PreferenceText", "Preference", "Notes")

            if not client_name or not pm_name:
                rows_skipped += 1
                continue

            client = repo.resolve_client_by_name_or_alias(client_name)
            if client is None:
                rows_skipped += 1
                continue
            client_id = int(client["client_id"])
            pm = repo.get_pm_by_client_name(client_id, pm_name)
            if pm is None:
                pm_id = repo.upsert_pm(client_id, pm_name, 1)
                pms_created += 1
            else:
                pm_id = int(pm["pm_id"])

            rows_valid += 1

            if pref_text:
                obs_date = parse_trade_date(get("date", "obs_date")) or date.today()
                existing = {
                    (str(obs["obs_type"]), str(obs["obs_text"]).strip(), str(obs["obs_date"]))
                    for obs in repo.list_pm_observations(pm_id)
                }
                key = ("PREFERENCE_NOTE", pref_text.strip(), obs_date.isoformat())
                if key not in existing:
                    repo.add_pm_observation(pm_id, "PREFERENCE_NOTE", pref_text, obs_date, 0.9)
                    obs_created += 1
                profile_texts.setdefault(pm_id, []).append(pref_text.strip())

            ccy_pair = normalize_ccy_pair(get("CcyPair", "ccy_pair"))
            product_type = normalize_product_type(get("productType", "product_type"))
            tenor_bucket = normalize_tenor_bucket(get("tenor_bucket", "tenor"))
            region = normalize_region(get("Client Region", "region"))
            country = normalize_country(get("clientregion", "country"))
            trade_date = parse_trade_date(get("date")) or date.today()
            days_old = max(0, (date.today() - trade_date).days)

            feature_keys = []
            if ccy_pair and product_type and tenor_bucket:
                feature_keys.append(("PAIR_PRODUCT_TENOR", ccy_pair, product_type, tenor_bucket))
            if ccy_pair and product_type:
                feature_keys.append(("PAIR_PRODUCT", ccy_pair, product_type, None))
            if ccy_pair:
                feature_keys.append(("PAIR", ccy_pair, None, None))
            if product_type:
                feature_keys.append(("PRODUCT", None, product_type, None))
            if tenor_bucket:
                feature_keys.append(("TENOR", None, None, tenor_bucket))

            for feature_kind, pair, product, tenor in feature_keys:
                if not region:
                    continue
                agg_key = ("PM", pm_id, region, country, feature_kind, pair, product, tenor)
                agg = agg_map.get(agg_key)
                if agg is None:
                    agg = {
                        "trade_count": 0,
                        "hit_notional_sum_m": 0.0,
                        "last_trade_date": "",
                        "score_30d": 0.0,
                        "score_90d": 0.0,
                        "score_365d": 0.0,
                        "recency_score": 0.0,
                    }
                    agg_map[agg_key] = agg
                agg["trade_count"] += 1
                trade_date_str = trade_date.isoformat()
                if not agg["last_trade_date"] or trade_date_str > agg["last_trade_date"]:
                    agg["last_trade_date"] = trade_date_str
                agg["score_30d"] += _decay(days_old, 30.0)
                agg["score_90d"] += _decay(days_old, 90.0)
                agg["score_365d"] += _decay(days_old, 365.0)
                agg["recency_score"] += _decay(days_old, 180.0)

    if clear_existing_pm_features:
        repo.clear_rfq_features("PM")

    for key, agg in agg_map.items():
        entity_type, entity_id, region, country, feature_kind, pair, product, tenor = key
        feature_rows.append(
            (
                entity_type,
                entity_id,
                region,
                country,
                feature_kind,
                pair,
                product,
                tenor,
                agg["trade_count"],
                agg["hit_notional_sum_m"],
                agg["last_trade_date"],
                round(agg["score_30d"], 8),
                round(agg["score_90d"], 8),
                round(agg["score_365d"], 8),
                round(agg["recency_score"], 8),
            )
        )
    repo.upsert_rfq_features_bulk(feature_rows)

    for pm_id, snippets in profile_texts.items():
        profile_text = " | ".join(snippets[:50]).strip() or f"PM {pm_id}"
        repo.upsert_entity_profile_cache("PM", pm_id, profile_text)

    conn.close()
    return {
        "rows_read": rows_read,
        "rows_valid": rows_valid,
        "rows_skipped": rows_skipped,
        "pms_created": pms_created,
        "pm_observations_created": obs_created,
        "pm_features_upserted": len(feature_rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest PM CSV into PM entities and feature aggregates.")
    parser.add_argument("--csv", required=True, help="Path to PM CSV.")
    parser.add_argument("--db", default=None, help="Optional SQLite DB path.")
    parser.add_argument("--append", action="store_true", help="Append/update PM features instead of clearing.")
    args = parser.parse_args()

    stats = ingest_pm_csv(Path(args.csv), db_path=args.db, clear_existing_pm_features=(not args.append))
    print("PM ingestion complete.")
    print(
        "Read: {rows_read} | Valid: {rows_valid} | Skipped: {rows_skipped} | PMs created: {pms_created} | "
        "PM obs created: {pm_observations_created} | PM features: {pm_features_upserted}".format(**stats)
    )


if __name__ == "__main__":
    main()

