from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.database import initialize_database
from core.logging_utils import get_logger
from core.repository import Repository
from core.rfq import (
    infer_client_type_from_sector,
    normalize_ccy_pair,
    normalize_country,
    normalize_product_type,
    normalize_region,
    normalize_tenor_bucket,
    parse_hit_notional_m,
    parse_trade_date,
)


REQUIRED_COLUMNS = [
    "aspen_l2_client_name",
    "CcyPair",
    "Client Region",
    "productType",
    "date",
]
logger = get_logger("ingest_rfq_csv")


@dataclass
class Agg:
    trade_count: int = 0
    hit_notional_sum_m: float = 0.0
    last_trade_date: str = ""
    score_30d: float = 0.0
    score_90d: float = 0.0
    score_365d: float = 0.0
    recency_score: float = 0.0


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


def _ensure_headers(fieldnames: list[str] | None) -> None:
    if not fieldnames:
        raise ValueError("CSV has no header row.")
    lower = {_normalize_key(col) for col in fieldnames}
    missing = [_col for _col in REQUIRED_COLUMNS if _normalize_key(_col) not in lower]
    if missing:
        raise ValueError(f"Missing required RFQ columns: {', '.join(missing)}")


def _decay(days_old: int, window: float) -> float:
    return math.exp(-max(0, days_old) / window)


def _build_feature_keys(
    ccy_pair: str,
    product_type: str,
    tenor_bucket: str,
) -> list[tuple[str, str | None, str | None, str | None]]:
    keys: list[tuple[str, str | None, str | None, str | None]] = []
    if ccy_pair and product_type and tenor_bucket:
        keys.append(("PAIR_PRODUCT_TENOR", ccy_pair, product_type, tenor_bucket))
    if ccy_pair and product_type:
        keys.append(("PAIR_PRODUCT", ccy_pair, product_type, None))
    if ccy_pair:
        keys.append(("PAIR", ccy_pair, None, None))
    if product_type:
        keys.append(("PRODUCT", None, product_type, None))
    if tenor_bucket:
        keys.append(("TENOR", None, None, tenor_bucket))
    return keys


def _build_profile_text(client_name: str, rows: list[tuple[tuple[Any, ...], Agg]]) -> str:
    # Keep profile compact by taking top feature phrases.
    rows = sorted(rows, key=lambda item: (item[1].recency_score, item[1].trade_count), reverse=True)
    parts = [client_name]
    for (kind, pair, product, tenor), agg in rows[:20]:
        phrase = []
        if pair:
            phrase.append(pair)
        if product:
            phrase.append(product)
        if tenor:
            phrase.append(tenor)
        if not phrase:
            continue
        score_repeat = max(1, min(4, int(round(agg.recency_score))))
        parts.extend([" ".join(phrase)] * score_repeat)
    return " | ".join(parts)


def ingest_rfq_csv(
    csv_path: Path,
    db_path: str | None = None,
) -> dict[str, int]:
    conn = initialize_database(db_path)
    repo = Repository(conn)
    run_id = repo.create_rfq_ingest_run(str(csv_path), status="RUNNING")

    rows_read = 0
    rows_valid = 0
    rows_skipped = 0
    clients_created = 0
    pms_created = 0

    client_id_cache: dict[str, int] = {}
    pm_id_cache: dict[tuple[int, str], int] = {}
    aggs: dict[tuple[str, int, str, str, str, str | None, str | None, str | None], Agg] = {}
    profile_buckets: dict[int, list[tuple[tuple[Any, ...], Agg]]] = defaultdict(list)
    pm_profile_names: dict[int, str] = {}

    try:
        logger.info(
            "rfq_ingest_start file=%s mode=additive",
            csv_path,
        )
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            _ensure_headers(reader.fieldnames)

            for row in reader:
                rows_read += 1
                get = _build_row_getter(row)

                client_name = get("aspen_l2_client_name")
                ccy_pair = normalize_ccy_pair(get("CcyPair"))
                region = normalize_region(get("Client Region"))
                country = normalize_country(get("clientregion"))
                product_type = normalize_product_type(get("productType"))
                tenor_bucket = normalize_tenor_bucket(get("tenor_bucket"))
                sector = get("Client Sector")
                pm_name = get("portfolioManager", "Portfolio Manager", "portfolio manager", "PM")
                trade_date = parse_trade_date(get("date"))
                notional_m = parse_hit_notional_m(get("Hit Notional"))

                if not client_name or not ccy_pair or not product_type or not region or trade_date is None:
                    rows_skipped += 1
                    continue

                rows_valid += 1
                client_id = client_id_cache.get(client_name)
                if client_id is None:
                    existing = repo.resolve_client_by_name_or_alias(client_name)
                    if existing:
                        client_id = int(existing["client_id"])
                    else:
                        client_type = infer_client_type_from_sector(sector)
                        client_id = repo.create_client(client_name, client_type, 1)
                        clients_created += 1
                    repo.add_client_alias(client_id, client_name, source="RFQ")
                    client_id_cache[client_name] = client_id

                days_old = max(0, (date.today() - trade_date).days)
                feature_keys = _build_feature_keys(ccy_pair, product_type, tenor_bucket)

                for feature_kind, pair, prod, tenor in feature_keys:
                    agg_key = (
                        "CLIENT",
                        client_id,
                        region,
                        country,
                        feature_kind,
                        pair,
                        prod,
                        tenor,
                    )
                    agg = aggs.get(agg_key)
                    if agg is None:
                        agg = Agg()
                        aggs[agg_key] = agg
                    agg.trade_count += 1
                    agg.hit_notional_sum_m += notional_m
                    trade_date_str = trade_date.isoformat()
                    if not agg.last_trade_date or trade_date_str > agg.last_trade_date:
                        agg.last_trade_date = trade_date_str
                    agg.score_30d += _decay(days_old, 30.0)
                    agg.score_90d += _decay(days_old, 90.0)
                    agg.score_365d += _decay(days_old, 365.0)
                    agg.recency_score += _decay(days_old, 180.0)

                pm_name_clean = (pm_name or "").strip()
                if pm_name_clean:
                    pm_cache_key = (client_id, pm_name_clean.casefold())
                    pm_id = pm_id_cache.get(pm_cache_key)
                    if pm_id is None:
                        existing_pm = repo.get_pm_by_client_name(client_id, pm_name_clean)
                        if existing_pm is not None:
                            pm_id = int(existing_pm["pm_id"])
                        else:
                            pm_id = repo.upsert_pm(client_id=client_id, pm_name=pm_name_clean, active_flag=1)
                            pms_created += 1
                        pm_id_cache[pm_cache_key] = pm_id
                        pm_profile_names[pm_id] = pm_name_clean
                        repo.upsert_pm_metadata(pm_id=pm_id, source_sheet=csv_path.name)

                    for feature_kind, pair, prod, tenor in feature_keys:
                        pm_agg_key = (
                            "PM",
                            pm_id,
                            region,
                            country,
                            feature_kind,
                            pair,
                            prod,
                            tenor,
                        )
                        pm_agg = aggs.get(pm_agg_key)
                        if pm_agg is None:
                            pm_agg = Agg()
                            aggs[pm_agg_key] = pm_agg
                        pm_agg.trade_count += 1
                        pm_agg.hit_notional_sum_m += notional_m
                        trade_date_str = trade_date.isoformat()
                        if not pm_agg.last_trade_date or trade_date_str > pm_agg.last_trade_date:
                            pm_agg.last_trade_date = trade_date_str
                        pm_agg.score_30d += _decay(days_old, 30.0)
                        pm_agg.score_90d += _decay(days_old, 90.0)
                        pm_agg.score_365d += _decay(days_old, 365.0)
                        pm_agg.recency_score += _decay(days_old, 180.0)

        upsert_rows: list[tuple[Any, ...]] = []
        for key, agg in aggs.items():
            entity_type, entity_id, region, country, feature_kind, pair, prod, tenor = key
            upsert_rows.append(
                (
                    entity_type,
                    entity_id,
                    region,
                    country,
                    feature_kind,
                    pair,
                    prod,
                    tenor,
                    agg.trade_count,
                    round(agg.hit_notional_sum_m, 6),
                    agg.last_trade_date,
                    round(agg.score_30d, 8),
                    round(agg.score_90d, 8),
                    round(agg.score_365d, 8),
                    round(agg.recency_score, 8),
                )
            )
            profile_buckets[entity_id].append(((feature_kind, pair, prod, tenor), agg))
        repo.upsert_rfq_features_bulk(upsert_rows, additive=True)

        for client_name, client_id in client_id_cache.items():
            profile_text = _build_profile_text(client_name, profile_buckets.get(client_id, []))
            repo.upsert_entity_profile_cache("CLIENT", client_id, profile_text)
        for pm_id, pm_name in pm_profile_names.items():
            profile_text = _build_profile_text(pm_name, profile_buckets.get(pm_id, []))
            repo.upsert_entity_profile_cache("PM", pm_id, profile_text)

        repo.update_rfq_ingest_run(
            run_id=run_id,
            status="SUCCESS",
            rows_read=rows_read,
            rows_valid=rows_valid,
            rows_skipped=rows_skipped,
            error_summary=None,
        )
        conn.close()
        logger.info(
            "rfq_ingest_done run_id=%s read=%s valid=%s skipped=%s clients_created=%s features=%s",
            run_id,
            rows_read,
            rows_valid,
            rows_skipped,
            clients_created,
            len(upsert_rows),
        )
        return {
            "run_id": run_id,
            "rows_read": rows_read,
            "rows_valid": rows_valid,
            "rows_skipped": rows_skipped,
            "clients_created": clients_created,
            "pms_created": pms_created,
            "features_upserted": len(upsert_rows),
        }
    except Exception as exc:
        logger.exception("rfq_ingest_failed run_id=%s file=%s", run_id, csv_path)
        repo.update_rfq_ingest_run(
            run_id=run_id,
            status="FAILED",
            rows_read=rows_read,
            rows_valid=rows_valid,
            rows_skipped=rows_skipped,
            error_summary=str(exc),
        )
        conn.close()
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest RFQ CSV into aggregate feature tables.")
    parser.add_argument("--csv", required=True, help="Path to RFQ CSV file.")
    parser.add_argument("--db", default=None, help="Optional SQLite DB path.")
    args = parser.parse_args()

    stats = ingest_rfq_csv(
        csv_path=Path(args.csv),
        db_path=args.db,
    )
    print("RFQ ingestion complete.")
    print(
        "Run: {run_id} | Read: {rows_read} | Valid: {rows_valid} | Skipped: {rows_skipped} | "
        "Clients created: {clients_created} | PMs created: {pms_created} | Features upserted: {features_upserted}".format(**stats)
    )


if __name__ == "__main__":
    main()
