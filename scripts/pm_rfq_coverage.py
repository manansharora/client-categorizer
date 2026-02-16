from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any


def _normalize_key(value: str) -> str:
    return value.strip().lower()


def _normalize_token(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", (value or "").casefold())
    return " ".join(cleaned.split())


def _build_row_getter(row: dict[str, str]):
    keyed = {_normalize_key(k): (v or "") for k, v in row.items()}

    def get_field(*names: str) -> str:
        for name in names:
            value = keyed.get(_normalize_key(name), "")
            if value.strip():
                return value.strip()
        return ""

    return get_field


def compute_pm_rfq_coverage(pm_csv_path: Path, rfq_csv_path: Path, sample_limit: int = 20) -> dict[str, Any]:
    pm_rows_read = 0
    pm_rows_valid = 0
    rfq_rows_read = 0
    rfq_rows_with_pm = 0

    pm_names: set[str] = set()
    pm_pairs: set[tuple[str, str]] = set()
    pm_original_names: dict[str, str] = {}
    rfq_names: set[str] = set()
    rfq_pairs: set[tuple[str, str]] = set()

    with pm_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("PM CSV has no header row.")
        for row in reader:
            pm_rows_read += 1
            get = _build_row_getter(row)
            pm_name_raw = get("Portfolio Manager", "PM")
            client_raw = get("Client", "Client Name", "aspen_l2_client_name")
            pm_name = _normalize_token(pm_name_raw)
            client_name = _normalize_token(client_raw)
            if not pm_name:
                continue
            pm_rows_valid += 1
            pm_names.add(pm_name)
            pm_original_names.setdefault(pm_name, pm_name_raw.strip())
            if client_name:
                pm_pairs.add((pm_name, client_name))

    with rfq_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("RFQ CSV has no header row.")
        for row in reader:
            rfq_rows_read += 1
            get = _build_row_getter(row)
            pm_name = _normalize_token(get("portfolioManager", "Portfolio Manager", "portfolio manager", "PM"))
            if not pm_name:
                continue
            rfq_rows_with_pm += 1
            client_name = _normalize_token(get("aspen_l2_client_name", "Client", "Client Name"))
            rfq_names.add(pm_name)
            if client_name:
                rfq_pairs.add((pm_name, client_name))

    matched_names = pm_names & rfq_names
    matched_pairs = pm_pairs & rfq_pairs
    unmatched_names = sorted(pm_names - rfq_names)

    pm_unique_names = len(pm_names)
    pm_unique_pairs = len(pm_pairs)
    matched_name_count = len(matched_names)
    matched_pair_count = len(matched_pairs)

    return {
        "pm_csv_path": str(pm_csv_path),
        "rfq_csv_path": str(rfq_csv_path),
        "pm_rows_read": pm_rows_read,
        "pm_rows_valid": pm_rows_valid,
        "rfq_rows_read": rfq_rows_read,
        "rfq_rows_with_pm": rfq_rows_with_pm,
        "pm_unique_names": pm_unique_names,
        "pm_unique_client_pairs": pm_unique_pairs,
        "rfq_unique_names": len(rfq_names),
        "rfq_unique_client_pairs": len(rfq_pairs),
        "matched_pm_names": matched_name_count,
        "matched_pm_names_pct": round((matched_name_count / pm_unique_names) if pm_unique_names else 0.0, 6),
        "matched_pm_client_pairs": matched_pair_count,
        "matched_pm_client_pairs_pct": round((matched_pair_count / pm_unique_pairs) if pm_unique_pairs else 0.0, 6),
        "unmatched_pm_names": len(unmatched_names),
        "unmatched_pm_name_sample": [pm_original_names.get(name, name) for name in unmatched_names[:sample_limit]],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report PM coverage: how many PMs in PM CSV have any associated RFQ rows in RFQ CSV."
    )
    parser.add_argument("--pm-csv", required=True, help="Path to PM CSV file.")
    parser.add_argument("--rfq-csv", required=True, help="Path to RFQ CSV file.")
    parser.add_argument("--sample-limit", type=int, default=20, help="Maximum unmatched PM names to print.")
    parser.add_argument("--json", action="store_true", help="Print full result as JSON.")
    args = parser.parse_args()

    stats = compute_pm_rfq_coverage(
        pm_csv_path=Path(args.pm_csv),
        rfq_csv_path=Path(args.rfq_csv),
        sample_limit=max(1, args.sample_limit),
    )
    if args.json:
        print(json.dumps(stats, indent=2))
        return

    print("PM/RFQ coverage complete.")
    print(f"PM unique names: {stats['pm_unique_names']}")
    print(f"Matched PM names: {stats['matched_pm_names']} ({stats['matched_pm_names_pct']:.1%})")
    print(f"PM unique PM+Client pairs: {stats['pm_unique_client_pairs']}")
    print(f"Matched PM+Client pairs: {stats['matched_pm_client_pairs']} ({stats['matched_pm_client_pairs_pct']:.1%})")
    print(f"Unmatched PM names: {stats['unmatched_pm_names']}")
    if stats["unmatched_pm_name_sample"]:
        print("Unmatched PM sample:")
        for name in stats["unmatched_pm_name_sample"]:
            print(f"- {name}")


if __name__ == "__main__":
    main()
