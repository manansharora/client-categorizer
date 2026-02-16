import csv
from pathlib import Path

from scripts.pm_rfq_coverage import compute_pm_rfq_coverage


def test_compute_pm_rfq_coverage_matches_names_and_pairs(tmp_path: Path) -> None:
    pm_csv = tmp_path / "pm.csv"
    rfq_csv = tmp_path / "rfq.csv"

    pm_rows = [
        {"Portfolio Manager": "Manu Kapoor", "Client": "Brevan"},
        {"Portfolio Manager": "Kevin Chen", "Client": "Capula"},
        {"Portfolio Manager": "Unknown PM", "Client": "Bluecrest"},
    ]
    with pm_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["Portfolio Manager", "Client"])
        writer.writeheader()
        writer.writerows(pm_rows)

    rfq_rows = [
        {"aspen_l2_client_name": "BREVAN", "portfolioManager": "manu kapoor"},
        {"aspen_l2_client_name": "Other Client", "portfolioManager": "Kevin Chen"},
    ]
    with rfq_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["aspen_l2_client_name", "portfolioManager"])
        writer.writeheader()
        writer.writerows(rfq_rows)

    stats = compute_pm_rfq_coverage(pm_csv, rfq_csv, sample_limit=10)
    assert stats["pm_unique_names"] == 3
    assert stats["matched_pm_names"] == 2
    assert stats["pm_unique_client_pairs"] == 3
    assert stats["matched_pm_client_pairs"] == 1
    assert stats["unmatched_pm_names"] == 1
    assert "Unknown PM" in stats["unmatched_pm_name_sample"]
