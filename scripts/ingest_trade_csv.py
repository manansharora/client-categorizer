from __future__ import annotations

import argparse
import ast
import csv
from datetime import date
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.constants import CLIENT_TYPES
from core.database import initialize_database
from core.repository import Repository
from core.service import ClientCategorizerService


def _normalize_key(value: str) -> str:
    return value.strip().lower()


def _build_row_getter(row: dict[str, str]):
    keyed = {_normalize_key(k): (v or "") for k, v in row.items()}

    def get_field(name: str) -> str:
        return keyed.get(_normalize_key(name), "").strip()

    return get_field


def parse_client_list(raw_value: str) -> list[str]:
    value = (raw_value or "").strip()
    if not value:
        return []

    parsed: list[str] | None = None
    try:
        obj = ast.literal_eval(value)
        if isinstance(obj, list):
            parsed = [str(item).strip() for item in obj if str(item).strip()]
        elif isinstance(obj, str) and obj.strip():
            parsed = [obj.strip()]
    except Exception:
        parsed = None

    if parsed is None:
        cleaned = value.strip("[]")
        parts = [p.strip().strip('"').strip("'") for p in cleaned.split(",")]
        parsed = [p for p in parts if p]

    deduped: list[str] = []
    seen: set[str] = set()
    for name in parsed:
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(name)
    return deduped


def build_idea_text(trade: str, sales_feedback: str) -> str:
    trade = trade.strip()
    sales_feedback = sales_feedback.strip()
    if sales_feedback:
        return f"Trade: {trade}\n\nSales Feedback: {sales_feedback}"
    return f"Trade: {trade}"


def build_observation_text(trade: str, sales_feedback: str, created_by: str) -> str:
    parts = [f"Trade: {trade.strip()}"]
    if sales_feedback.strip():
        parts.append(f"Sales Feedback: {sales_feedback.strip()}")
    if created_by.strip():
        parts.append(f"Created By: {created_by.strip()}")
    return "\n".join(parts)


def ingest_trade_csv(
    csv_path: Path,
    db_path: str | None = None,
    default_client_type: str = "BANK",
    observation_type: str = "CALL_NOTE",
) -> dict[str, int]:
    if default_client_type not in CLIENT_TYPES:
        raise ValueError(f"Invalid default client type: {default_client_type}")

    conn = initialize_database(db_path)
    repo = Repository(conn)
    service = ClientCategorizerService(repo)

    counters = {
        "rows_processed": 0,
        "ideas_created": 0,
        "ideas_updated": 0,
        "clients_created": 0,
        "observations_created": 0,
    }
    touched_clients: set[int] = set()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row.")

        for row in reader:
            get = _build_row_getter(row)
            trade = get("Trade")
            sales_feedback = get("Sales Feedback")
            created_by = get("Created By")
            clients_raw = get("Client")

            if not trade:
                continue

            counters["rows_processed"] += 1
            idea_title = trade
            idea_text = build_idea_text(trade, sales_feedback)

            existing_idea = repo.get_idea_by_title(idea_title)
            if existing_idea:
                idea_id = int(existing_idea["idea_id"])
                repo.update_idea(idea_id, idea_title, idea_text, created_by or None)
                counters["ideas_updated"] += 1
            else:
                idea_id = repo.create_idea(idea_title, idea_text, created_by or None)
                counters["ideas_created"] += 1
            service.auto_tag_idea(idea_id, idea_text)

            client_names = parse_client_list(clients_raw)
            observation_text = build_observation_text(trade, sales_feedback, created_by)

            for client_name in client_names:
                existing_client = repo.resolve_client_by_name_or_alias(client_name)
                if existing_client:
                    client_id = int(existing_client["client_id"])
                    repo.add_client_alias(client_id, client_name, source="TRADE_CSV")
                else:
                    client_id = repo.create_client(client_name, default_client_type, 1)
                    repo.set_manual_tags("CLIENT", client_id, [default_client_type])
                    repo.add_client_alias(client_id, client_name, source="TRADE_CSV")
                    counters["clients_created"] += 1

                touched_clients.add(client_id)
                existing_obs = {
                    (str(obs["obs_type"]), str(obs["obs_text"]).strip())
                    for obs in repo.list_observations(client_id)
                }
                obs_key = (observation_type, observation_text.strip())
                if obs_key in existing_obs:
                    continue

                obs_id = repo.add_observation(
                    client_id=client_id,
                    obs_type=observation_type,
                    obs_text=observation_text,
                    obs_date=date.today(),
                    source_confidence=0.8,
                )
                service.auto_tag_observation(obs_id, observation_text)
                counters["observations_created"] += 1

    for client_id in touched_clients:
        service.refresh_client_tags_from_observations(client_id)

    conn.close()
    return counters


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest CSV rows into ideas and client observations. "
            "Required columns: Trade, Sales Feedback, Client, Created By"
        )
    )
    parser.add_argument("--csv", required=True, help="Path to the source CSV file.")
    parser.add_argument("--db", default=None, help="Optional path to SQLite DB.")
    parser.add_argument(
        "--default-client-type",
        default="BANK",
        help=f"Client type for newly created clients. Default: BANK. Allowed: {', '.join(CLIENT_TYPES)}",
    )
    parser.add_argument(
        "--observation-type",
        default="CALL_NOTE",
        help="Observation type to write for each trade-client pair. Default: CALL_NOTE",
    )
    args = parser.parse_args()

    stats = ingest_trade_csv(
        csv_path=Path(args.csv),
        db_path=args.db,
        default_client_type=args.default_client_type,
        observation_type=args.observation_type,
    )
    print("CSV ingestion complete.")
    print(
        "Rows processed: {rows_processed} | Ideas created: {ideas_created} | Ideas updated: {ideas_updated} "
        "| Clients created: {clients_created} | Observations created: {observations_created}".format(**stats)
    )


if __name__ == "__main__":
    main()
