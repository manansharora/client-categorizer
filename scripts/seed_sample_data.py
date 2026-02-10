from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.database import initialize_database
from core.repository import Repository
from core.service import ClientCategorizerService


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return date.today()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed short sample clients/ideas into the SQLite DB.")
    parser.add_argument("--db", default=None, help="Optional path to SQLite DB.")
    parser.add_argument(
        "--data",
        default=str(ROOT / "data" / "sample_data.json"),
        help="Path to sample data JSON file.",
    )
    args = parser.parse_args()

    conn = initialize_database(args.db)
    repo = Repository(conn)
    service = ClientCategorizerService(repo)

    data_path = Path(args.data)
    payload = json.loads(data_path.read_text(encoding="utf-8"))

    for client in payload.get("clients", []):
        name = client["client_name"]
        client_type = client["client_type"]

        existing = repo.get_client_by_name(name)
        if existing:
            client_id = int(existing["client_id"])
            repo.update_client(client_id, client_type, int(existing["active_flag"]))
        else:
            client_id = repo.create_client(name, client_type, 1)
            repo.set_manual_tags("CLIENT", client_id, [client_type])

        for obs in client.get("observations", []):
            obs_type = obs["obs_type"]
            obs_text = obs["obs_text"]
            obs_date = _parse_date(obs.get("obs_date", ""))
            confidence = float(obs.get("source_confidence", 0.8))

            if repo.observation_exists(client_id, obs_type, obs_text, obs_date.isoformat()):
                continue

            obs_id = repo.add_observation(client_id, obs_type, obs_text, obs_date, confidence)
            service.auto_tag_observation(obs_id, obs_text)

        service.refresh_client_tags_from_observations(client_id)

    for idea in payload.get("ideas", []):
        title = idea["idea_title"]
        text = idea["idea_text"]
        created_by = idea.get("created_by")

        existing = repo.get_idea_by_title(title)
        if existing:
            idea_id = int(existing["idea_id"])
            repo.update_idea(idea_id, title, text, created_by)
        else:
            idea_id = repo.create_idea(title, text, created_by)
        service.auto_tag_idea(idea_id, text)

    conn.close()
    print("Seeded sample clients, observations, and ideas (idempotent).")


if __name__ == "__main__":
    main()

