from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.constants import DEFAULT_FASTTEXT_MODEL_PATH
from core.database import initialize_database
from core.features import FastTextEmbedder
from core.logging_utils import get_logger
from core.repository import Repository
from core.service import ClientCategorizerService

logger = get_logger("retrain_semantic_model")


def retrain_semantic_model(db_path: str | None, model_path: str | None, epochs: int) -> dict[str, int]:
    conn = initialize_database(db_path)
    repo = Repository(conn)
    service = ClientCategorizerService(repo)

    texts: list[str] = []

    client_cache = repo.conn.execute(
        "SELECT profile_text FROM entity_profile_cache WHERE entity_type = 'CLIENT'"
    ).fetchall()
    pm_cache = repo.conn.execute(
        "SELECT profile_text FROM entity_profile_cache WHERE entity_type = 'PM'"
    ).fetchall()
    ideas = repo.list_ideas()
    obs = repo.list_observations()
    pm_obs = repo.list_pm_observations()

    for row in client_cache:
        texts.append(service.normalize_text(str(row["profile_text"])))
    for row in pm_cache:
        texts.append(service.normalize_text(str(row["profile_text"])))
    for row in ideas:
        texts.append(service.normalize_text(str(row["idea_text"])))
    for row in obs:
        texts.append(service.normalize_text(str(row["obs_text"])))
    for row in pm_obs:
        texts.append(service.normalize_text(str(row["obs_text"])))

    corpus = [t for t in texts if t.strip()]
    embedder = FastTextEmbedder(
        enable_training=True,
        model_path=model_path or str(DEFAULT_FASTTEXT_MODEL_PATH),
        epochs=epochs,
    )
    embedder.fit(corpus)
    conn.close()
    return {"corpus_size": len(corpus)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Train or refresh persistent FastText model from DB corpus.")
    parser.add_argument("--db", default=None, help="Optional SQLite DB path.")
    parser.add_argument(
        "--model-path",
        default=str(DEFAULT_FASTTEXT_MODEL_PATH),
        help="Output model path. Defaults to data/fasttext.model",
    )
    parser.add_argument("--epochs", type=int, default=25, help="FastText training epochs.")
    args = parser.parse_args()

    stats = retrain_semantic_model(args.db, args.model_path, args.epochs)
    print(f"Semantic model retrained. Corpus size: {stats['corpus_size']}. Model: {args.model_path}")
    logger.info("semantic_retrain_done corpus_size=%s model_path=%s epochs=%s", stats["corpus_size"], args.model_path, args.epochs)


if __name__ == "__main__":
    main()

