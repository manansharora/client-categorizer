import hashlib
import os
from pathlib import Path
from collections.abc import Iterable

import numpy as np
from rank_bm25 import BM25Okapi

from core.constants import DEFAULT_FASTTEXT_MODEL_PATH
from core.text_processing import tokenize

try:
    from gensim.models import FastText
except Exception:  # pragma: no cover
    FastText = None


def min_max_scale(values: list[float]) -> list[float]:
    if not values:
        return []
    min_v = min(values)
    max_v = max(values)
    if max_v - min_v < 1e-9:
        return [0.0 if max_v <= 0 else 1.0 for _ in values]
    return [(v - min_v) / (max_v - min_v) for v in values]


def bm25_score_documents(query_text: str, documents: list[str]) -> list[float]:
    tokenized_docs = [tokenize(d) for d in documents]
    if not tokenized_docs or not any(tokenized_docs):
        return [0.0 for _ in documents]
    bm25 = BM25Okapi(tokenized_docs)
    query_tokens = tokenize(query_text)
    if not query_tokens:
        return [0.0 for _ in documents]
    scores = bm25.get_scores(query_tokens)
    return min_max_scale(scores.tolist())


def cosine_similarity(vec_a: np.ndarray, vec_b: np.ndarray) -> float:
    denom = float(np.linalg.norm(vec_a) * np.linalg.norm(vec_b))
    if denom <= 1e-12:
        return 0.0
    cos = float(np.dot(vec_a, vec_b) / denom)
    # Map [-1, 1] to [0, 1].
    return max(0.0, min(1.0, (cos + 1.0) / 2.0))


class FastTextEmbedder:
    def __init__(
        self,
        vector_size: int = 100,
        window: int = 5,
        min_count: int = 1,
        epochs: int = 25,
        fallback_dim: int = 128,
        enable_training: bool | None = None,
        model_path: str | None = None,
    ):
        self.vector_size = vector_size
        self.window = window
        self.min_count = min_count
        self.epochs = epochs
        self.fallback_dim = fallback_dim
        if enable_training is None:
            env_value = os.getenv("CLIENT_CATEGORIZER_FASTTEXT_TRAIN", "").strip().lower()
            enable_training = env_value in {"1", "true", "yes"}
        self.enable_training = bool(enable_training)
        if model_path is None:
            model_path = os.getenv("CLIENT_CATEGORIZER_FASTTEXT_MODEL_PATH", str(DEFAULT_FASTTEXT_MODEL_PATH))
        self.model_path = Path(model_path) if model_path else None
        self.model = None
        self._load_model_if_present()

    def _load_model_if_present(self) -> None:
        if self.model_path is None or not self.model_path.exists() or FastText is None:
            return
        try:
            self.model = FastText.load(str(self.model_path))
        except Exception:
            self.model = None

    def fit(self, texts: Iterable[str]) -> None:
        if not self.enable_training:
            return
        tokenized = [tokenize(text) for text in texts if text and tokenize(text)]
        if FastText is not None and len(tokenized) >= 2:
            model = FastText(
                vector_size=self.vector_size,
                window=self.window,
                min_count=self.min_count,
                workers=1,
                sg=1,
            )
            model.build_vocab(corpus_iterable=tokenized)
            model.train(corpus_iterable=tokenized, total_examples=len(tokenized), epochs=self.epochs)
            self.model = model
            if self.model_path is not None:
                self.model_path.parent.mkdir(parents=True, exist_ok=True)
                model.save(str(self.model_path))
        else:
            self.model = None

    def _fallback_encode(self, text: str) -> np.ndarray:
        vec = np.zeros(self.fallback_dim, dtype=np.float32)
        tokens = tokenize(text)
        for tok in tokens:
            tok = f"^{tok}$"
            if len(tok) < 3:
                chunks = [tok]
            else:
                chunks = [tok[i : i + 3] for i in range(0, len(tok) - 2)]
            for chunk in chunks:
                idx = int(hashlib.md5(chunk.encode("utf-8")).hexdigest(), 16) % self.fallback_dim
                vec[idx] += 1.0
        norm = np.linalg.norm(vec)
        if norm > 1e-12:
            vec = vec / norm
        return vec

    def encode(self, text: str) -> np.ndarray:
        if self.model is None:
            return self._fallback_encode(text)
        tokens = tokenize(text)
        if not tokens:
            return np.zeros(self.vector_size, dtype=np.float32)
        vectors = [self.model.wv[tok] for tok in tokens if tok in self.model.wv]
        if not vectors:
            return np.zeros(self.vector_size, dtype=np.float32)
        return np.mean(np.array(vectors), axis=0)

