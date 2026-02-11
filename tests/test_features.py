from core.features import FastTextEmbedder


def test_fasttext_embedder_defaults_to_no_training() -> None:
    embedder = FastTextEmbedder()
    embedder.fit(["a short sentence", "another short sentence"])
    assert embedder.model is None

