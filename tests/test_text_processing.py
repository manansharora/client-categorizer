from core.seed_data import DEFAULT_SYNONYMS
from core.text_processing import build_synonym_map, normalize_text


def test_normalize_text_applies_domain_synonyms() -> None:
    rows = [
        {"surface_form": s, "canonical_form": c, "tag_code_optional": t}
        for s, c, t in DEFAULT_SYNONYMS
    ]
    synonym_map = build_synonym_map(rows)
    raw = "Client likes KI structures in 3m NDF around cb meetings"
    normalized = normalize_text(raw, synonym_map)
    assert "knock" in normalized
    assert "3m" in normalized
    assert "central" in normalized


def test_normalize_text_removes_generic_noise() -> None:
    normalized = normalize_text("The client and the PM are in discussion for this idea", {})
    assert "client" not in normalized
    assert "pm" not in normalized
    assert "discussion" in normalized

