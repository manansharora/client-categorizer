from core.matching import (
    adjusted_family_weights,
    combine_scores,
    taxonomy_overlap_score,
)


def test_combine_scores_uses_expected_weights() -> None:
    score = combine_scores(semantic_score=1.0, lexical_score=0.0, taxonomy_score=0.0)
    assert abs(score - 0.45) < 1e-9


def test_taxonomy_overlap_returns_high_when_sets_match() -> None:
    weights = adjusted_family_weights("BANK")
    query = {"PRODUCT": {"FX_VANILLA_OPTION"}, "INTENT": {"HEDGING"}}
    candidate = {"PRODUCT": {"FX_VANILLA_OPTION"}, "INTENT": {"HEDGING"}}
    score = taxonomy_overlap_score(query, candidate, weights)
    assert score > 0.4


def test_adjusted_weights_are_normalized() -> None:
    weights = adjusted_family_weights("HF_MACRO")
    total = sum(weights.values())
    assert abs(total - 1.0) < 1e-9
    assert weights["INTENT"] > 0
    assert weights["THEME"] > 0

