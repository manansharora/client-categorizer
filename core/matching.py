from collections import Counter, defaultdict

from core.constants import BASE_FAMILY_WEIGHTS, MATCH_WEIGHTS
from core.features import cosine_similarity
from core.text_processing import tokenize


def adjusted_family_weights(client_type: str) -> dict[str, float]:
    weights = dict(BASE_FAMILY_WEIGHTS)

    if client_type.startswith("HF_"):
        weights["INTENT"] += 0.05
        weights["THEME"] += 0.05
        weights["PRODUCT"] -= 0.05
        weights["MARKET"] -= 0.05
    elif client_type.startswith("ASSET_MANAGER_"):
        weights["PRODUCT"] += 0.05
        weights["RISK"] += 0.03
        weights["TENOR"] += 0.02
        weights["INTENT"] -= 0.05
        weights["THEME"] -= 0.05
    elif client_type == "BANK":
        weights["PRODUCT"] += 0.05
        weights["MARKET"] += 0.05
        weights["INTENT"] -= 0.05
        weights["THEME"] -= 0.05

    # Safety clamp and normalization.
    for key in weights:
        weights[key] = max(weights[key], 0.01)
    total = sum(weights.values())
    return {k: v / total for k, v in weights.items()}


def tags_to_family_map(tags: list[dict]) -> dict[str, set[str]]:
    family_map: dict[str, set[str]] = defaultdict(set)
    for row in tags:
        family_map[row["tag_family"]].add(row["tag_code"])
    return family_map


def taxonomy_overlap_score(
    query_family_tags: dict[str, set[str]],
    candidate_family_tags: dict[str, set[str]],
    family_weights: dict[str, float],
) -> float:
    score = 0.0
    for family, weight in family_weights.items():
        q_set = query_family_tags.get(family, set())
        c_set = candidate_family_tags.get(family, set())
        if not q_set and not c_set:
            continue
        union = q_set | c_set
        if not union:
            continue
        overlap = len(q_set & c_set) / len(union)
        score += weight * overlap
    return max(0.0, min(1.0, score))


def top_matching_terms(query_text: str, candidate_text: str, k: int = 5) -> list[str]:
    q_tokens = tokenize(query_text)
    c_tokens = tokenize(candidate_text)
    if not q_tokens or not c_tokens:
        return []
    q_counts = Counter(q_tokens)
    c_counts = Counter(c_tokens)
    intersection = []
    for tok in (set(q_counts.keys()) & set(c_counts.keys())):
        score = q_counts[tok] * c_counts[tok]
        intersection.append((tok, score))
    intersection.sort(key=lambda x: (-x[1], x[0]))
    return [tok for tok, _ in intersection[:k]]


def combine_scores(semantic_score: float, lexical_score: float, taxonomy_score: float) -> float:
    return (
        MATCH_WEIGHTS["semantic"] * semantic_score
        + MATCH_WEIGHTS["lexical"] * lexical_score
        + MATCH_WEIGHTS["taxonomy"] * taxonomy_score
    )


def semantic_scores_from_vectors(query_vec, candidate_vecs) -> list[float]:
    return [cosine_similarity(query_vec, cand_vec) for cand_vec in candidate_vecs]

