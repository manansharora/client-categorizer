import re
from collections import defaultdict
from collections.abc import Iterable

from core.text_processing import normalize_text


def build_tag_term_map(
    taxonomy_rows: Iterable[dict], synonym_rows: Iterable[dict]
) -> dict[str, set[str]]:
    tag_terms: dict[str, set[str]] = defaultdict(set)

    for row in taxonomy_rows:
        code = row["tag_code"]
        label = str(row["tag_label"]).lower().strip()
        tag_terms[code].add(label)
        tag_terms[code].add(code.lower().replace("_", " "))
        # Add compact forms for common patterns.
        tag_terms[code].add(code.lower())

    for row in synonym_rows:
        tag_code = row["tag_code_optional"]
        if tag_code:
            tag_terms[tag_code].add(str(row["surface_form"]).lower().strip())
            tag_terms[tag_code].add(str(row["canonical_form"]).lower().strip())

    return tag_terms


def _count_term_matches(text: str, terms: set[str]) -> tuple[int, int]:
    match_count = 0
    max_term_tokens = 1
    for term in terms:
        if not term:
            continue
        pattern = re.compile(rf"(?<!\w){re.escape(term)}(?!\w)")
        if pattern.search(text):
            match_count += 1
            max_term_tokens = max(max_term_tokens, len(term.split()))
    return match_count, max_term_tokens


def extract_tags(
    text: str,
    taxonomy_rows: list[dict],
    synonym_rows: list[dict],
    synonym_map: dict[str, str],
) -> list[tuple[str, float]]:
    normalized = normalize_text(text, synonym_map)
    if not normalized:
        return []

    tag_terms = build_tag_term_map(taxonomy_rows, synonym_rows)
    extracted: list[tuple[str, float]] = []

    for row in taxonomy_rows:
        tag_code = row["tag_code"]
        tag_family = str(row.get("tag_family", "")).upper()
        if tag_family == "CLIENT_TYPE":
            continue
        terms = tag_terms.get(tag_code, set())
        count, max_term_tokens = _count_term_matches(normalized, terms)
        if count <= 0:
            continue
        confidence = min(1.0, 0.35 + 0.20 * count + 0.05 * (max_term_tokens - 1))
        extracted.append((tag_code, round(confidence, 4)))

    extracted.sort(key=lambda x: (-x[1], x[0]))
    return extracted
