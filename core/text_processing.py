import re
from collections.abc import Iterable


TOKEN_REGEX = re.compile(r"[a-z0-9_+\-/]+")
SPACE_REGEX = re.compile(r"\s+")

# Keep domain-important short tokens like FX, KI, KO, RR and tenor tokens.
STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "to",
    "for",
    "of",
    "on",
    "in",
    "with",
    "at",
    "by",
    "from",
    "is",
    "are",
    "be",
    "that",
    "this",
    "it",
    "as",
    "we",
    "they",
    "client",
    "pm",
    "desk",
}


def normalize_whitespace(text: str) -> str:
    return SPACE_REGEX.sub(" ", text).strip()


def build_synonym_map(synonym_rows: Iterable[dict]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in synonym_rows:
        surface = str(row["surface_form"]).strip().lower()
        canonical = str(row["canonical_form"]).strip().lower()
        if surface:
            mapping[surface] = canonical
    return mapping


def apply_phrase_replacements(text: str, synonym_map: dict[str, str]) -> str:
    replaced = f" {text.lower()} "
    # Replace longer phrases first to avoid partial overlaps.
    for surface in sorted(synonym_map, key=len, reverse=True):
        canonical = synonym_map[surface]
        pattern = re.compile(rf"(?<!\w){re.escape(surface)}(?!\w)")
        replaced = pattern.sub(canonical, replaced)
    return normalize_whitespace(replaced)


def tokenize(text: str) -> list[str]:
    return TOKEN_REGEX.findall(text.lower())


def is_tenor_token(token: str) -> bool:
    return bool(re.fullmatch(r"\d+[dwmy]", token))


def normalize_text(text: str, synonym_map: dict[str, str] | None = None) -> str:
    if not text:
        return ""
    synonym_map = synonym_map or {}
    replaced = apply_phrase_replacements(text, synonym_map)
    tokens: list[str] = []
    for tok in tokenize(replaced):
        if tok in STOPWORDS and not is_tenor_token(tok):
            continue
        if len(tok) == 1 and not tok.isdigit():
            continue
        tokens.append(tok)
    return " ".join(tokens)

