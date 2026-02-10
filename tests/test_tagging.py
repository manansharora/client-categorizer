from core.seed_data import DEFAULT_SYNONYMS, TAXONOMY_TAGS
from core.tagging import extract_tags
from core.text_processing import build_synonym_map


def _taxonomy_rows() -> list[dict]:
    return [
        {"tag_family": family, "tag_code": code, "tag_label": label}
        for family, code, label in TAXONOMY_TAGS
    ]


def _synonym_rows() -> list[dict]:
    return [
        {"surface_form": s, "canonical_form": c, "tag_code_optional": t}
        for s, c, t in DEFAULT_SYNONYMS
    ]


def test_extract_tags_for_fx_option_language() -> None:
    taxonomy_rows = _taxonomy_rows()
    synonym_rows = _synonym_rows()
    synonym_map = build_synonym_map(synonym_rows)
    text = "3m KO and KI idea with RR in G10 after central bank event"
    tags = extract_tags(text, taxonomy_rows, synonym_rows, synonym_map)
    tag_codes = {code for code, _ in tags}
    assert "KNOCK_IN" in tag_codes
    assert "KNOCK_OUT" in tag_codes
    assert "FX_VOL_STRUCTURE" in tag_codes
    assert "G10_FX" in tag_codes
    assert "CENTRAL_BANK" in tag_codes

