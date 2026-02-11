from core.rfq import extract_structured_signals_from_text


def test_extracts_multiple_pairs_and_digital_products() -> None:
    text = "FX Trade Idea: AUDJPY and USDJPY dual digital (DIG). Correlation is high."
    signals = extract_structured_signals_from_text(text)
    assert "AUDJPY" in signals["ccy_pairs"]
    assert "USDJPY" in signals["ccy_pairs"]
    assert "DIG" in signals["product_types"]
    assert "DIGKNO" in signals["product_types"]


def test_extracts_non_digital_product_keywords() -> None:
    text = "Client may like knockout basket forward structures in EURUSD."
    signals = extract_structured_signals_from_text(text)
    assert "EURUSD" in signals["ccy_pairs"]
    assert any(p in signals["product_types"] for p in ["KNO", "BASKET", "FWDSTRUCT"])

