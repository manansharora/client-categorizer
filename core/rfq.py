from __future__ import annotations

import re
from datetime import date, datetime

from core.constants import CLIENT_TYPES, REGIONS


PRODUCT_TYPES = {
    "EUR",
    "KNO",
    "NDO",
    "EKIKO",
    "EKI",
    "DIG",
    "FWDACC",
    "NT",
    "VKO",
    "RKO",
    "TPF",
    "PTPF",
    "DOT",
    "OT",
    "VOLSWAP",
    "WRKI",
    "RKI",
    "MBAR",
    "DKO",
    "STRUCTSWP",
    "CORRSWP",
    "EKO",
    "MDIG",
    "DKI",
    "DIGRKO",
    "WRKO",
    "WKNO",
    "DCD",
    "DNT",
    "AVGSTRIKE",
    "KNI",
    "GENACCRUAL",
    "WDIGKNO",
    "COMMODITYFORWARD",
    "KIKO",
    "DIGKNO",
    "RFADER",
    "FWDSTRUCT",
    "WDIGRKO",
    "FVA",
    "BOWO",
    "COMMODITYSWAP",
    "KOFVA",
    "WDKO",
    "VARSWAP",
    "DIGDKO",
    "AVGRATE",
    "COMMODITYFUTURE",
    "FXFUTOPT",
    "WDIGDKO",
    "WKNI",
    "WDKI",
    "AMER",
    "BASKET",
    "COMPOUND",
    "RTIMER",
    "AVGRATE_FWD",
    "WDNT",
}


REGION_MAP = {
    "EUROPE": "EUROPE",
    "APAC": "APAC",
    "AMERICA": "AMERICA",
    "AMERICAS": "AMERICA",
    "CEEMA": "CEEMEA",
    "CEEMEA": "CEEMEA",
}


TENOR_BUCKET_MAP = {
    "<1W": "<1W",
    "1W": "1W",
    "2W-1M": "2W-1M",
    "1M-3M": "1M-3M",
    "3M-6M": "3M-6M",
    "6M-1Y": "6M-1Y",
    ">1Y": ">1Y",
}

PRODUCT_KEYWORD_MAP = {
    "DUAL DIGITAL": ["DIG", "DIGKNO", "DIGRKO", "DIGDKO", "MDIG", "WDIGKNO", "WDIGRKO", "WDIGDKO"],
    "DIGITAL": ["DIG", "DIGKNO", "DIGRKO", "DIGDKO", "MDIG", "WDIGKNO", "WDIGRKO", "WDIGDKO"],
    "DIGITALS": ["DIG", "DIGKNO", "DIGRKO", "DIGDKO", "MDIG", "WDIGKNO", "WDIGRKO", "WDIGDKO"],
    "KNOCK OUT": ["KNO", "DKO", "RKO", "WRKO", "WDKO", "WKNO", "DIGKNO", "DIGRKO", "DIGDKO"],
    "KNOCKOUT": ["KNO", "DKO", "RKO", "WRKO", "WDKO", "WKNO", "DIGKNO", "DIGRKO", "DIGDKO"],
    "KIKO": ["KIKO", "EKIKO", "EKI"],
    "KNOCK IN": ["KNI", "DKI", "RKI", "WRKI", "WDKI", "WKNI"],
    "KNOCKIN": ["KNI", "DKI", "RKI", "WRKI", "WDKI", "WKNI"],
    "VOL SWAP": ["VOLSWAP", "VARSWAP", "CORRSWP"],
    "VAR SWAP": ["VARSWAP", "VOLSWAP"],
    "BASKET": ["BASKET"],
    "COMPOUND": ["COMPOUND"],
    "FORWARD": ["FWDSTRUCT", "FWDACC", "AVGRATE_FWD", "COMMODITYFORWARD"],
}


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        token = value.strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def normalize_region(value: str) -> str:
    region = (value or "").strip().upper()
    return REGION_MAP.get(region, region if region in REGIONS else "")


def normalize_country(value: str) -> str:
    return (value or "").strip().upper()


def normalize_ccy_pair(value: str) -> str:
    raw = re.sub(r"[^A-Za-z]", "", (value or "").upper())
    if len(raw) >= 6:
        return raw[:6]
    return ""


def normalize_product_type(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]", "", (value or "").upper())
    return token


def normalize_tenor_bucket(value: str) -> str:
    token = (value or "").strip().upper()
    return TENOR_BUCKET_MAP.get(token, token)


def parse_trade_date(value: str) -> date | None:
    text = (value or "").strip()
    if not text:
        return None
    formats = [
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_hit_notional_m(value: str) -> float:
    text = (value or "").strip().upper().replace(",", "")
    if not text:
        return 0.0
    if text.endswith("M"):
        text = text[:-1]
    try:
        return float(text)
    except ValueError:
        return 0.0


def infer_client_type_from_sector(sector: str) -> str:
    token = (sector or "").strip().upper()
    if "HF" in token:
        return "HF_MACRO"
    if "REAL MONEY" in token:
        return "ASSET_MANAGER_LONG_ONLY"
    if "BANK" in token or "PB" in token:
        return "BANK"
    if "CORPORATE" in token:
        return "CORPORATE_TREASURY"
    if "INTERNAL" in token:
        return "BANK"
    return CLIENT_TYPES[0]


def region_fallbacks(region: str) -> list[str]:
    region = normalize_region(region)
    if not region:
        return [r for r in REGIONS]
    ordered = [region]
    for item in REGIONS:
        if item != region:
            ordered.append(item)
    return ordered


def extract_structured_signals_from_text(text: str) -> dict[str, str | list[str]]:
    raw = (text or "").upper()
    pairs = _dedupe_keep_order([normalize_ccy_pair(match) for match in re.findall(r"\b([A-Z]{6})\b", raw)])

    products: list[str] = []
    for token in re.findall(r"\b[A-Z0-9_]{3,30}\b", raw):
        if token in PRODUCT_TYPES:
            products.append(token)
    for phrase, mapped in PRODUCT_KEYWORD_MAP.items():
        if phrase in raw:
            products.extend(mapped)
    products = [p for p in _dedupe_keep_order(products) if p in PRODUCT_TYPES]

    tenor = ""
    if re.search(r"\b1W\b", raw):
        tenor = "1W"
    elif re.search(r"\b1M\b|\b2W\b", raw):
        tenor = "2W-1M"
    elif re.search(r"\b3M\b|\b2M\b", raw):
        tenor = "1M-3M"
    elif re.search(r"\b6M\b|\b9M\b|\b12M\b|\b1Y\b", raw):
        tenor = "6M-1Y"

    region = ""
    for reg in REGION_MAP:
        if reg in raw:
            region = REGION_MAP[reg]
            break

    primary_pair = pairs[0] if pairs else ""
    primary_product = products[0] if products else ""
    return {
        "ccy_pair": primary_pair,
        "product_type": primary_product,
        "ccy_pairs": pairs,
        "product_types": products,
        "tenor_bucket": tenor,
        "region": region,
    }
