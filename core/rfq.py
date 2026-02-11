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


def extract_structured_signals_from_text(text: str) -> dict[str, str]:
    raw = (text or "").upper()
    pair_match = re.search(r"\b([A-Z]{6})\b", raw)
    pair = normalize_ccy_pair(pair_match.group(1)) if pair_match else ""

    product = ""
    for token in re.findall(r"\b[A-Z0-9_]{3,20}\b", raw):
        if token in PRODUCT_TYPES:
            product = token
            break

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

    return {
        "ccy_pair": pair,
        "product_type": product,
        "tenor_bucket": tenor,
        "region": region,
    }

