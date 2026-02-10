import math
from datetime import date, datetime

from core.constants import RECENCY_HALFLIFE_DAYS, SIGNAL_WEIGHTS


def _parse_obs_date(obs_date_str: str) -> date:
    try:
        return datetime.strptime(obs_date_str, "%Y-%m-%d").date()
    except ValueError:
        return date.today()


def recency_weight(obs_date_str: str) -> float:
    obs_date = _parse_obs_date(obs_date_str)
    days_old = max(0, (date.today() - obs_date).days)
    if RECENCY_HALFLIFE_DAYS <= 0:
        return 1.0
    return math.exp(-days_old / RECENCY_HALFLIFE_DAYS)


def observation_weight(obs_type: str, obs_date_str: str, source_confidence: float) -> float:
    base = SIGNAL_WEIGHTS.get(obs_type, 0.6)
    return base * recency_weight(obs_date_str) * max(0.0, min(1.0, source_confidence))


def weighted_text_from_observations(observations: list[dict], normalizer) -> str:
    chunks: list[str] = []
    for obs in observations:
        normalized = normalizer(obs["obs_text"])
        if not normalized:
            continue
        w = observation_weight(obs["obs_type"], obs["obs_date"], float(obs["source_confidence"]))
        repeats = max(1, int(round(w * 3)))
        chunks.extend([normalized] * repeats)
    return " ".join(chunks).strip()

