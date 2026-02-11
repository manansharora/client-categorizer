from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from core.constants import MATCH_CLIENT_CANDIDATE_CAP, MATCH_CLIENT_FALLBACK_MIN, MATCH_PM_CAP
from core.features import FastTextEmbedder, bm25_score_documents, min_max_scale
from core.matching import adjusted_family_weights, tags_to_family_map, taxonomy_overlap_score, top_matching_terms
from core.profiles import weighted_text_from_observations
from core.repository import Repository
from core.rfq import extract_structured_signals_from_text, region_fallbacks
from core.tagging import extract_tags
from core.text_processing import build_synonym_map, normalize_text


@dataclass
class ClientProfile:
    client_id: int
    client_name: str
    client_type: str
    text: str
    tags: list[dict[str, Any]]


def _weighted_sum(values: dict[str, float]) -> float:
    return values["structured"] * 0.45 + values["semantic"] * 0.25 + values["lexical"] * 0.15 + values["taxonomy"] * 0.15


class ClientCategorizerService:
    def __init__(self, repo: Repository):
        self.repo = repo
        self._taxonomy_rows: list[dict[str, Any]] = []
        self._synonym_rows: list[dict[str, Any]] = []
        self._synonym_map: dict[str, str] = {}
        self.refresh_resources()

    def refresh_resources(self) -> None:
        self._taxonomy_rows = [dict(r) for r in self.repo.list_taxonomy_tags()]
        self._synonym_rows = [dict(r) for r in self.repo.list_synonyms()]
        self._synonym_map = build_synonym_map(self._synonym_rows)

    def normalize_text(self, text: str) -> str:
        return normalize_text(text, self._synonym_map)

    def infer_tags(self, text: str) -> list[tuple[str, float]]:
        return extract_tags(
            text=text,
            taxonomy_rows=self._taxonomy_rows,
            synonym_rows=self._synonym_rows,
            synonym_map=self._synonym_map,
        )

    def auto_tag_observation(self, obs_id: int, obs_text: str) -> None:
        tags = self.infer_tags(obs_text)
        self.repo.replace_entity_tags("OBSERVATION", obs_id, tags, origin="RULE")

    def auto_tag_idea(self, idea_id: int, idea_text: str) -> None:
        tags = self.infer_tags(idea_text)
        self.repo.replace_entity_tags("IDEA", idea_id, tags, origin="RULE")

    def refresh_client_tags_from_observations(self, client_id: int) -> None:
        observations = [dict(o) for o in self.repo.list_observations(client_id)]
        merged: dict[str, float] = {}
        for obs in observations:
            obs_tags = [dict(t) for t in self.repo.list_entity_tags("OBSERVATION", int(obs["obs_id"]))]
            for tag in obs_tags:
                code = tag["tag_code"]
                merged[code] = max(merged.get(code, 0.0), float(tag["confidence"]) * 0.9)
        self.repo.replace_entity_tags("CLIENT", client_id, [(k, v) for k, v in merged.items()], origin="RULE")

    def _idea_tags(self, idea_id: int | None, idea_text: str) -> list[dict[str, Any]]:
        if idea_id is None:
            inferred = self.infer_tags(idea_text)
            family_map = {r["tag_code"]: r["tag_family"] for r in self._taxonomy_rows}
            return [{"tag_code": code, "confidence": conf, "tag_family": family_map.get(code, "UNKNOWN")} for code, conf in inferred]
        tags = [dict(t) for t in self.repo.list_entity_tags("IDEA", idea_id)]
        if not tags:
            inferred = self.infer_tags(idea_text)
            self.repo.replace_entity_tags("IDEA", idea_id, inferred, origin="RULE")
            tags = [dict(t) for t in self.repo.list_entity_tags("IDEA", idea_id)]
        return tags

    def build_client_profile(self, client_id: int) -> ClientProfile | None:
        client = self.repo.get_client(client_id)
        if client is None:
            return None

        cache = self.repo.get_entity_profile_cache("CLIENT", client_id)
        if cache:
            profile_text = str(cache["profile_text"])
        else:
            observations = [dict(o) for o in self.repo.list_observations(client_id)]
            profile_text = weighted_text_from_observations(observations, self.normalize_text)

        tags = [dict(t) for t in self.repo.list_entity_tags("CLIENT", client_id)]
        return ClientProfile(
            client_id=int(client["client_id"]),
            client_name=str(client["client_name"]),
            client_type=str(client["client_type"]),
            text=profile_text,
            tags=tags,
        )

    def _build_profiles_for_client_ids(self, client_ids: list[int]) -> list[ClientProfile]:
        client_rows = {int(c["client_id"]): dict(c) for c in self.repo.list_clients_by_ids(client_ids)}
        profile_cache = {int(r["entity_id"]): str(r["profile_text"]) for r in self.repo.list_entity_profile_cache_bulk("CLIENT", client_ids)}
        tags_bulk = [dict(t) for t in self.repo.list_entity_tags_bulk("CLIENT", client_ids)]
        tags_by_client: dict[int, list[dict[str, Any]]] = {cid: [] for cid in client_ids}
        for row in tags_bulk:
            tags_by_client[int(row["entity_id"])].append(row)

        profiles: list[ClientProfile] = []
        for client_id in client_ids:
            client = client_rows.get(client_id)
            if not client:
                continue
            text = profile_cache.get(client_id, "")
            if not text:
                observations = [dict(o) for o in self.repo.list_observations(client_id)]
                text = weighted_text_from_observations(observations, self.normalize_text)
            profiles.append(
                ClientProfile(
                    client_id=client_id,
                    client_name=str(client["client_name"]),
                    client_type=str(client["client_type"]),
                    text=text,
                    tags=tags_by_client.get(client_id, []),
                )
            )
        return profiles

    def _choose_feature_stages(self, pair: str, product: str, tenor: str) -> list[list[str]]:
        stages: list[list[str]] = []
        if pair and product and tenor:
            stages.append(["PAIR_PRODUCT_TENOR"])
        if pair and product:
            stages.append(["PAIR_PRODUCT"])
        if pair and product:
            stages.append(["PAIR", "PRODUCT"])
        elif pair:
            stages.append(["PAIR", "PAIR_PRODUCT"])
        elif product:
            stages.append(["PRODUCT", "PAIR_PRODUCT"])
        if tenor:
            stages.append(["TENOR"])
        if not stages:
            stages.append(["PAIR_PRODUCT", "PAIR", "PRODUCT"])
        return stages

    def _candidate_clients(
        self,
        idea_text: str,
        region: str | None,
        country: str | None,
        cap: int,
        fallback_min: int,
    ) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
        signals = extract_structured_signals_from_text(idea_text)
        target_region = (region or signals.get("region") or "EUROPE").upper()
        target_country = (country or "").upper() or None
        pair = signals.get("ccy_pair", "")
        product = signals.get("product_type", "")
        tenor = signals.get("tenor_bucket", "")

        stage_defs = self._choose_feature_stages(pair, product, tenor)
        regions_to_try = region_fallbacks(target_region)

        candidates: dict[int, dict[str, Any]] = {}
        used_regions: list[str] = []

        for idx, reg in enumerate(regions_to_try):
            region_penalty = 1.0 if idx == 0 else 0.85
            used_regions.append(reg)

            for stage in stage_defs:
                rows = self.repo.query_rfq_candidate_entities(
                    entity_type="CLIENT",
                    region=reg,
                    country=target_country if idx == 0 else None,
                    feature_kinds=stage,
                    ccy_pair=pair or None,
                    product_type=product or None,
                    tenor_bucket=tenor or None,
                    limit=cap,
                )
                for row in rows:
                    client_id = int(row["entity_id"])
                    structured_raw = float(row["recency_sum"] or 0.0) + 0.002 * float(row["trade_count_sum"] or 0.0)
                    structured_raw *= region_penalty
                    existing = candidates.get(client_id)
                    payload = {
                        "structured_raw": structured_raw,
                        "trade_count_sum": float(row["trade_count_sum"] or 0.0),
                        "score_30d_sum": float(row["score_30d_sum"] or 0.0),
                        "score_90d_sum": float(row["score_90d_sum"] or 0.0),
                        "score_365d_sum": float(row["score_365d_sum"] or 0.0),
                        "matched_region": reg,
                        "strict_region": idx == 0,
                        "stage": ",".join(stage),
                    }
                    if existing is None or payload["structured_raw"] > existing["structured_raw"]:
                        candidates[client_id] = payload

            if idx == 0 and len(candidates) >= fallback_min:
                break

        metadata = {
            "target_region": target_region,
            "target_country": target_country,
            "used_regions": used_regions,
            "fallback_used": len(used_regions) > 1,
            "signals": signals,
        }
        return candidates, metadata

    def _extract_explanation(
        self,
        query_text: str,
        candidate_text: str,
        query_family: dict[str, set[str]],
        candidate_family: dict[str, set[str]],
        scores: dict[str, float],
        structured_meta: dict[str, Any],
    ) -> dict[str, Any]:
        matched_tags: list[str] = []
        for family, query_tags in query_family.items():
            matched_tags.extend(sorted(query_tags & candidate_family.get(family, set())))
        top_terms = top_matching_terms(query_text, candidate_text, k=5)
        explanation_text = (
            f"Structured={scores['structured']:.3f}, Semantic={scores['semantic']:.3f}, "
            f"Lexical={scores['lexical']:.3f}, Taxonomy={scores['taxonomy']:.3f}, Final={scores['final']:.3f}"
        )
        return {
            "matched_tags": sorted(set(matched_tags)),
            "top_terms": top_terms,
            "explanation_text": explanation_text,
            "feature_evidence": {
                "region": structured_meta.get("matched_region"),
                "stage": structured_meta.get("stage"),
                "trade_count_sum": structured_meta.get("trade_count_sum", 0.0),
                "score_30d_sum": structured_meta.get("score_30d_sum", 0.0),
                "score_90d_sum": structured_meta.get("score_90d_sum", 0.0),
                "score_365d_sum": structured_meta.get("score_365d_sum", 0.0),
            },
        }

    def _pm_profile_text(self, pm_id: int) -> str:
        cache = self.repo.get_entity_profile_cache("PM", pm_id)
        if cache:
            return str(cache["profile_text"])
        observations = [dict(o) for o in self.repo.list_pm_observations(pm_id)]
        if not observations:
            return ""
        return " ".join(self.normalize_text(o["obs_text"]) for o in observations).strip()

    def rank_pms_for_clients(
        self,
        idea_text: str,
        client_results: list[dict[str, Any]],
        region: str | None,
        country: str | None,
        per_client_cap: int = MATCH_PM_CAP,
    ) -> dict[int, list[dict[str, Any]]]:
        signals = extract_structured_signals_from_text(idea_text)
        target_region = (region or signals.get("region") or "EUROPE").upper()
        target_country = (country or "").upper() or None
        pair = signals.get("ccy_pair", "")
        product = signals.get("product_type", "")
        tenor = signals.get("tenor_bucket", "")
        ranked: dict[int, list[dict[str, Any]]] = {}

        for client_row in client_results[:10]:
            client_id = int(client_row["target_entity_id"])
            pms = [dict(p) for p in self.repo.list_pms(client_id=client_id, active_only=True)]
            if not pms:
                ranked[client_id] = []
                continue

            pm_ids = [int(p["pm_id"]) for p in pms]
            pm_profiles = {pm_id: self._pm_profile_text(pm_id) for pm_id in pm_ids}
            docs = [pm_profiles[pm_id] for pm_id in pm_ids]
            lexical_scores = bm25_score_documents(idea_text, docs)
            embedder = FastTextEmbedder()
            embedder.fit([idea_text] + docs)
            query_vec = embedder.encode(idea_text)
            semantic_scores = [float(0.0) for _ in pm_ids]
            for idx, text in enumerate(docs):
                semantic_scores[idx] = float((query_vec @ embedder.encode(text)) / ((query_vec @ query_vec) ** 0.5 + 1e-9))
            semantic_scores = min_max_scale(semantic_scores)

            raw_structured: list[float] = []
            pm_meta: dict[int, dict[str, Any]] = {}
            for pm_id in pm_ids:
                feats = [dict(f) for f in self.repo.list_rfq_features_for_entity("PM", pm_id)]
                trade_sum = 0.0
                recency_sum = 0.0
                for feat in feats:
                    if str(feat["region"]) != target_region:
                        continue
                    if target_country and str(feat["country"] or "") != target_country:
                        continue
                    if pair and str(feat["ccy_pair"] or "") not in ("", pair):
                        continue
                    if product and str(feat["product_type"] or "") not in ("", product):
                        continue
                    if tenor and str(feat["tenor_bucket"] or "") not in ("", tenor):
                        continue
                    trade_sum += float(feat["trade_count"] or 0.0)
                    recency_sum += float(feat["recency_score"] or 0.0)
                raw = recency_sum + 0.002 * trade_sum
                raw_structured.append(raw)
                pm_meta[pm_id] = {"trade_sum": trade_sum, "recency_sum": recency_sum}

            structured_scores = min_max_scale(raw_structured)
            pm_results: list[dict[str, Any]] = []
            for idx, pm in enumerate(pms):
                pm_id = int(pm["pm_id"])
                pm_base = 0.5 * structured_scores[idx] + 0.3 * semantic_scores[idx] + 0.2 * lexical_scores[idx]
                alpha = min(0.7, pm_meta[pm_id]["trade_sum"] / 100.0)
                final = alpha * pm_base + (1.0 - alpha) * float(client_row["final_score"])
                pm_results.append(
                    {
                        "pm_id": pm_id,
                        "pm_name": str(pm["pm_name"]),
                        "pm_score": round(float(final), 6),
                        "alpha": round(float(alpha), 4),
                        "trade_sum": round(float(pm_meta[pm_id]["trade_sum"]), 4),
                        "recency_sum": round(float(pm_meta[pm_id]["recency_sum"]), 4),
                    }
                )
            pm_results.sort(key=lambda x: x["pm_score"], reverse=True)
            ranked[client_id] = pm_results[:per_client_cap]

        return ranked

    def match_clients_for_idea(
        self,
        idea_text: str,
        idea_id: int | None = None,
        input_ref: str | None = None,
        top_n: int = 20,
        region: str | None = None,
        country: str | None = None,
        candidate_cap: int = MATCH_CLIENT_CANDIDATE_CAP,
        fallback_min: int = MATCH_CLIENT_FALLBACK_MIN,
        include_pm: bool = True,
    ) -> tuple[int, list[dict[str, Any]], dict[str, Any]]:
        normalized_idea = self.normalize_text(idea_text)
        idea_tags = self._idea_tags(idea_id, normalized_idea)
        idea_family = tags_to_family_map(idea_tags)

        candidate_meta, meta = self._candidate_clients(
            idea_text=normalized_idea,
            region=region,
            country=country,
            cap=candidate_cap,
            fallback_min=fallback_min,
        )
        candidate_ids = list(candidate_meta.keys())
        profiles = self._build_profiles_for_client_ids(candidate_ids)
        if not profiles:
            run_id = self.repo.create_match_run("JOB_B", input_ref or (f"idea:{idea_id}" if idea_id else "adhoc"))
            return run_id, [], meta

        docs = [p.text for p in profiles]
        lexical_scores = bm25_score_documents(normalized_idea, docs)
        embedder = FastTextEmbedder()
        embedder.fit([normalized_idea] + docs)
        query_vec = embedder.encode(normalized_idea)
        semantic_raw: list[float] = []
        for doc in docs:
            vec = embedder.encode(doc)
            denom = float(((query_vec @ query_vec) ** 0.5) * ((vec @ vec) ** 0.5) + 1e-9)
            semantic_raw.append(float((query_vec @ vec) / denom if denom > 0 else 0.0))
        semantic_scores = min_max_scale(semantic_raw)

        structured_raw = [float(candidate_meta[p.client_id]["structured_raw"]) for p in profiles]
        structured_scores = min_max_scale(structured_raw)

        results: list[dict[str, Any]] = []
        for idx, profile in enumerate(profiles):
            family_weights = adjusted_family_weights(profile.client_type)
            candidate_family = tags_to_family_map(profile.tags)
            taxonomy = taxonomy_overlap_score(idea_family, candidate_family, family_weights)
            scores = {
                "structured": float(structured_scores[idx]),
                "semantic": float(semantic_scores[idx]),
                "lexical": float(lexical_scores[idx]),
                "taxonomy": float(taxonomy),
                "final": 0.0,
            }
            scores["final"] = _weighted_sum(scores)
            explanation = self._extract_explanation(
                query_text=normalized_idea,
                candidate_text=profile.text,
                query_family=idea_family,
                candidate_family=candidate_family,
                scores=scores,
                structured_meta=candidate_meta[profile.client_id],
            )
            results.append(
                {
                    "target_entity_id": profile.client_id,
                    "target_name": profile.client_name,
                    "semantic_score": round(scores["semantic"], 6),
                    "lexical_score": round(scores["lexical"], 6),
                    "taxonomy_score": round(scores["taxonomy"], 6),
                    "structured_score": round(scores["structured"], 6),
                    "final_score": round(scores["final"], 6),
                    "explanation": explanation,
                }
            )

        results.sort(key=lambda x: x["final_score"], reverse=True)
        results = results[:top_n]
        if include_pm and results:
            pm_rankings = self.rank_pms_for_clients(normalized_idea, results, region=region, country=country)
            for row in results:
                row["pm_drilldown"] = pm_rankings.get(int(row["target_entity_id"]), [])

        run_id = self.repo.create_match_run("JOB_B", input_ref or (f"idea:{idea_id}" if idea_id else "adhoc"))
        self.repo.add_match_results(run_id, "CLIENT", results)
        return run_id, results, meta

    def match_ideas_for_client(
        self,
        client_id: int,
        input_ref: str | None = None,
        top_n: int = 20,
    ) -> tuple[int, list[dict[str, Any]]]:
        profile = self.build_client_profile(client_id)
        if profile is None:
            raise ValueError(f"Client not found: {client_id}")

        client_family = tags_to_family_map(profile.tags)
        ideas = [dict(i) for i in self.repo.list_ideas()]
        idea_docs = [self.normalize_text(i["idea_text"]) for i in ideas]
        lexical_scores = bm25_score_documents(profile.text, idea_docs)

        embedder = FastTextEmbedder()
        embedder.fit([profile.text] + idea_docs)
        query_vec = embedder.encode(profile.text)
        semantic_raw: list[float] = []
        for text in idea_docs:
            vec = embedder.encode(text)
            denom = float(((query_vec @ query_vec) ** 0.5) * ((vec @ vec) ** 0.5) + 1e-9)
            semantic_raw.append(float((query_vec @ vec) / denom if denom > 0 else 0.0))
        semantic_scores = min_max_scale(semantic_raw)

        results: list[dict[str, Any]] = []
        family_weights = adjusted_family_weights(profile.client_type)
        for idx, idea in enumerate(ideas):
            idea_tags = self._idea_tags(int(idea["idea_id"]), idea_docs[idx])
            idea_family = tags_to_family_map(idea_tags)
            taxonomy = taxonomy_overlap_score(client_family, idea_family, family_weights)
            final_score = 0.45 * semantic_scores[idx] + 0.35 * lexical_scores[idx] + 0.20 * taxonomy
            explanation = {
                "matched_tags": sorted(set().union(*[client_family.get(k, set()) & idea_family.get(k, set()) for k in client_family.keys()])),
                "top_terms": top_matching_terms(profile.text, idea_docs[idx], k=5),
                "explanation_text": (
                    f"Semantic={semantic_scores[idx]:.3f}, Lexical={lexical_scores[idx]:.3f}, "
                    f"Taxonomy={taxonomy:.3f}, Final={final_score:.3f}"
                ),
            }
            results.append(
                {
                    "target_entity_id": int(idea["idea_id"]),
                    "target_name": idea["idea_title"],
                    "semantic_score": round(float(semantic_scores[idx]), 6),
                    "lexical_score": round(float(lexical_scores[idx]), 6),
                    "taxonomy_score": round(float(taxonomy), 6),
                    "final_score": round(float(final_score), 6),
                    "explanation": explanation,
                }
            )

        results.sort(key=lambda x: x["final_score"], reverse=True)
        results = results[:top_n]
        run_id = self.repo.create_match_run("JOB_A", input_ref or f"client:{client_id}")
        self.repo.add_match_results(run_id, "IDEA", results)
        return run_id, results
