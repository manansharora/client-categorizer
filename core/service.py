from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.features import FastTextEmbedder, bm25_score_documents
from core.matching import (
    adjusted_family_weights,
    combine_scores,
    semantic_scores_from_vectors,
    tags_to_family_map,
    taxonomy_overlap_score,
    top_matching_terms,
)
from core.profiles import weighted_text_from_observations
from core.repository import Repository
from core.tagging import extract_tags
from core.text_processing import build_synonym_map, normalize_text


@dataclass
class ClientProfile:
    client_id: int
    client_name: str
    client_type: str
    text: str
    tags: list[dict[str, Any]]


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
            for t in obs_tags:
                code = t["tag_code"]
                merged[code] = max(merged.get(code, 0.0), float(t["confidence"]) * 0.9)
        tags = [(code, conf) for code, conf in merged.items()]
        self.repo.replace_entity_tags("CLIENT", client_id, tags, origin="RULE")

    def _aggregate_client_tags(self, client_id: int) -> list[dict[str, Any]]:
        tag_map: dict[str, dict[str, Any]] = {}
        client_tags = [dict(r) for r in self.repo.list_entity_tags("CLIENT", client_id)]
        for row in client_tags:
            code = row["tag_code"]
            tag_map[code] = row

        observations = [dict(o) for o in self.repo.list_observations(client_id)]
        for obs in observations:
            obs_tags = [dict(r) for r in self.repo.list_entity_tags("OBSERVATION", int(obs["obs_id"]))]
            for row in obs_tags:
                code = row["tag_code"]
                existing = tag_map.get(code)
                if existing is None or float(row["confidence"]) > float(existing["confidence"]):
                    tag_map[code] = row
        return list(tag_map.values())

    def build_client_profile(self, client_id: int) -> ClientProfile | None:
        client = self.repo.get_client(client_id)
        if not client:
            return None
        observations = [dict(o) for o in self.repo.list_observations(client_id)]
        profile_text = weighted_text_from_observations(observations, self.normalize_text)
        tags = self._aggregate_client_tags(client_id)
        return ClientProfile(
            client_id=int(client["client_id"]),
            client_name=str(client["client_name"]),
            client_type=str(client["client_type"]),
            text=profile_text,
            tags=tags,
        )

    def _idea_tags(self, idea_id: int | None, idea_text: str) -> list[dict[str, Any]]:
        if idea_id is None:
            inferred = self.infer_tags(idea_text)
            family_map = {r["tag_code"]: r["tag_family"] for r in self._taxonomy_rows}
            return [
                {"tag_code": code, "confidence": conf, "tag_family": family_map.get(code, "UNKNOWN")}
                for code, conf in inferred
            ]
        tags = [dict(t) for t in self.repo.list_entity_tags("IDEA", idea_id)]
        if not tags:
            inferred = self.infer_tags(idea_text)
            self.repo.replace_entity_tags("IDEA", idea_id, inferred, origin="RULE")
            tags = [dict(t) for t in self.repo.list_entity_tags("IDEA", idea_id)]
        return tags

    def _extract_explanation(
        self,
        query_text: str,
        candidate_text: str,
        query_family: dict[str, set[str]],
        candidate_family: dict[str, set[str]],
        semantic_score: float,
        lexical_score: float,
        taxonomy_score: float,
        final_score: float,
    ) -> dict[str, Any]:
        matched_tags: list[str] = []
        for family, query_tags in query_family.items():
            candidate_tags = candidate_family.get(family, set())
            matched_tags.extend(sorted(query_tags & candidate_tags))
        matched_tags = sorted(set(matched_tags))
        top_terms = top_matching_terms(query_text, candidate_text, k=5)
        explanation_text = (
            f"Semantic={semantic_score:.3f}, Lexical={lexical_score:.3f}, "
            f"Taxonomy={taxonomy_score:.3f}, Final={final_score:.3f}"
        )
        return {
            "matched_tags": matched_tags,
            "top_terms": top_terms,
            "explanation_text": explanation_text,
        }

    def match_clients_for_idea(
        self,
        idea_text: str,
        idea_id: int | None = None,
        input_ref: str | None = None,
        top_n: int = 20,
    ) -> tuple[int, list[dict[str, Any]]]:
        normalized_idea = self.normalize_text(idea_text)
        idea_tags = self._idea_tags(idea_id, normalized_idea)
        idea_family = tags_to_family_map(idea_tags)

        clients = [dict(c) for c in self.repo.list_clients(active_only=True)]
        profiles: list[ClientProfile] = []
        for client in clients:
            profile = self.build_client_profile(int(client["client_id"]))
            if profile:
                profiles.append(profile)

        client_docs = [p.text for p in profiles]
        lexical_scores = bm25_score_documents(normalized_idea, client_docs)

        embedder = FastTextEmbedder()
        embedder.fit([normalized_idea] + client_docs)
        idea_vec = embedder.encode(normalized_idea)
        client_vecs = [embedder.encode(text) for text in client_docs]
        semantic_scores = semantic_scores_from_vectors(idea_vec, client_vecs)

        results: list[dict[str, Any]] = []
        for idx, profile in enumerate(profiles):
            family_weights = adjusted_family_weights(profile.client_type)
            candidate_family = tags_to_family_map(profile.tags)
            taxonomy_score = taxonomy_overlap_score(idea_family, candidate_family, family_weights)
            semantic_score = semantic_scores[idx]
            lexical_score = lexical_scores[idx]
            final_score = combine_scores(semantic_score, lexical_score, taxonomy_score)

            explanation = self._extract_explanation(
                query_text=normalized_idea,
                candidate_text=profile.text,
                query_family=idea_family,
                candidate_family=candidate_family,
                semantic_score=semantic_score,
                lexical_score=lexical_score,
                taxonomy_score=taxonomy_score,
                final_score=final_score,
            )
            result = {
                "target_entity_id": profile.client_id,
                "target_name": profile.client_name,
                "semantic_score": round(float(semantic_score), 6),
                "lexical_score": round(float(lexical_score), 6),
                "taxonomy_score": round(float(taxonomy_score), 6),
                "final_score": round(float(final_score), 6),
                "explanation": explanation,
            }
            results.append(result)
            self.repo.upsert_entity_vector("CLIENT", profile.client_id, "FASTTEXT", client_vecs[idx].tolist())

        if idea_id is not None:
            self.repo.upsert_entity_vector("IDEA", idea_id, "FASTTEXT", idea_vec.tolist())

        results.sort(key=lambda x: x["final_score"], reverse=True)
        results = results[:top_n]
        run_id = self.repo.create_match_run("JOB_B", input_ref or (f"idea:{idea_id}" if idea_id else "adhoc"))
        self.repo.add_match_results(run_id, "CLIENT", results)
        return run_id, results

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
        client_vec = embedder.encode(profile.text)
        idea_vecs = [embedder.encode(text) for text in idea_docs]
        semantic_scores = semantic_scores_from_vectors(client_vec, idea_vecs)

        family_weights = adjusted_family_weights(profile.client_type)
        results: list[dict[str, Any]] = []

        for idx, idea in enumerate(ideas):
            idea_id = int(idea["idea_id"])
            idea_tags = self._idea_tags(idea_id, idea_docs[idx])
            idea_family = tags_to_family_map(idea_tags)
            taxonomy_score = taxonomy_overlap_score(client_family, idea_family, family_weights)
            semantic_score = semantic_scores[idx]
            lexical_score = lexical_scores[idx]
            final_score = combine_scores(semantic_score, lexical_score, taxonomy_score)
            explanation = self._extract_explanation(
                query_text=profile.text,
                candidate_text=idea_docs[idx],
                query_family=client_family,
                candidate_family=idea_family,
                semantic_score=semantic_score,
                lexical_score=lexical_score,
                taxonomy_score=taxonomy_score,
                final_score=final_score,
            )
            result = {
                "target_entity_id": idea_id,
                "target_name": idea["idea_title"],
                "semantic_score": round(float(semantic_score), 6),
                "lexical_score": round(float(lexical_score), 6),
                "taxonomy_score": round(float(taxonomy_score), 6),
                "final_score": round(float(final_score), 6),
                "explanation": explanation,
            }
            results.append(result)
            self.repo.upsert_entity_vector("IDEA", idea_id, "FASTTEXT", idea_vecs[idx].tolist())

        self.repo.upsert_entity_vector("CLIENT", profile.client_id, "FASTTEXT", client_vec.tolist())
        results.sort(key=lambda x: x["final_score"], reverse=True)
        results = results[:top_n]

        run_id = self.repo.create_match_run("JOB_A", input_ref or f"client:{client_id}")
        self.repo.add_match_results(run_id, "IDEA", results)
        return run_id, results

