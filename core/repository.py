import json
from datetime import date
from typing import Any

import sqlite3


class Repository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create_client(self, client_name: str, client_type: str, active_flag: int = 1) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO clients(client_name, client_type, active_flag)
            VALUES (?, ?, ?)
            """,
            (client_name.strip(), client_type, active_flag),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def update_client(self, client_id: int, client_type: str, active_flag: int) -> None:
        self.conn.execute(
            """
            UPDATE clients
            SET client_type = ?, active_flag = ?, updated_at = CURRENT_TIMESTAMP
            WHERE client_id = ?
            """,
            (client_type, active_flag, client_id),
        )
        self.conn.commit()

    def list_clients(self, active_only: bool = False) -> list[sqlite3.Row]:
        if active_only:
            cur = self.conn.execute(
                "SELECT * FROM clients WHERE active_flag = 1 ORDER BY client_name ASC"
            )
        else:
            cur = self.conn.execute("SELECT * FROM clients ORDER BY client_name ASC")
        return cur.fetchall()

    def get_client(self, client_id: int) -> sqlite3.Row | None:
        cur = self.conn.execute("SELECT * FROM clients WHERE client_id = ?", (client_id,))
        return cur.fetchone()

    def get_client_by_name(self, client_name: str) -> sqlite3.Row | None:
        cur = self.conn.execute(
            "SELECT * FROM clients WHERE client_name = ?",
            (client_name.strip(),),
        )
        return cur.fetchone()

    def add_observation(
        self,
        client_id: int,
        obs_type: str,
        obs_text: str,
        obs_date: date,
        source_confidence: float,
    ) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO client_observations(client_id, obs_type, obs_text, obs_date, source_confidence)
            VALUES (?, ?, ?, ?, ?)
            """,
            (client_id, obs_type, obs_text.strip(), obs_date.isoformat(), source_confidence),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def list_observations(self, client_id: int | None = None) -> list[sqlite3.Row]:
        if client_id is None:
            cur = self.conn.execute(
                """
                SELECT o.*, c.client_name
                FROM client_observations o
                JOIN clients c ON c.client_id = o.client_id
                ORDER BY o.obs_date DESC, o.obs_id DESC
                """
            )
        else:
            cur = self.conn.execute(
                """
                SELECT * FROM client_observations
                WHERE client_id = ?
                ORDER BY obs_date DESC, obs_id DESC
                """,
                (client_id,),
            )
        return cur.fetchall()

    def observation_exists(self, client_id: int, obs_type: str, obs_text: str, obs_date: str) -> bool:
        cur = self.conn.execute(
            """
            SELECT 1
            FROM client_observations
            WHERE client_id = ? AND obs_type = ? AND obs_date = ? AND obs_text = ?
            LIMIT 1
            """,
            (client_id, obs_type, obs_date, obs_text.strip()),
        )
        return cur.fetchone() is not None

    def create_idea(self, idea_title: str, idea_text: str, created_by: str | None) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO ideas(idea_title, idea_text, created_by)
            VALUES (?, ?, ?)
            """,
            (idea_title.strip(), idea_text.strip(), (created_by or "").strip() or None),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def update_idea(self, idea_id: int, idea_title: str, idea_text: str, created_by: str | None) -> None:
        self.conn.execute(
            """
            UPDATE ideas
            SET idea_title = ?, idea_text = ?, created_by = ?
            WHERE idea_id = ?
            """,
            (idea_title.strip(), idea_text.strip(), (created_by or "").strip() or None, idea_id),
        )
        self.conn.commit()

    def get_idea(self, idea_id: int) -> sqlite3.Row | None:
        cur = self.conn.execute("SELECT * FROM ideas WHERE idea_id = ?", (idea_id,))
        return cur.fetchone()

    def get_idea_by_title(self, idea_title: str) -> sqlite3.Row | None:
        cur = self.conn.execute(
            "SELECT * FROM ideas WHERE idea_title = ?",
            (idea_title.strip(),),
        )
        return cur.fetchone()

    def list_ideas(self) -> list[sqlite3.Row]:
        cur = self.conn.execute("SELECT * FROM ideas ORDER BY created_at DESC, idea_id DESC")
        return cur.fetchall()

    def list_taxonomy_tags(self, taxonomy_version: str | None = None) -> list[sqlite3.Row]:
        if taxonomy_version:
            cur = self.conn.execute(
                """
                SELECT * FROM taxonomy_tags
                WHERE taxonomy_version = ?
                ORDER BY tag_family, tag_code
                """,
                (taxonomy_version,),
            )
        else:
            cur = self.conn.execute("SELECT * FROM taxonomy_tags ORDER BY tag_family, tag_code")
        return cur.fetchall()

    def add_taxonomy_tag(
        self, tag_family: str, tag_code: str, tag_label: str, taxonomy_version: str = "v1"
    ) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO taxonomy_tags(tag_family, tag_code, tag_label, taxonomy_version)
            VALUES (?, ?, ?, ?)
            """,
            (tag_family.strip(), tag_code.strip(), tag_label.strip(), taxonomy_version),
        )
        self.conn.commit()

    def list_synonyms(self, taxonomy_version: str | None = None) -> list[sqlite3.Row]:
        if taxonomy_version:
            cur = self.conn.execute(
                """
                SELECT * FROM synonyms
                WHERE taxonomy_version = ?
                ORDER BY surface_form
                """,
                (taxonomy_version,),
            )
        else:
            cur = self.conn.execute("SELECT * FROM synonyms ORDER BY surface_form")
        return cur.fetchall()

    def add_synonym(
        self,
        surface_form: str,
        canonical_form: str,
        tag_code_optional: str | None,
        taxonomy_version: str = "v1",
    ) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO synonyms(surface_form, canonical_form, tag_code_optional, taxonomy_version)
            VALUES (?, ?, ?, ?)
            """,
            (
                surface_form.strip().lower(),
                canonical_form.strip().lower(),
                tag_code_optional.strip() if tag_code_optional else None,
                taxonomy_version,
            ),
        )
        self.conn.commit()

    def list_entity_tags(self, entity_type: str, entity_id: int) -> list[sqlite3.Row]:
        cur = self.conn.execute(
            """
            SELECT et.*, tt.tag_family, tt.tag_label
            FROM entity_tags et
            JOIN taxonomy_tags tt ON tt.tag_code = et.tag_code
            WHERE et.entity_type = ? AND et.entity_id = ?
            ORDER BY et.confidence DESC, et.tag_code ASC
            """,
            (entity_type, entity_id),
        )
        return cur.fetchall()

    def replace_entity_tags(
        self,
        entity_type: str,
        entity_id: int,
        tags: list[tuple[str, float]],
        origin: str,
    ) -> None:
        self.conn.execute(
            """
            DELETE FROM entity_tags
            WHERE entity_type = ? AND entity_id = ? AND origin = ?
            """,
            (entity_type, entity_id, origin),
        )
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO entity_tags(entity_type, entity_id, tag_code, confidence, origin)
            VALUES (?, ?, ?, ?, ?)
            """,
            [(entity_type, entity_id, code, conf, origin) for code, conf in tags],
        )
        self.conn.commit()

    def set_manual_tags(self, entity_type: str, entity_id: int, tag_codes: list[str]) -> None:
        tags = [(code, 1.0) for code in tag_codes]
        self.replace_entity_tags(entity_type, entity_id, tags, origin="MANUAL")

    def upsert_entity_vector(
        self, entity_type: str, entity_id: int, vector_type: str, vector: list[float]
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO entity_vectors(entity_type, entity_id, vector_type, vector_blob, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(entity_type, entity_id, vector_type)
            DO UPDATE SET vector_blob = excluded.vector_blob, updated_at = CURRENT_TIMESTAMP
            """,
            (entity_type, entity_id, vector_type, json.dumps(vector)),
        )
        self.conn.commit()

    def create_match_run(self, run_type: str, input_ref: str | None) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO match_runs(run_type, input_ref)
            VALUES (?, ?)
            """,
            (run_type, input_ref),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def add_match_results(self, run_id: int, target_entity_type: str, results: list[dict[str, Any]]) -> None:
        rows = [
            (
                run_id,
                target_entity_type,
                int(r["target_entity_id"]),
                float(r["semantic_score"]),
                float(r["lexical_score"]),
                float(r["taxonomy_score"]),
                float(r["final_score"]),
                json.dumps(r["explanation"]),
            )
            for r in results
        ]
        self.conn.executemany(
            """
            INSERT INTO match_results(
                run_id, target_entity_type, target_entity_id,
                semantic_score, lexical_score, taxonomy_score, final_score, explanation_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    def add_feedback(
        self, run_id: int, target_entity_id: int, feedback_label: str, comment_optional: str | None
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO feedback(run_id, target_entity_id, feedback_label, comment_optional)
            VALUES (?, ?, ?, ?)
            """,
            (run_id, target_entity_id, feedback_label, comment_optional),
        )
        self.conn.commit()

    def list_feedback(self, limit: int = 200) -> list[sqlite3.Row]:
        cur = self.conn.execute(
            """
            SELECT *
            FROM feedback
            ORDER BY timestamp DESC, feedback_id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cur.fetchall()

    def list_recent_match_results(self, limit: int = 300) -> list[sqlite3.Row]:
        cur = self.conn.execute(
            """
            SELECT mr.*, m.run_type, m.executed_at
            FROM match_results mr
            JOIN match_runs m ON m.run_id = mr.run_id
            ORDER BY mr.result_id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cur.fetchall()
