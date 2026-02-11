import sqlite3
from pathlib import Path

from core.constants import DATA_DIR, DEFAULT_DB_PATH
from core.seed_data import DEFAULT_SYNONYMS, TAXONOMY_TAGS


def get_connection(db_path: Path | str | None = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def initialize_schema(conn: sqlite3.Connection) -> None:
    schema_path = DATA_DIR / "schema.sql"
    with open(schema_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()


def seed_taxonomy(conn: sqlite3.Connection, taxonomy_version: str = "v1") -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO taxonomy_tags(tag_family, tag_code, tag_label, taxonomy_version)
        VALUES (?, ?, ?, ?)
        """,
        [(family, code, label, taxonomy_version) for family, code, label in TAXONOMY_TAGS],
    )
    conn.commit()


def seed_synonyms(conn: sqlite3.Connection, taxonomy_version: str = "v1") -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO synonyms(surface_form, canonical_form, tag_code_optional, taxonomy_version)
        VALUES (?, ?, ?, ?)
        """,
        [
            (surface, canonical, tag_code, taxonomy_version)
            for surface, canonical, tag_code in DEFAULT_SYNONYMS
        ],
    )
    conn.commit()


def initialize_database(db_path: Path | str | None = None) -> sqlite3.Connection:
    conn = get_connection(db_path)
    initialize_schema(conn)
    seed_taxonomy(conn)
    seed_synonyms(conn)
    return conn
