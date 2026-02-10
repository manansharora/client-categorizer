CREATE TABLE IF NOT EXISTS clients (
    client_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_name TEXT NOT NULL UNIQUE,
    client_type TEXT NOT NULL,
    active_flag INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS client_observations (
    obs_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    obs_type TEXT NOT NULL,
    obs_text TEXT NOT NULL,
    obs_date TEXT NOT NULL,
    source_confidence REAL NOT NULL DEFAULT 1.0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients (client_id)
);

CREATE TABLE IF NOT EXISTS ideas (
    idea_id INTEGER PRIMARY KEY AUTOINCREMENT,
    idea_title TEXT NOT NULL,
    idea_text TEXT NOT NULL,
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS taxonomy_tags (
    tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_family TEXT NOT NULL,
    tag_code TEXT NOT NULL UNIQUE,
    tag_label TEXT NOT NULL,
    taxonomy_version TEXT NOT NULL DEFAULT 'v1'
);

CREATE TABLE IF NOT EXISTS synonyms (
    syn_id INTEGER PRIMARY KEY AUTOINCREMENT,
    surface_form TEXT NOT NULL,
    canonical_form TEXT NOT NULL,
    tag_code_optional TEXT,
    taxonomy_version TEXT NOT NULL DEFAULT 'v1',
    UNIQUE(surface_form, canonical_form, taxonomy_version)
);

CREATE TABLE IF NOT EXISTS entity_tags (
    entity_tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    tag_code TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 1.0,
    origin TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_type, entity_id, tag_code, origin),
    FOREIGN KEY (tag_code) REFERENCES taxonomy_tags (tag_code)
);

CREATE TABLE IF NOT EXISTS entity_vectors (
    entity_vec_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    vector_type TEXT NOT NULL,
    vector_blob TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_type, entity_id, vector_type)
);

CREATE TABLE IF NOT EXISTS match_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type TEXT NOT NULL,
    input_ref TEXT,
    executed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS match_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    target_entity_type TEXT NOT NULL,
    target_entity_id INTEGER NOT NULL,
    semantic_score REAL NOT NULL,
    lexical_score REAL NOT NULL,
    taxonomy_score REAL NOT NULL,
    final_score REAL NOT NULL,
    explanation_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES match_runs (run_id)
);

CREATE TABLE IF NOT EXISTS feedback (
    feedback_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    target_entity_id INTEGER NOT NULL,
    feedback_label TEXT NOT NULL,
    comment_optional TEXT,
    timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES match_runs (run_id)
);

CREATE INDEX IF NOT EXISTS idx_observations_client ON client_observations(client_id);
CREATE INDEX IF NOT EXISTS idx_entity_tags ON entity_tags(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_match_results_run ON match_results(run_id);
CREATE INDEX IF NOT EXISTS idx_feedback_run ON feedback(run_id);
