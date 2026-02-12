CREATE TABLE IF NOT EXISTS clients (
    client_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_name TEXT NOT NULL UNIQUE,
    client_type TEXT NOT NULL,
    active_flag INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS client_aliases (
    alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    alias_name TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL DEFAULT 'MANUAL',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients (client_id)
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

CREATE TABLE IF NOT EXISTS client_pms (
    pm_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    pm_name TEXT NOT NULL,
    active_flag INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(client_id, pm_name),
    FOREIGN KEY (client_id) REFERENCES clients (client_id)
);

CREATE TABLE IF NOT EXISTS pm_observations (
    pm_obs_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pm_id INTEGER NOT NULL,
    obs_type TEXT NOT NULL,
    obs_text TEXT NOT NULL,
    obs_date TEXT NOT NULL,
    source_confidence REAL NOT NULL DEFAULT 1.0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pm_id) REFERENCES client_pms (pm_id)
);

CREATE TABLE IF NOT EXISTS pm_metadata (
    pm_meta_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pm_id INTEGER NOT NULL UNIQUE,
    salesperson TEXT,
    client_segment TEXT,
    email TEXT,
    source_sheet TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pm_id) REFERENCES client_pms (pm_id)
);

CREATE TABLE IF NOT EXISTS ideas (
    idea_id INTEGER PRIMARY KEY AUTOINCREMENT,
    idea_title TEXT NOT NULL,
    idea_text TEXT NOT NULL,
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rfq_ingest_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    rows_read INTEGER NOT NULL DEFAULT 0,
    rows_valid INTEGER NOT NULL DEFAULT 0,
    rows_skipped INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    error_summary TEXT
);

CREATE TABLE IF NOT EXISTS rfq_entity_feature_agg (
    feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    region TEXT NOT NULL,
    country TEXT,
    feature_kind TEXT NOT NULL,
    ccy_pair TEXT,
    product_type TEXT,
    tenor_bucket TEXT,
    trade_count INTEGER NOT NULL DEFAULT 0,
    hit_notional_sum_m REAL NOT NULL DEFAULT 0.0,
    last_trade_date TEXT,
    score_30d REAL NOT NULL DEFAULT 0.0,
    score_90d REAL NOT NULL DEFAULT 0.0,
    score_365d REAL NOT NULL DEFAULT 0.0,
    recency_score REAL NOT NULL DEFAULT 0.0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_type, entity_id, region, country, feature_kind, ccy_pair, product_type, tenor_bucket)
);

CREATE TABLE IF NOT EXISTS entity_profile_cache (
    cache_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    profile_text TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_type, entity_id)
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
CREATE INDEX IF NOT EXISTS idx_client_alias_name ON client_aliases(alias_name);
CREATE INDEX IF NOT EXISTS idx_client_alias_client ON client_aliases(client_id);
CREATE INDEX IF NOT EXISTS idx_client_pms_client ON client_pms(client_id, active_flag);
CREATE INDEX IF NOT EXISTS idx_pm_obs_pm ON pm_observations(pm_id);
CREATE INDEX IF NOT EXISTS idx_pm_meta_pm ON pm_metadata(pm_id);
CREATE INDEX IF NOT EXISTS idx_entity_tags ON entity_tags(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_match_results_run ON match_results(run_id);
CREATE INDEX IF NOT EXISTS idx_feedback_run ON feedback(run_id);
CREATE INDEX IF NOT EXISTS idx_rfq_entity_kind ON rfq_entity_feature_agg(entity_type, feature_kind);
CREATE INDEX IF NOT EXISTS idx_rfq_entity_region ON rfq_entity_feature_agg(entity_type, region, feature_kind, ccy_pair, product_type, recency_score DESC);
CREATE INDEX IF NOT EXISTS idx_rfq_entity_country ON rfq_entity_feature_agg(entity_type, country, feature_kind, ccy_pair, product_type, recency_score DESC);
CREATE INDEX IF NOT EXISTS idx_rfq_entity_lookup ON rfq_entity_feature_agg(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_profile_cache_entity ON entity_profile_cache(entity_type, entity_id);
