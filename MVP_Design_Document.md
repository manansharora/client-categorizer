# MVP Design Document: Semantic Client-Idea Matching for FX Structuring (Streamlit + SQLite)

## Summary
Build a Python-only internal app that solves two primary jobs for an FX structuring desk.

1. Job A: Given a client plus optional notes/preferences/call logs, return relevant idea categories and suggested idea angles.
2. Job B: Given an event/trade idea, return a ranked list of clients to target, with clear explanations.

This MVP prioritizes semantic quality over UI sophistication and avoids LLMs.  
The recommended semantic core is a hybrid of lexical matching + fastText embeddings + taxonomy overlap.

## Scope
In scope:
- Taxonomy-first classification and matching.
- Streamlit app with presentable input/output screens.
- SQLite persistence.
- Explainable ranking output.
- Feedback capture for future learning (Job C preparation).

Out of scope:
- Graph/client-to-client relationship modeling.
- Regime modeling as a first-class scoring component.
- External API layer.
- Heavy deep learning pipelines.

## Final Architecture (Decision Complete)
1. Presentation layer: `Streamlit`.
2. Persistence: `SQLite` file database.
3. Semantic and classification engine: internal Python modules.
4. Batch utilities: simple scheduled scripts for profile refresh and index refresh.
5. No separate backend API in MVP. App calls domain modules directly.

## Technology Stack
- Python 3.11
- Streamlit
- SQLite
- pandas, numpy
- scikit-learn
- rank-bm25
- fastText embeddings (via `gensim` or `fasttext` package)
- rapidfuzz (for synonym/acronym normalization assistance)

## Domain Taxonomy v1
Use controlled vocabularies with stable IDs. Text input is mapped to tags before scoring.

### Taxonomy Families
1. Client Type
- `BANK`
- `HF_MACRO`
- `HF_RELVAL`
- `HF_SYSTEMATIC`
- `ASSET_MANAGER_LONG_ONLY`
- `ASSET_MANAGER_MULTI_ASSET`
- `PENSION_INSURANCE`
- `CORPORATE_TREASURY`
- `OFFICIAL_SOVEREIGN`

2. Product / Structure
- `FX_SPOT_FWD_NDF`
- `FX_VANILLA_OPTION`
- `FX_EXOTIC_BARRIER_DIGITAL`
- `FX_VOL_STRUCTURE`
- `RATES_SWAP_OIS_BASIS`
- `RATES_OPTIONS`
- `BOND_CASH_SOV_CORP`
- `BOND_DERIV_OVERLAY`
- `CROSS_ASSET_PACKAGE`

3. Expression / Intent
- `HEDGING`
- `YIELD_ENHANCEMENT`
- `DIRECTIONAL_VIEW`
- `VOLATILITY_VIEW`
- `RELATIVE_VALUE`
- `TAIL_PROTECTION`
- `CARRY`
- `CURVE_EXPRESSION`

4. Risk / Payoff Features
- `LOW_DELTA`
- `HIGH_CONVEXITY`
- `KNOCK_IN`
- `KNOCK_OUT`
- `CAPITAL_PROTECTED`
- `LEVERED`
- `PATH_DEPENDENT`
- `DRAWDOWN_SENSITIVE`

5. Tenor Bucket
- `INTRADAY`
- `1W_1M`
- `1M_3M`
- `3M_12M`
- `1Y_PLUS`

6. Market Focus
- `G10_FX`
- `EM_ASIA_FX`
- `EM_LATAM_FX`
- `EMEA_FX`
- `USD_RATES`
- `EUR_RATES`
- `LOCAL_RATES`
- `IG_CREDIT`
- `HY_CREDIT`

7. Theme / Event
- `CENTRAL_BANK`
- `INFLATION`
- `MACRO_DATA`
- `ELECTION_GEO`
- `SUPPLY_DEMAND_BONDS`
- `RISK_ON_OFF`
- `POLICY_DIVERGENCE`

8. Behavioral Preference
- `PREFERS_SIMPLE`
- `LIKES_COMPLEX`
- `FAST_DECISION`
- `EDUCATION_NEEDED`
- `LARGE_TICKET`
- `SMALL_TICKET`

### Taxonomy Governance for MVP
- Taxonomy versioned in DB (`v1`, `v1.1`, etc.).
- Synonyms/acronyms map to canonical taxonomy IDs.
- New tags allowed only through taxonomy manager screen.

## Input Model (Simple, Mostly Optional)
Required fields:
- Client intake: `client_name`, `client_type`
- Idea intake: `idea_title`, `idea_text`

Optional fields:
- Client: call notes, PM preferences, historic trade snippets.
- Idea: proposed structure, target tenor, key terms.
- Manual tag overrides on both entities.

## Data Schema (SQLite)
1. `clients`
- `client_id` (PK)
- `client_name` (unique)
- `client_type`
- `active_flag`
- `created_at`, `updated_at`

2. `client_observations`
- `obs_id` (PK)
- `client_id` (FK)
- `obs_type` (`CALL_NOTE`, `TRADE_NOTE`, `PREFERENCE_NOTE`)
- `obs_text`
- `obs_date`
- `source_confidence` (0-1)

3. `ideas`
- `idea_id` (PK)
- `idea_title`
- `idea_text`
- `created_by`
- `created_at`

4. `taxonomy_tags`
- `tag_id` (PK)
- `tag_family`
- `tag_code` (unique)
- `tag_label`
- `taxonomy_version`

5. `synonyms`
- `syn_id` (PK)
- `surface_form`
- `canonical_form`
- `tag_code_optional`
- `taxonomy_version`

6. `entity_tags`
- `entity_tag_id` (PK)
- `entity_type` (`CLIENT`, `OBSERVATION`, `IDEA`)
- `entity_id`
- `tag_code`
- `confidence` (0-1)
- `origin` (`RULE`, `MODEL`, `MANUAL`)

7. `entity_vectors`
- `entity_vec_id` (PK)
- `entity_type`
- `entity_id`
- `vector_type` (`FASTTEXT`, `TFIDF_SVD`)
- `vector_blob`
- `updated_at`

8. `match_runs`
- `run_id` (PK)
- `run_type` (`JOB_A`, `JOB_B`)
- `input_ref`
- `executed_at`

9. `match_results`
- `result_id` (PK)
- `run_id` (FK)
- `target_entity_type`
- `target_entity_id`
- `semantic_score`
- `lexical_score`
- `taxonomy_score`
- `final_score`
- `explanation_json`

10. `feedback`
- `feedback_id` (PK)
- `run_id` (FK)
- `target_entity_id`
- `feedback_label` (`USEFUL`, `NOT_USEFUL`, `CONTACTED`, `TRADED`)
- `comment_optional`
- `timestamp`

## Semantic Engine Design

### 1) Text Normalization
- Lowercase, punctuation normalization, numeric-token retention (`3m`, `10y`).
- Acronym mapping (`RR`, `KO`, `KI`, `NDF`, etc.) via synonym table.
- Multi-word phrase normalization (`yield enhancement`, `tail hedge`, `carry basket`).
- Stopword policy keeps finance-relevant short tokens.

### 2) Feature Generation
- Lexical representation: BM25 corpus over normalized observation and idea texts.
- Semantic representation: fastText embedding averaged over normalized tokens.
- Optional auxiliary: TF-IDF + SVD retained as fallback diagnostic feature.

### 3) Tag Projection
- Rule-first tag extraction from taxonomy dictionary and phrase patterns.
- Confidence scoring by hit strength and phrase specificity.
- Manual edit in UI can override model/rule output.
- No supervised classifier required for launch.

### 4) Client Profile Composition
- Client profile text is built from latest observations with recency decay.
- Signal-type weights:
- `TRADE_NOTE`: 1.0
- `CALL_NOTE`: 0.8
- `PREFERENCE_NOTE`: 0.7
- Profile vectors are recomputed after each new observation.

## Matching Logic (Core)
Final ranking score:

`Final = 0.45 * Semantic + 0.35 * Lexical + 0.20 * Taxonomy`

### Semantic Score
- Cosine similarity between idea vector and client profile vector.
- Clamp to [0,1] after normalization.

### Lexical Score
- BM25 relevance of idea text against client observation corpus.
- Normalized to [0,1] across candidate set.

### Taxonomy Score
- Weighted overlap on tag families.
- Family weights by default:
- Product 0.30
- Intent 0.25
- Theme 0.20
- Risk 0.10
- Tenor 0.10
- Market Focus 0.05

### Client-Type Weight Adjustment
Apply a deterministic family-weight override by client type:
- `HF_*`: increase `Intent` and `Theme`; decrease `Behavior`.
- `ASSET_MANAGER_*`: increase `Product`, `Risk`, `Tenor`.
- `BANK`: increase `Product`, `Market Focus`.

## Job Flows

### Job A: Client -> Relevant Ideas
1. User selects client and optionally adds new note text.
2. System refreshes profile and inferred tags.
3. System scores all active idea records.
4. Output shows top ideas and top category angles.
5. Output includes reason panel with score components and matched tags.

### Job B: Idea/Event -> Ranked Clients
1. User enters idea/event text and optional suggested structure.
2. System extracts tags and vectors.
3. System scores all active clients.
4. Output shows ranked client list with reasons.
5. User can mark outcomes (useful/contacted/traded/not useful).

## Streamlit App Structure
1. Page: `Client Manager`
- Add/edit clients.
- Add observations.
- View inferred tags and profile summary.

2. Page: `Idea Manager`
- Add/edit idea/event records.
- View inferred tags.

3. Page: `Match Clients for Idea` (Job B)
- Input idea text.
- Return ranked clients + score breakdown + explanation.

4. Page: `Match Ideas for Client` (Job A)
- Select client.
- Return ranked ideas + category suggestions + explanation.

5. Page: `Taxonomy & Synonyms`
- Manage tags, synonyms, acronyms, taxonomy version.

6. Page: `Feedback Review`
- View recent recommendations and captured outcomes.

## Public Interfaces / Contracts
No external HTTP API in MVP.  
Public interfaces are the Streamlit forms and the persisted data contracts.

### UI Input Contracts
- Client create/update contract includes required and optional fields listed above.
- Observation contract supports typed notes with date and confidence.
- Idea create/update contract includes title/text and optional hints.

### Output Contract (both jobs)
Each recommendation row contains:
- `target_name`
- `final_score`
- `semantic_score`
- `lexical_score`
- `taxonomy_score`
- `matched_tags`
- `top_terms`
- `explanation_text`

### Internal Module Interfaces
- `normalize_text(text) -> normalized_text`
- `extract_tags(text, taxonomy, synonyms) -> [(tag, confidence)]`
- `build_vector(text) -> vector`
- `score_match(idea, client_profile) -> score_components`
- `rank_candidates(input_entity) -> ordered_results`

## Evaluation Plan
Use historical desk notes/trade outcomes where available.

Primary metrics:
- Job B: Precision@5, Precision@10
- Job A: Precision@5 for idea relevance
- Explainability acceptance: percentage of recommendations desk users judge as "reason makes sense"
- Latency target: <2 seconds per ranking run on MVP data size

Acceptance thresholds for MVP sign-off:
- Precision@5 at least 0.55 on validation set.
- Mean user usefulness rating at least 3.5/5 in pilot.
- 90th percentile run latency under 2 seconds.

## Test Cases and Scenarios
1. Acronym handling:
- Input includes `KI`, `RR`, `NDF`; system maps correctly to canonical tags.

2. Short note robustness:
- Single-line call note still yields non-empty tag extraction and ranking output.

3. Mixed asset idea:
- Idea mentioning FX + rates produces multi-family tags and sensible ranking.

4. Client-type sensitivity:
- Same idea ranks HF and Asset Manager differently due to type-specific weighting.

5. Contradictory inputs:
- Manual tag override supersedes weak automatic tags.

6. Sparse client data:
- New client with minimal notes still receives output with lower confidence flag.

7. Explainability integrity:
- Every recommendation contains non-empty matched tags and score components.

8. Data persistence:
- Restart app and verify clients, ideas, tags, and feedback remain intact.

## Agent Implementation Sequence (Execution-Ready Checklist)
This checklist is intended for direct execution by another coding agent with minimal interpretation.

### Phase 1: Project Scaffolding and Configuration
1. Create project structure:
- `app/` for Streamlit pages and app entrypoint.
- `core/` for domain logic (normalization, tagging, matching, profiles).
- `data/` for schema SQL, seed files, and local DB.
- `tests/` for unit and integration tests.
2. Add dependency manifest with pinned major versions for:
- `streamlit`, `pandas`, `numpy`, `scikit-learn`, `rank-bm25`, `rapidfuzz`, `gensim` or `fasttext`.
3. Add run entrypoint for local execution.
4. Definition of done:
- App launches from one command.
- Imports resolve with no missing package errors.

### Phase 2: Database and Repository Layer
1. Implement SQLite connection manager with transaction-safe helpers.
2. Create idempotent schema initializer for all required tables in this doc.
3. Add repositories for CRUD:
- Clients.
- Observations.
- Ideas.
- Taxonomy tags.
- Synonyms.
- Entity tags.
- Entity vectors.
- Match runs/results.
- Feedback.
4. Definition of done:
- Fresh DB initializes end-to-end.
- CRUD smoke tests pass for each table family.

### Phase 3: Taxonomy and Seed Data
1. Add taxonomy v1 seed file with all families and tag codes from this document.
2. Add synonym/acronym seed file for FX/rates/bonds vocabulary.
3. Implement idempotent seed loader.
4. Definition of done:
- Re-running seed command does not duplicate rows.
- All expected tags are queryable by family.

### Phase 4: Text Normalization and Tag Projection
1. Implement normalization pipeline:
- Lowercasing.
- Punctuation cleanup.
- Tenor token preservation (`1m`, `3m`, `10y`).
- Synonym and acronym canonicalization.
2. Implement rule-first tag extraction using taxonomy terms and synonym map.
3. Implement confidence scoring for extracted tags.
4. Persist auto-generated tags into `entity_tags` with `origin=RULE`.
5. Definition of done:
- Known acronyms (`KI`, `KO`, `RR`, `NDF`) map correctly.
- Sample domain texts produce non-empty, sensible tags.

### Phase 5: Semantic and Lexical Feature Layer
1. Implement BM25 corpus and index generation for:
- Client profile text and/or observation corpora.
- Idea text corpora.
2. Implement fastText-based embedding builder:
- Token vector lookup.
- Document embedding by weighted average.
3. Store vectors in `entity_vectors` as serialized blobs.
4. Add fallback behavior for empty or OOV-heavy text.
5. Definition of done:
- Semantic similarity returns stable numeric output in [0,1].
- Lexical scoring returns normalized output in [0,1].

### Phase 6: Client Profile Builder
1. Implement profile aggregation from observations with signal weights:
- `TRADE_NOTE=1.0`, `CALL_NOTE=0.8`, `PREFERENCE_NOTE=0.7`.
2. Add recency decay weighting to text and tag aggregation.
3. Produce profile outputs:
- Aggregated text.
- Aggregated and inferred tags.
- Profile embedding.
4. Definition of done:
- Updating observations refreshes profile outputs deterministically.

### Phase 7: Matching Engine (Job A and Job B)
1. Implement component scores:
- Semantic score (cosine similarity).
- Lexical score (BM25 normalized).
- Taxonomy score (family-weighted overlap).
2. Implement final score formula:
- `Final = 0.45 * Semantic + 0.35 * Lexical + 0.20 * Taxonomy`.
3. Implement client-type family-weight overrides:
- `HF_*`: boost intent and theme.
- `ASSET_MANAGER_*`: boost product, risk, tenor.
- `BANK`: boost product and market focus.
4. Implement explanation payload generation:
- Matched tags.
- Top contributing terms.
- Component score breakdown.
5. Persist run metadata and ranked results into `match_runs` and `match_results`.
6. Definition of done:
- Given a known test case, rankings are reproducible.
- Every output row includes non-empty explanation fields.

### Phase 8: Streamlit Application Pages
1. Implement `Client Manager` page:
- Create and edit clients.
- Add observations.
- View inferred tags and profile summary.
2. Implement `Idea Manager` page:
- Create and edit ideas.
- View inferred tags.
3. Implement `Match Clients for Idea` page (Job B).
4. Implement `Match Ideas for Client` page (Job A).
5. Implement `Taxonomy & Synonyms` page:
- Add and update tags and synonyms.
- Enforce taxonomy version tracking.
6. Implement `Feedback Review` page:
- Capture and display usefulness/contact/trade outcomes.
7. Definition of done:
- All pages load without error.
- Core user path for Job A and Job B is functional from UI only.

### Phase 9: Testing and Validation
1. Unit tests:
- Normalization.
- Synonym mapping.
- Tag extraction.
- Score calculations.
2. Integration tests:
- Job A flow end-to-end with seed data.
- Job B flow end-to-end with seed data.
- Persistence across app restart.
3. Add test fixtures with representative FX/rates/bonds examples.
4. Definition of done:
- Test suite passes locally.
- Key acceptance scenarios from this doc are covered.

### Phase 10: Documentation and Handoff
1. Add `README` with:
- Setup instructions.
- DB init and seed commands.
- Run command.
- Test command.
2. Document architecture and module map.
3. Document known limitations and optional next-step backlog.
4. Definition of done:
- A new engineer can run app and tests from docs without manual guidance.

## Assumptions and Defaults Chosen
- No external API is required for MVP.
- SQLite is sufficient for scale and simplicity in MVP.
- fastText + BM25 + taxonomy overlap is the semantic baseline.
- Regime signals are excluded from scoring in v1.
- Client-to-client relationship modeling is excluded.
- Feedback is captured but not used to retrain in v1.
- Tag extraction is rule-first with optional manual override, not supervised ML at launch.
- Default score blend is fixed at `0.45/0.35/0.20` unless pilot calibration proves otherwise.

