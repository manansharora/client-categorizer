# Idea Relevance Engine (MVP)

Streamlit + SQLite MVP for semantic matching of ideas.

## What It Does
- Job A: `client -> ranked ideas`
- Job B: `idea/event -> ranked clients`
- Hybrid scoring:
- Semantic (fastText-style embedding similarity)
- Lexical (BM25 relevance)
- Taxonomy overlap (family-weighted)
- Score formula:
- `Final = 0.45 * Semantic + 0.35 * Lexical + 0.20 * Taxonomy`

## Tech Stack
- Python 3.11
- Streamlit
- SQLite
- scikit-learn, rank-bm25, rapidfuzz, gensim

## Project Structure
- `app.py`: Streamlit app entrypoint
- `core/`: domain logic and data access
- `data/schema.sql`: SQLite schema
- `scripts/init_db.py`: initialize and seed DB
- `scripts/ingest_rfq_csv.py`: ingest RFQ CSV into aggregate feature store
- `scripts/ingest_pm_csv.py`: ingest PM sheet into PM entities + PM semantic profiles
- `scripts/reset_and_rebuild_from_rfq.py`: one-shot reset and rebuild workflow
- `tests/`: unit and integration tests

## Setup
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python scripts/init_db.py
```

## Run App
```bash
python -m streamlit run app.py
```

## Run Tests
```bash
pytest -q
```

## Ingest Trade CSV
Use this when you have columns like `Trade`, `Sales Feedback`, `Client`, `Created By`.

```bash
python scripts/ingest_trade_csv.py --csv path\to\your_file.csv
```

Optional flags:
- `--db path\to\db.sqlite`
- `--default-client-type BANK`
- `--observation-type CALL_NOTE`

## Ingest RFQ CSV
Use RFQ structure from `RFQ_description_document.md`.

```bash
python scripts/ingest_rfq_csv.py --csv path\to\rfq_file.csv
```

Optional flags:
- `--db path\to\db.sqlite`
- `--append` to keep existing RFQ client aggregates and update in place.

## Ingest PM CSV
Use a separate PM file with:
- required: `Portfolio Manager`, `Client`
- optional semantic fields: `Style`, `Example trade`, `Tags`
- optional metadata fields: `Salesperson`, `Client Segment`, `Email`
- optional structured fields for PM aggregate features: `CcyPair`, `productType`, `tenor_bucket`, `Client Region`, `clientregion`, `date`

Notes:
- `Ranking` and `Axes` columns are ignored by design.
- PM profile text is built from `Style + Example trade + Tags`.

```bash
python scripts/ingest_pm_csv.py --csv path\to\pm_file.csv
```

Optional flags:
- `--db path\to\db.sqlite`
- `--append` to keep existing PM aggregates and update in place.

Sample PM file:
- `data/sample_pm.csv` uses the sheet-style columns from your PM tracker.
- `Ranking` and `Axes` are present in the file but ignored by ingestion.

## Where PM Shows In Streamlit
PMs are shown on:
- `Match Clients for Idea`
- Run a match, then open each client in the `Feedback` section expander.
- You will see `PM drilldown` with `pm_name`, `pm_score`, semantic/lexical/structured components, and top terms.

Quick local test flow:
```bash
python scripts/ingest_rfq_csv.py --csv data/sample_rfq.csv
python scripts/ingest_pm_csv.py --csv data/sample_pm.csv
python -m streamlit run app.py
```

## Reset And Rebuild (RFQ Canonical Names)
This wipes/recreates the DB, ingests RFQ first (canonical client names), then optional trade and PM files.

```bash
python scripts/reset_and_rebuild_from_rfq.py --rfq-csv path\to\rfq_file.csv --trade-csv path\to\trade_file.csv --pm-csv path\to\pm_file.csv
```

## Retrain FastText Semantic Model
Train a persistent FastText model from current DB corpus (client/PM profiles, ideas, observations):

```bash
python scripts/retrain_semantic_model.py --epochs 30
```

Optional flags:
- `--db path\to\db.sqlite`
- `--model-path path\to\fasttext.model`

## Notes
- Database file is created at `data/client_categorizer.db`.
- Taxonomy and synonym seed data are loaded idempotently.
- Manual tag overrides are supported in UI.
- Matching uses region-first candidate retrieval with fallback expansion.
- Debug logs are written to `logs/client_categorizer.log`.
- FastText training is disabled by default at query time.
- Query-time semantic behavior:
- If `data/fasttext.model` exists, it is loaded and used.
- Otherwise, fallback hash embeddings are used.
- To force per-query FastText retraining (not recommended for latency), set `CLIENT_CATEGORIZER_FASTTEXT_TRAIN=1`.
