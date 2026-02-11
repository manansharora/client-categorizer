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
- `scripts/ingest_pm_csv.py`: ingest PM mapping/preferences CSV
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
Use a separate PM file (for example: `Client`, `PM`, `PreferenceText`, plus optional RFQ-like columns).

```bash
python scripts/ingest_pm_csv.py --csv path\to\pm_file.csv
```

Optional flags:
- `--db path\to\db.sqlite`
- `--append` to keep existing PM aggregates and update in place.

## Reset And Rebuild (RFQ Canonical Names)
This wipes/recreates the DB, ingests RFQ first (canonical client names), then optional trade and PM files.

```bash
python scripts/reset_and_rebuild_from_rfq.py --rfq-csv path\to\rfq_file.csv --trade-csv path\to\trade_file.csv --pm-csv path\to\pm_file.csv
```

## Notes
- Database file is created at `data/client_categorizer.db`.
- Taxonomy and synonym seed data are loaded idempotently.
- Manual tag overrides are supported in UI.
- Matching uses region-first candidate retrieval with fallback expansion.
- Fast semantic mode is default (no per-query FastText training). Set `CLIENT_CATEGORIZER_FASTTEXT_TRAIN=1` to enable training.
