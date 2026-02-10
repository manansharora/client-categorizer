# FX Structuring Client-Idea Relevance Engine (MVP)

Streamlit + SQLite MVP for semantic matching between FX/rates/bonds ideas and clients.

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

## Notes
- Database file is created at `data/client_categorizer.db`.
- Taxonomy and synonym seed data are loaded idempotently.
- Manual tag overrides are supported in UI for clients and ideas.
