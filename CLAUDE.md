# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A FastAPI application that scrapes HMRC's conditionally exempt heritage assets register, stores historical snapshots, tracks changes over time using SCD Type 2, and provides a REST API + web UI for exploration. The database contains ~40k "Works of Art" assets and ~550 "Land & Buildings / Collections" items.

## Commands

```bash
uv sync                          # Install dependencies
uv run python main.py             # Start server on http://localhost:8000
uv run ruff check app/ scripts/   # Lint
uv run ruff format app/ scripts/  # Format

# Scraping & data processing
uv run python scripts/incremental_scrape.py --limit 10 --dry-run  # Test scrape
uv run python scripts/process_snapshot.py 2026-03-01               # Process raw→Asset SCD2
uv run python scripts/scrape_land_buildings.py                     # Scrape L&B + Collections
```

No automated test suite exists. Manual testing documented in `TESTING.md`.

## Architecture

**Framework:** FastAPI + SQLAlchemy ORM + SQLite (single file: `heritage_assets.db`, ~246MB)

**Data pipeline:**
1. `app/scraper.py` — `HMRCScraper` fetches summaries + details from HMRC (parallel with ThreadPoolExecutor, retry with backoff)
2. `RawSnapshot` table — preserves exact scraped data as JSON
3. `app/tidying.py` — `tidy_raw_record()` normalizes addresses, phone numbers, postcodes
4. `scripts/process_snapshot.py` — SCD2 logic: compares old vs new, closes old versions (`valid_until`), creates new versions
5. `ChangeEvent` table — logs what changed and when

**SCD Type 2 queries:**
- Current assets: `WHERE valid_until IS NULL`
- As-of date X: `WHERE valid_from <= X AND (valid_until IS NULL OR valid_until > X)`

**Key models** (`app/models.py`): `Asset`, `RawSnapshot`, `ChangeEvent`, `LandBuilding`, `SnapshotMetadata`

**Search:** FTS5 virtual tables (`assets_fts`, `land_buildings_fts`) with BM25 ranking. Rebuilt via `rebuild_fts_index(engine)` / `rebuild_land_buildings_fts_index(engine)`.

**Frontend:** Single-file vanilla HTML/JS at `app/static/browse.html` — no build step, served by FastAPI at `/browse`.

**Config:** Pydantic Settings in `config.py`, env vars prefixed `HERITAGE_` (e.g. `HERITAGE_DATABASE_URL`, `HERITAGE_API_PORT`). Reads `.env` file.

## Key Files

- `main.py` — entry point (starts uvicorn)
- `app/api.py` — all FastAPI routes
- `app/models.py` — SQLAlchemy models + FTS5 table creation
- `app/schemas.py` — Pydantic response schemas
- `app/database.py` — SQLAlchemy engine/session setup
- `app/scraper.py` — HMRC scraper with parallel fetching
- `app/tidying.py` — data cleaning (phone normalization, UK postcode extraction, address parsing)
- `config.py` — settings with env var loading
- `data/collections.csv` — owner/collection name mappings

## Conventions

- Python 3.11+, ruff for linting (line-length 100, E/F/I/W rules)
- httpx for HTTP requests (not requests)
- SQLAlchemy ORM exclusively (raw SQL only for FTS5 queries)
- Database sessions via `get_db()` dependency injection
- No migration system — schema defined in Python, `create_tables(engine)` is idempotent
- SQLite has no native bool — uses Integer for boolean fields (e.g. `has_map`)
- Phone numbers normalized to digits only (UK format, converts +44/0044 to 0)
- Server runs on port 8000 by default
