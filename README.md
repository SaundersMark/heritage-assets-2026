# Heritage Assets 2026

Database and API for tracking UK conditionally exempt heritage assets, with historical change tracking.

## What is this?

HMRC publishes a [register of conditionally exempt heritage assets](http://www.visitukheritage.gov.uk/) - items like paintings, furniture, and manuscripts that have been granted inheritance tax exemption in exchange for public access commitments.

This project:
- Scrapes the HMRC register periodically
- Stores historical snapshots of the data
- Tracks changes over time using SCD Type 2 (slowly changing dimensions)
- Provides a REST API and web UI for exploration

## Quick Start

```bash
# Install dependencies
uv sync

# Run the server
uv run python main.py

# Open browser
open http://localhost:8000/browse
```

## Data

The database contains:
- **Assets**: ~40,000 heritage items with location, category, contact details
- **Raw Snapshots**: Original data as scraped from HMRC
- **Change Events**: Record of additions, updates, and removals over time

Historical data goes back to January 2023.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /assets` | List current assets (with search, filtering, pagination) |
| `GET /assets/{id}` | Get a specific asset |
| `GET /assets/{id}/history` | Get all versions of an asset |
| `GET /assets/{id}/history-summary` | Get summary of changes |
| `GET /changes` | List change events |
| `GET /stats` | Database statistics |
| `GET /browse` | Web UI |

## Scraping

Run an incremental scrape (only fetches assets not scraped in last 7 days):

```bash
uv run python scripts/incremental_scrape.py
```

Options:
- `--skip-days N` - Skip assets scraped within N days (default: 7)
- `--delay N` - Seconds between requests (default: 0.5)
- `--dry-run` - Show what would be scraped without scraping
- `--limit N` - Only scrape N assets (for testing)

## Project Structure

```
├── app/
│   ├── api.py          # FastAPI routes
│   ├── database.py     # SQLAlchemy setup
│   ├── models.py       # Database models
│   ├── schemas.py      # Pydantic schemas
│   ├── scraper.py      # HMRC scraper
│   ├── tidying.py      # Data cleaning/normalisation
│   └── static/
│       └── browse.html # Web UI
├── scripts/
│   ├── import_historical.py   # Import from CSV snapshots
│   └── incremental_scrape.py  # Incremental scraper
├── config.py           # Settings
├── main.py             # Entry point
└── pyproject.toml
```

## License

MIT
