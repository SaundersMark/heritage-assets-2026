# Deployment Guide

## Prerequisites

1. Python 3.11+ installed
2. cloudflared configured for tunnel access
3. systemd (Linux)

## Installation

### 1. Clone and setup

```bash
cd /home/mark/code_26/heritage_assets
uv sync
```

### 2. Import historical data

```bash
uv run python scripts/import_historical.py \
    --data-dir /home/mark/code/tax/heritage_assets/data/historic_downloads
```

### 3. Run initial scrape to get current data

```bash
uv run python -c "from app.scraper import run_scrape_and_update; run_scrape_and_update()"
```

### 4. Install systemd services

```bash
# Copy service files
sudo cp deploy/heritage-api.service /etc/systemd/system/
sudo cp deploy/heritage-scrape.service /etc/systemd/system/
sudo cp deploy/heritage-scrape.timer /etc/systemd/system/

# Edit heritage-api.service to set your API key
sudo nano /etc/systemd/system/heritage-api.service

# Reload and enable
sudo systemctl daemon-reload
sudo systemctl enable heritage-api
sudo systemctl start heritage-api

# Enable weekly scrape timer
sudo systemctl enable heritage-scrape.timer
sudo systemctl start heritage-scrape.timer
```

### 5. Configure cloudflared

Add to your cloudflared config (e.g., `~/.cloudflared/config.yml`):

```yaml
ingress:
  - hostname: heritage.yourdomain.com
    service: http://localhost:8000
```

## Verification

### Check API is running

```bash
curl http://localhost:8000/health
curl http://localhost:8000/stats
```

### Check timer status

```bash
systemctl status heritage-scrape.timer
systemctl list-timers heritage-scrape.timer
```

### Manual scrape

```bash
# Via systemd
sudo systemctl start heritage-scrape

# Or directly
uv run python -c "from app.scraper import run_scrape_and_update; run_scrape_and_update()"
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /assets` | Current assets (filtering, pagination) |
| `GET /assets/{id}` | Single asset |
| `GET /assets/{id}/history` | Asset version history |
| `GET /assets/as-of/{date}` | Historical state |
| `GET /changes` | Change events |
| `GET /changes/{date1}/{date2}` | Changes between dates |
| `GET /raw-snapshots` | Available snapshots |
| `GET /raw-snapshots/{date}` | Raw data for date |
| `GET /stats` | Overall statistics |
| `POST /scrape` | Trigger scrape (needs API key) |
| `GET /health` | Health check |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HERITAGE_DATABASE_URL` | `sqlite:///heritage_assets.db` | Database connection string |
| `HERITAGE_API_HOST` | `0.0.0.0` | API bind address |
| `HERITAGE_API_PORT` | `8000` | API port |
| `HERITAGE_API_KEY` | (required) | API key for /scrape endpoint |
