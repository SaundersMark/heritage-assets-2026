# Heritage Assets Testing Guide

Pre-deployment tests to verify the system works correctly.

## 1. Basic Connectivity Tests

### 1.1 Database Connection
```bash
uv run python -c "
from app.database import engine, get_session
from app.models import Asset, RawSnapshot, ChangeEvent

with get_session() as session:
    assets = session.query(Asset).filter(Asset.valid_until.is_(None)).count()
    versions = session.query(Asset).count()
    raw = session.query(RawSnapshot).count()
    changes = session.query(ChangeEvent).count()

print(f'Database OK:')
print(f'  Current assets: {assets}')
print(f'  Total versions: {versions}')
print(f'  Raw snapshots: {raw}')
print(f'  Change events: {changes}')
"
```

### 1.2 HMRC Website Connectivity
```bash
uv run python -c "
from app.scraper import HMRCScraper

with HMRCScraper() as scraper:
    summaries = scraper.scrape_summaries()
    print(f'HMRC connection OK: {len(summaries)} assets found')
    if summaries:
        print(f'Sample: {summaries[0].unique_id} - {summaries[0].description[:60]}...')
"
```

## 2. API Endpoint Tests

### 2.1 Start API Server (in separate terminal)
```bash
uv run python main.py
```

### 2.2 Test All Endpoints (in another terminal)
```bash
# Health check
curl -s http://localhost:8000/health | python3 -m json.tool

# Statistics
curl -s http://localhost:8000/stats | python3 -m json.tool

# List assets (first page)
curl -s "http://localhost:8000/assets?page_size=5" | python3 -m json.tool

# Filter by location
curl -s "http://localhost:8000/assets?location=LONDON&page_size=3" | python3 -m json.tool

# Filter by category
curl -s "http://localhost:8000/assets?category=Paintings&page_size=3" | python3 -m json.tool

# Search
curl -s "http://localhost:8000/assets?search=Churchill&page_size=5" | python3 -m json.tool

# Single asset
curl -s http://localhost:8000/assets/5953 | python3 -m json.tool

# Asset history
curl -s http://localhost:8000/assets/5953/history | python3 -m json.tool

# Historical state (as of June 2023)
curl -s "http://localhost:8000/assets/as-of/2023-06-01?page_size=3" | python3 -m json.tool

# Changes list
curl -s "http://localhost:8000/changes?page_size=5" | python3 -m json.tool

# Changes by type
curl -s "http://localhost:8000/changes?change_type=added&page_size=5" | python3 -m json.tool

# Changes between dates
curl -s "http://localhost:8000/changes/2024-01-01/2024-03-31" | python3 -m json.tool

# List snapshots
curl -s http://localhost:8000/raw-snapshots | python3 -m json.tool

# Raw snapshot data
curl -s "http://localhost:8000/raw-snapshots/2024-03-02?page_size=2" | python3 -m json.tool
```

## 3. Data Integrity Tests

### 3.1 SCD2 Verification
```bash
uv run python -c "
from app.database import get_session
from app.models import Asset

with get_session() as session:
    # Check for duplicate current versions (should be 0)
    from sqlalchemy import func
    dupes = session.query(Asset.unique_id, func.count(Asset.id))\
        .filter(Asset.valid_until.is_(None))\
        .group_by(Asset.unique_id)\
        .having(func.count(Asset.id) > 1)\
        .all()

    if dupes:
        print(f'ERROR: {len(dupes)} assets have multiple current versions!')
        for uid, count in dupes[:5]:
            print(f'  {uid}: {count} versions')
    else:
        print('OK: No duplicate current versions')

    # Check valid_from < valid_until for closed versions
    invalid = session.query(Asset)\
        .filter(Asset.valid_until.isnot(None))\
        .filter(Asset.valid_from >= Asset.valid_until)\
        .count()

    if invalid:
        print(f'ERROR: {invalid} versions have valid_from >= valid_until')
    else:
        print('OK: All date ranges are valid')
"
```

### 3.2 Tidying Verification
```bash
uv run python -c "
from app.database import get_session
from app.models import Asset

with get_session() as session:
    # Check phone number normalization
    assets_with_phone = session.query(Asset)\
        .filter(Asset.valid_until.is_(None))\
        .filter(Asset.telephone.isnot(None))\
        .limit(10).all()

    print('Phone number samples (should be digits only):')
    for a in assets_with_phone[:5]:
        print(f'  {a.unique_id}: {a.telephone}')

    # Check postcode extraction
    assets_with_postcode = session.query(Asset)\
        .filter(Asset.valid_until.is_(None))\
        .filter(Asset.address_postcode.isnot(None))\
        .limit(10).all()

    print('\\nPostcode samples:')
    for a in assets_with_postcode[:5]:
        print(f'  {a.unique_id}: {a.address_postcode} ({a.address_city})')
"
```

### 3.3 Historical Query Accuracy
```bash
uv run python -c "
from datetime import date
from app.database import get_session
from app.models import Asset, SnapshotMetadata

with get_session() as session:
    # Get snapshot dates
    snapshots = session.query(SnapshotMetadata).order_by(SnapshotMetadata.snapshot_date).all()
    print('Available snapshots:')
    for s in snapshots:
        print(f'  {s.snapshot_date}: {s.asset_count} assets')

    # Query as-of each snapshot date should match snapshot count
    print('\\nAs-of queries (should match snapshot counts):')
    for s in snapshots:
        count = session.query(Asset).filter(
            Asset.valid_from <= s.snapshot_date,
            (Asset.valid_until.is_(None)) | (Asset.valid_until > s.snapshot_date)
        ).count()
        match = '✓' if count == s.asset_count else '✗'
        print(f'  {s.snapshot_date}: {count} (expected {s.asset_count}) {match}')
"
```

## 4. Sample Scrape Test

### 4.1 Scrape a Few Assets (Detail Fetch Test)
```bash
uv run python -c "
from app.scraper import HMRCScraper

with HMRCScraper() as scraper:
    # Get summaries
    summaries = scraper.scrape_summaries()
    print(f'Found {len(summaries)} assets')

    # Scrape details for just 5 assets
    test_ids = [s.unique_id for s in summaries[:5]]
    print(f'\\nFetching details for 5 assets: {test_ids}')

    details = scraper.scrape_details_batch(test_ids, max_workers=2)

    for uid, d in details.items():
        print(f'\\n{uid}:')
        print(f'  Owner: {d.owner_id}')
        print(f'  Contact: {d.contact_name}')
        print(f'  Address: {d.contact_address[:60] if d.contact_address else None}...')
        print(f'  Phone: {d.telephone_no}')
"
```

### 4.2 Test Tidying on Live Data
```bash
uv run python -c "
from app.scraper import HMRCScraper
from app.tidying import tidy_raw_record

with HMRCScraper() as scraper:
    summaries = scraper.scrape_summaries()[:3]

    for summary in summaries:
        details = scraper.scrape_details(summary.unique_id)

        # Build raw record
        raw = {
            'uniqueID': summary.unique_id,
            'description': summary.description,
            'location': summary.location,
            'category': summary.category,
            'owner_id': details.owner_id if details else None,
            'access_details': details.access_details if details else None,
            'contact_name': details.contact_name if details else None,
            'contact_address': details.contact_address if details else None,
            'telephone_no': details.telephone_no if details else None,
            'fax_no': details.fax_no if details else None,
            'email': details.email if details else None,
            'website': details.website if details else None,
        }

        # Tidy it
        tidied = tidy_raw_record(raw)

        print(f'\\n=== {tidied.unique_id} ===')
        print(f'Raw address: {raw[\"contact_address\"]}')
        print(f'Tidied:')
        print(f'  Line 1: {tidied.contact.address_line1}')
        print(f'  Line 2: {tidied.contact.address_line2}')
        print(f'  City: {tidied.contact.address_city}')
        print(f'  Postcode: {tidied.contact.address_postcode}')
        print(f'  Phone: {tidied.contact.telephone}')
"
```

## 5. Full Scrape Dry Run

### 5.1 Check What Would Change (Without Saving)
```bash
uv run python -c "
from app.scraper import HMRCScraper
from app.database import get_session
from app.models import Asset
from app.tidying import tidy_raw_record

# Get current state
with get_session() as session:
    current_ids = set(
        a.unique_id for a in
        session.query(Asset.unique_id).filter(Asset.valid_until.is_(None)).all()
    )
print(f'Current database: {len(current_ids)} assets')

# Scrape HMRC (summaries only - fast)
with HMRCScraper() as scraper:
    summaries = scraper.scrape_summaries()
    new_ids = set(s.unique_id for s in summaries)

print(f'HMRC website: {len(new_ids)} assets')

# Compare
added = new_ids - current_ids
removed = current_ids - new_ids
common = new_ids & current_ids

print(f'\\nChanges detected:')
print(f'  Would be ADDED: {len(added)}')
print(f'  Would be REMOVED: {len(removed)}')
print(f'  Unchanged/updated: {len(common)}')

if added:
    print(f'\\nSample additions (first 5):')
    for uid in list(added)[:5]:
        s = next(s for s in summaries if s.unique_id == uid)
        print(f'  {uid}: {s.description[:50]}...')

if removed:
    print(f'\\nSample removals (first 5):')
    for uid in list(removed)[:5]:
        print(f'  {uid}')
"
```

## 6. Performance Tests

### 6.1 Query Performance
```bash
uv run python -c "
import time
from app.database import get_session
from app.models import Asset, ChangeEvent

with get_session() as session:
    # Current assets query
    start = time.time()
    count = session.query(Asset).filter(Asset.valid_until.is_(None)).count()
    print(f'Current assets count: {time.time()-start:.3f}s ({count} assets)')

    # Filtered query
    start = time.time()
    count = session.query(Asset)\
        .filter(Asset.valid_until.is_(None))\
        .filter(Asset.location == 'LONDON')\
        .count()
    print(f'London assets count: {time.time()-start:.3f}s ({count} assets)')

    # Historical query
    from datetime import date
    start = time.time()
    count = session.query(Asset).filter(
        Asset.valid_from <= date(2023, 6, 1),
        (Asset.valid_until.is_(None)) | (Asset.valid_until > date(2023, 6, 1))
    ).count()
    print(f'As-of 2023-06-01: {time.time()-start:.3f}s ({count} assets)')

    # Recent changes
    start = time.time()
    changes = session.query(ChangeEvent)\
        .filter(ChangeEvent.change_date >= date(2024, 1, 1))\
        .count()
    print(f'2024 changes count: {time.time()-start:.3f}s ({changes} changes)')
"
```

## 7. Expected Results Summary

After running all tests, you should see:

| Test | Expected Result |
|------|-----------------|
| Database connection | 36,167 current assets, 72,039 versions |
| HMRC connectivity | ~39,698 assets found (as of Feb 2026) |
| Duplicate check | 0 duplicates |
| Date range check | All valid |
| Phone normalization | Digits only (e.g., "01732462100") |
| Historical queries | Counts match snapshot metadata exactly |
| Query performance | All queries <25ms |
| Scrape comparison | ~3,891 new, ~360 removed since March 2024 |

### Actual Test Results (Feb 2026)

```
=== Database ===
Current assets: 36,167
Total versions: 72,039
Raw snapshots: 108,102
Change events: 72,333

=== SCD2 Integrity ===
OK: No duplicate current versions
OK: All date ranges are valid

=== Historical Accuracy ===
2023-01-25: 35,753 (expected 35,753) ✓
2023-09-30: 36,182 (expected 36,182) ✓
2024-03-02: 36,167 (expected 36,167) ✓

=== HMRC Website ===
Current: 39,698 assets
Would add: 3,891
Would remove: 360

=== Query Performance ===
Current assets count: 0.007s
London filter: 0.007s
As-of query: 0.021s
Changes query: 0.001s

=== API Endpoints ===
✓ Health, Stats, Assets, Filtering, Search
✓ Single asset, History, As-of
✓ Changes, Snapshots
```

## 8. Running a Full Live Scrape

**Warning**: This takes 30-60 minutes and makes ~40,000 HTTP requests.

```bash
# Only run when ready to update the database
uv run python -c "
from app.scraper import run_scrape_and_update
result = run_scrape_and_update()
print(result)
"
```

Or via the API (requires API key):
```bash
curl -X POST http://localhost:8000/scrape \
  -H "X-API-Key: your-api-key" | python3 -m json.tool
```
