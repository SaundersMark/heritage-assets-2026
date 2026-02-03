"""FastAPI REST API for Heritage Assets"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Security
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Asset, ChangeEvent, LandBuilding, RawSnapshot, SnapshotMetadata
from app.schemas import (
    AssetHistoryResponse,
    AssetResponse,
    ChangeEventResponse,
    PaginatedResponse,
    RawSnapshotResponse,
    ScrapeResponse,
    SnapshotMetadataResponse,
    StatsResponse,
)
from config import settings

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Heritage Assets API",
    description="REST API for UK Heritage Assets database with SCD Type 2 change tracking",
    version="2.0.0",
)

# Mount static files
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Security(api_key_header)):
    """Verify API key for protected endpoints"""
    if not settings.api_key:
        raise HTTPException(status_code=500, detail="API key not configured")
    if api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


# -----------------------------------------------------------------------------
# Assets endpoints
# -----------------------------------------------------------------------------


@app.get("/assets", response_model=PaginatedResponse)
def list_assets(
    location: Optional[str] = None,
    category: Optional[str] = None,
    search: Optional[str] = None,
    unique_id: Optional[str] = None,
    owner_id: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    List current assets with optional filtering and pagination.

    Only returns current versions (valid_until IS NULL).
    Uses FTS5 for fast text search when search parameter is provided.
    Supports exact match on unique_id or owner_id.
    """
    # Handle unique_id exact match
    if unique_id:
        query = db.query(Asset).filter(
            Asset.valid_until.is_(None),
            Asset.unique_id == unique_id
        )
        items = query.all()
        return PaginatedResponse(
            items=[AssetResponse.model_validate(a) for a in items],
            total=len(items),
            page=1,
            page_size=page_size,
            pages=1 if items else 0,
        )

    # Handle owner_id filter
    if owner_id:
        query = db.query(Asset).filter(
            Asset.valid_until.is_(None),
            Asset.owner_id == owner_id
        )
        if location:
            query = query.filter(Asset.location.ilike(f"%{location}%"))
        if category:
            query = query.filter(Asset.category.ilike(f"%{category}%"))

        total = query.count()
        pages = (total + page_size - 1) // page_size
        items = (
            query.order_by(Asset.unique_id)
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )
        return PaginatedResponse(
            items=[AssetResponse.model_validate(a) for a in items],
            total=total,
            page=page,
            page_size=page_size,
            pages=pages,
        )

    if search:
        # Use FTS5 for text search - much faster than ILIKE
        # Escape special FTS5 characters and add prefix matching
        search_term = search.replace('"', '""')
        # Use * for prefix matching (e.g., "paint" matches "painting")
        fts_query = f'"{search_term}"*'

        # Get matching asset IDs from FTS5, ordered by relevance (bm25)
        fts_sql = text("""
            SELECT unique_id, bm25(assets_fts) as rank
            FROM assets_fts
            WHERE assets_fts MATCH :query
            ORDER BY rank
        """)
        fts_results = db.execute(fts_sql, {"query": fts_query}).fetchall()
        matching_ids = [r[0] for r in fts_results]

        if not matching_ids:
            return PaginatedResponse(items=[], total=0, page=page, page_size=page_size, pages=0)

        # Build query for matched assets
        query = db.query(Asset).filter(
            Asset.valid_until.is_(None),
            Asset.unique_id.in_(matching_ids)
        )

        if location:
            query = query.filter(Asset.location.ilike(f"%{location}%"))
        if category:
            query = query.filter(Asset.category.ilike(f"%{category}%"))

        total = query.count()
        pages = (total + page_size - 1) // page_size

        # Preserve FTS5 ranking order
        items = query.all()
        id_to_rank = {r[0]: i for i, r in enumerate(fts_results)}
        items.sort(key=lambda a: id_to_rank.get(a.unique_id, 999999))
        items = items[(page - 1) * page_size : page * page_size]
    else:
        # No search - use standard query
        query = db.query(Asset).filter(Asset.valid_until.is_(None))

        if location:
            query = query.filter(Asset.location.ilike(f"%{location}%"))
        if category:
            query = query.filter(Asset.category.ilike(f"%{category}%"))

        total = query.count()
        pages = (total + page_size - 1) // page_size

        items = (
            query.order_by(Asset.unique_id)
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )

    return PaginatedResponse(
        items=[AssetResponse.model_validate(a) for a in items],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )


@app.get("/assets/{unique_id}", response_model=AssetResponse)
def get_asset(unique_id: str, db: Session = Depends(get_db)):
    """Get current version of a specific asset"""
    asset = (
        db.query(Asset)
        .filter(Asset.unique_id == unique_id, Asset.valid_until.is_(None))
        .first()
    )
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    return asset


@app.get("/assets/{unique_id}/history", response_model=AssetHistoryResponse)
def get_asset_history(unique_id: str, db: Session = Depends(get_db)):
    """Get all versions of an asset over time"""
    versions = (
        db.query(Asset)
        .filter(Asset.unique_id == unique_id)
        .order_by(Asset.valid_from.desc())
        .all()
    )

    if not versions:
        raise HTTPException(status_code=404, detail="Asset not found")

    current = next((v for v in versions if v.valid_until is None), None)
    history = [v for v in versions if v.valid_until is not None]

    return AssetHistoryResponse(
        unique_id=unique_id,
        current=AssetResponse.model_validate(current) if current else None,
        history=[AssetResponse.model_validate(v) for v in history],
    )


@app.get("/assets/as-of/{target_date}", response_model=PaginatedResponse)
def get_assets_as_of(
    target_date: date,
    location: Optional[str] = None,
    category: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Get state of all assets at a specific historical date.

    Returns assets where: valid_from <= date AND (valid_until IS NULL OR valid_until > date)
    """
    query = db.query(Asset).filter(
        Asset.valid_from <= target_date,
        (Asset.valid_until.is_(None)) | (Asset.valid_until > target_date),
    )

    if location:
        query = query.filter(Asset.location.ilike(f"%{location}%"))
    if category:
        query = query.filter(Asset.category.ilike(f"%{category}%"))

    total = query.count()
    pages = (total + page_size - 1) // page_size

    items = (
        query.order_by(Asset.unique_id)
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse(
        items=[AssetResponse.model_validate(a) for a in items],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )


# -----------------------------------------------------------------------------
# Changes endpoints
# -----------------------------------------------------------------------------


@app.get("/changes", response_model=PaginatedResponse)
def list_changes(
    change_type: Optional[str] = None,
    since: Optional[date] = None,
    until: Optional[date] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """List change events with optional filtering"""
    query = db.query(ChangeEvent)

    if change_type:
        query = query.filter(ChangeEvent.change_type == change_type)
    if since:
        query = query.filter(ChangeEvent.change_date >= since)
    if until:
        query = query.filter(ChangeEvent.change_date <= until)

    total = query.count()
    pages = (total + page_size - 1) // page_size

    items = (
        query.order_by(ChangeEvent.change_date.desc(), ChangeEvent.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse(
        items=[ChangeEventResponse.model_validate(e) for e in items],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )


@app.get("/changes/{date1}/{date2}", response_model=list[ChangeEventResponse])
def get_changes_between(date1: date, date2: date, db: Session = Depends(get_db)):
    """Get all changes between two dates"""
    start_date = min(date1, date2)
    end_date = max(date1, date2)

    changes = (
        db.query(ChangeEvent)
        .filter(ChangeEvent.change_date >= start_date, ChangeEvent.change_date <= end_date)
        .order_by(ChangeEvent.change_date, ChangeEvent.id)
        .all()
    )

    return [ChangeEventResponse.model_validate(c) for c in changes]


# -----------------------------------------------------------------------------
# Raw snapshots endpoints
# -----------------------------------------------------------------------------


@app.get("/raw-snapshots", response_model=list[SnapshotMetadataResponse])
def list_snapshots(db: Session = Depends(get_db)):
    """List all available snapshots"""
    snapshots = (
        db.query(SnapshotMetadata).order_by(SnapshotMetadata.snapshot_date.desc()).all()
    )
    return [SnapshotMetadataResponse.model_validate(s) for s in snapshots]


@app.get("/raw-snapshots/{target_date}", response_model=PaginatedResponse)
def get_raw_snapshot(
    target_date: date,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Get raw snapshot data for a specific date"""
    query = db.query(RawSnapshot).filter(RawSnapshot.snapshot_date == target_date)

    total = query.count()
    if total == 0:
        raise HTTPException(status_code=404, detail="Snapshot not found")

    pages = (total + page_size - 1) // page_size

    items = (
        query.order_by(RawSnapshot.unique_id)
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse(
        items=[RawSnapshotResponse.model_validate(r) for r in items],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )


# -----------------------------------------------------------------------------
# Stats endpoint
# -----------------------------------------------------------------------------


@app.get("/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    """Get overall database statistics"""
    current_count = db.query(Asset).filter(Asset.valid_until.is_(None)).count()
    version_count = db.query(Asset).count()
    raw_count = db.query(RawSnapshot).count()
    change_count = db.query(ChangeEvent).count()
    snapshot_count = db.query(SnapshotMetadata).count()

    # Date range
    oldest = db.query(func.min(SnapshotMetadata.snapshot_date)).scalar()
    newest = db.query(func.max(SnapshotMetadata.snapshot_date)).scalar()

    # Assets by location (current only)
    location_counts = (
        db.query(Asset.location, func.count(Asset.id))
        .filter(Asset.valid_until.is_(None))
        .group_by(Asset.location)
        .all()
    )

    # Assets by category (current only)
    category_counts = (
        db.query(Asset.category, func.count(Asset.id))
        .filter(Asset.valid_until.is_(None))
        .group_by(Asset.category)
        .all()
    )

    return StatsResponse(
        total_assets_current=current_count,
        total_asset_versions=version_count,
        total_raw_snapshots=raw_count,
        total_change_events=change_count,
        snapshots_count=snapshot_count,
        oldest_snapshot=oldest,
        newest_snapshot=newest,
        assets_by_location={loc: count for loc, count in location_counts},
        assets_by_category={cat: count for cat, count in category_counts},
    )


# -----------------------------------------------------------------------------
# Collections lookup (owner_id -> collection name mapping)
# -----------------------------------------------------------------------------

_collections_cache: dict[str, str] | None = None


def _load_collections() -> dict[str, str]:
    """Load collection names from CSV file"""
    import csv

    collections_path = settings.data_dir / "collections.csv"
    if not collections_path.exists():
        logger.warning(f"Collections file not found: {collections_path}")
        return {}

    mapping = {}
    with open(collections_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            owner_id = row.get("owner_id", "").strip()
            # Use accepted name if available, otherwise suggested
            name = row.get("my_accepted_collection_name", "").strip()
            if not name:
                name = row.get("suggested_collection_name", "").strip()
            # Skip "Unknown" - used to mark investigated but unidentified collections
            if owner_id and name and name.lower() != "unknown":
                mapping[owner_id] = name

    logger.info(f"Loaded {len(mapping)} collection names from {collections_path}")
    return mapping


def _get_collections() -> dict[str, str]:
    """Get collections, loading from CSV if not cached"""
    global _collections_cache
    if _collections_cache is None:
        _collections_cache = _load_collections()
    return _collections_cache


@app.get("/collections/{owner_id}")
def get_collection_name(owner_id: str):
    """Get collection name for an owner_id"""
    collections = _get_collections()
    name = collections.get(owner_id)
    return {"owner_id": owner_id, "collection_name": name}


@app.get("/collections")
def get_all_collections():
    """Get all collection name mappings"""
    collections = _get_collections()
    return {"count": len(collections), "collections": collections}


@app.post("/collections/reload")
def reload_collections():
    """Reload collection names from CSV file"""
    global _collections_cache
    _collections_cache = _load_collections()
    return {"success": True, "count": len(_collections_cache)}


# -----------------------------------------------------------------------------
# Scrape endpoint (authenticated)
# -----------------------------------------------------------------------------


@app.post("/scrape", response_model=ScrapeResponse)
async def trigger_scrape(
    background_tasks: BackgroundTasks,
    api_key: str = Depends(verify_api_key),
):
    """
    Trigger a manual scrape (authenticated).

    Runs in background and returns immediately.
    """
    from app.scraper import run_scrape_and_update

    def run_scrape():
        try:
            result = run_scrape_and_update()
            logger.info(f"Background scrape completed: {result}")
        except Exception as e:
            logger.error(f"Background scrape failed: {e}")

    background_tasks.add_task(run_scrape)

    return ScrapeResponse(
        success=True,
        message="Scrape started in background",
    )


# -----------------------------------------------------------------------------
# Health check
# -----------------------------------------------------------------------------


@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    """Health check endpoint"""
    try:
        # Simple query to verify database connection
        db.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")


# -----------------------------------------------------------------------------
# Live HMRC endpoints (for 2026 data exploration)
# -----------------------------------------------------------------------------

# Cache for live summaries (loaded once per server restart)
_live_summaries_cache: list[dict] | None = None


def _get_live_summaries() -> list[dict]:
    """Get live summaries, fetching from HMRC if not cached"""
    global _live_summaries_cache
    if _live_summaries_cache is None:
        from app.scraper import HMRCScraper
        from dataclasses import asdict

        logger.info("Fetching live summaries from HMRC...")
        with HMRCScraper() as scraper:
            summaries = scraper.scrape_summaries()
        _live_summaries_cache = [asdict(s) for s in summaries]
        logger.info(f"Cached {len(_live_summaries_cache)} live summaries")
    return _live_summaries_cache


@app.get("/live/summaries")
def search_live_summaries(
    search: Optional[str] = None,
    location: Optional[str] = None,
    category: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """
    Search current HMRC data (2026).

    Returns summaries only (unique_id, description, location, category).
    Use /live/details/{unique_id} to fetch full details for a specific asset.
    """
    summaries = _get_live_summaries()

    # Filter
    results = summaries
    if search:
        search_lower = search.lower()
        results = [s for s in results if search_lower in s["description"].lower()]
    if location:
        location_lower = location.lower()
        results = [s for s in results if location_lower in s["location"].lower()]
    if category:
        category_lower = category.lower()
        results = [s for s in results if category_lower in s["category"].lower()]

    # Paginate
    total = len(results)
    pages = (total + page_size - 1) // page_size if total > 0 else 1
    start = (page - 1) * page_size
    end = start + page_size
    items = results[start:end]

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": pages,
    }


@app.get("/live/details/{unique_id}")
def get_live_details(unique_id: str):
    """
    Fetch full details for a single asset from HMRC live.

    This makes a real-time request to HMRC to get contact info, etc.
    """
    from datetime import datetime
    from app.scraper import HMRCScraper
    from dataclasses import asdict

    # First check if the asset exists in summaries
    summaries = _get_live_summaries()
    summary = next((s for s in summaries if s["unique_id"] == unique_id), None)
    if not summary:
        raise HTTPException(status_code=404, detail="Asset not found in HMRC data")

    # Fetch details from HMRC
    with HMRCScraper() as scraper:
        details = scraper.scrape_details(unique_id)

    if not details:
        raise HTTPException(status_code=502, detail="Failed to fetch details from HMRC")

    # Combine summary and details, add scrape timestamp
    result = {
        "_scraped_at": datetime.now().isoformat(),
        "_data_source": "HMRC Live",
        **summary,
        **asdict(details),
    }
    return result


@app.get("/assets/{unique_id}/raw-history")
def get_asset_raw_history(unique_id: str, db: Session = Depends(get_db)):
    """Get raw snapshot history for an asset - shows exactly what HMRC returned at each scrape"""
    snapshots = (
        db.query(RawSnapshot)
        .filter(RawSnapshot.unique_id == unique_id)
        .order_by(RawSnapshot.snapshot_date.desc())
        .all()
    )

    if not snapshots:
        raise HTTPException(status_code=404, detail="No raw snapshots found for this asset")

    return [
        {
            "snapshot_date": str(s.snapshot_date),
            "raw_data": s.raw_data,
        }
        for s in snapshots
    ]


@app.get("/assets/{unique_id}/changes")
def get_asset_changes(unique_id: str, db: Session = Depends(get_db)):
    """Get change events for a specific asset"""
    changes = (
        db.query(ChangeEvent)
        .filter(ChangeEvent.unique_id == unique_id)
        .order_by(ChangeEvent.change_date.desc())
        .all()
    )

    return [
        {
            "change_date": str(c.change_date),
            "change_type": c.change_type,
            "changed_fields": c.changed_fields.split(",") if c.changed_fields else [],
            "summary": c.summary,
        }
        for c in changes
    ]


@app.get("/assets/{unique_id}/history-summary")
def get_asset_history_summary(unique_id: str, db: Session = Depends(get_db)):
    """Get a summary of change history for an asset"""
    changes = (
        db.query(ChangeEvent)
        .filter(ChangeEvent.unique_id == unique_id)
        .order_by(ChangeEvent.change_date)
        .all()
    )

    if not changes:
        return {"first_seen": None, "last_updated": None, "change_count": 0, "changes": []}

    return {
        "first_seen": str(changes[0].change_date),
        "last_updated": str(changes[-1].change_date),
        "change_count": len(changes),
        "changes": [
            {
                "date": str(c.change_date),
                "type": c.change_type,
                "fields": c.changed_fields.split(",") if c.changed_fields else [],
            }
            for c in changes
        ],
    }


@app.get("/live/stats")
def get_live_stats():
    """Get stats for current HMRC data"""
    summaries = _get_live_summaries()

    locations: dict[str, int] = {}
    categories: dict[str, int] = {}
    for s in summaries:
        loc = s["location"]
        cat = s["category"]
        locations[loc] = locations.get(loc, 0) + 1
        categories[cat] = categories.get(cat, 0) + 1

    return {
        "total": len(summaries),
        "assets_by_location": dict(sorted(locations.items())),
        "assets_by_category": dict(sorted(categories.items())),
    }


# -----------------------------------------------------------------------------
# Land & Buildings / Collections endpoints
# -----------------------------------------------------------------------------


@app.get("/land-buildings")
def list_land_buildings(
    search: Optional[str] = None,
    item_type: Optional[str] = None,
    country: Optional[str] = None,
    unique_id: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    List Land & Buildings / Collections with optional filtering.

    Uses FTS5 for fast text search when search parameter is provided.
    """
    # Handle unique_id exact match
    if unique_id:
        item = db.query(LandBuilding).filter(LandBuilding.unique_id == unique_id).first()
        if not item:
            return {"items": [], "total": 0, "page": 1, "page_size": page_size, "pages": 0}
        return {
            "items": [_land_building_to_dict(item)],
            "total": 1,
            "page": 1,
            "page_size": page_size,
            "pages": 1,
        }

    if search:
        # Use FTS5 for text search
        search_term = search.replace('"', '""')
        fts_query = f'"{search_term}"*'

        fts_sql = text("""
            SELECT unique_id, bm25(land_buildings_fts) as rank
            FROM land_buildings_fts
            WHERE land_buildings_fts MATCH :query
            ORDER BY rank
        """)
        fts_results = db.execute(fts_sql, {"query": fts_query}).fetchall()
        matching_ids = [r[0] for r in fts_results]

        if not matching_ids:
            return {"items": [], "total": 0, "page": page, "page_size": page_size, "pages": 0}

        query = db.query(LandBuilding).filter(LandBuilding.unique_id.in_(matching_ids))

        if item_type:
            query = query.filter(LandBuilding.item_type == item_type)
        if country:
            query = query.filter(LandBuilding.country.ilike(f"%{country}%"))

        total = query.count()
        pages = (total + page_size - 1) // page_size

        items = query.all()
        id_to_rank = {r[0]: i for i, r in enumerate(fts_results)}
        items.sort(key=lambda a: id_to_rank.get(a.unique_id, 999999))
        items = items[(page - 1) * page_size : page * page_size]
    else:
        query = db.query(LandBuilding)

        if item_type:
            query = query.filter(LandBuilding.item_type == item_type)
        if country:
            query = query.filter(LandBuilding.country.ilike(f"%{country}%"))

        total = query.count()
        pages = (total + page_size - 1) // page_size

        items = (
            query.order_by(LandBuilding.name)
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )

    return {
        "items": [_land_building_to_dict(item) for item in items],
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": pages,
    }


def _land_building_to_dict(item: LandBuilding) -> dict:
    """Convert LandBuilding to dict for API response"""
    return {
        "unique_id": item.unique_id,
        "item_type": item.item_type,
        "country": item.country,
        "name": item.name,
        "description": item.description,
        "access_details": item.access_details,
        "os_grid_ref": item.os_grid_ref,
        "contact_name": item.contact_name,
        "contact_address": item.contact_address,
        "telephone": item.telephone,
        "fax": item.fax,
        "email": item.email,
        "website": item.website,
        "undertakings": item.undertakings,
        "scraped_at": item.scraped_at.isoformat() if item.scraped_at else None,
    }


@app.get("/land-buildings/{unique_id}")
def get_land_building(unique_id: str, db: Session = Depends(get_db)):
    """Get a specific Land & Building or Collection item"""
    item = db.query(LandBuilding).filter(LandBuilding.unique_id == unique_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return _land_building_to_dict(item)


@app.get("/land-buildings-stats")
def get_land_buildings_stats(db: Session = Depends(get_db)):
    """Get stats for Land & Buildings / Collections"""
    total = db.query(LandBuilding).count()
    land_count = db.query(LandBuilding).filter(LandBuilding.item_type == "land_building").count()
    collection_count = db.query(LandBuilding).filter(LandBuilding.item_type == "collection").count()
    with_undertakings = db.query(LandBuilding).filter(LandBuilding.undertakings.isnot(None)).count()

    country_counts = (
        db.query(LandBuilding.country, func.count(LandBuilding.id))
        .group_by(LandBuilding.country)
        .all()
    )

    return {
        "total": total,
        "land_buildings": land_count,
        "collections": collection_count,
        "with_undertakings": with_undertakings,
        "by_country": {country: count for country, count in country_counts},
    }


# -----------------------------------------------------------------------------
# Browse UI
# -----------------------------------------------------------------------------


@app.get("/browse")
def browse_ui():
    """Serve the browse UI"""
    html_path = Path(__file__).parent / "static" / "browse.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="Browse UI not found")
    return FileResponse(html_path, media_type="text/html")
