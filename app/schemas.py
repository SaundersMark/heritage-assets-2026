"""Pydantic schemas for API request/response models"""

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class AssetBase(BaseModel):
    """Base asset fields"""

    unique_id: str
    owner_id: Optional[str] = None
    description: str
    location: str
    category: str
    access_details: Optional[str] = None
    contact_name: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    address_city: Optional[str] = None
    address_postcode: Optional[str] = None
    telephone: Optional[str] = None
    fax: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None


class AssetResponse(AssetBase):
    """Asset response with SCD2 metadata"""

    id: int
    valid_from: date
    valid_until: Optional[date] = None

    model_config = ConfigDict(from_attributes=True)


class AssetHistoryResponse(BaseModel):
    """Asset with full version history"""

    unique_id: str
    current: Optional[AssetResponse] = None
    history: list[AssetResponse] = []


class ChangeEventResponse(BaseModel):
    """Change event response"""

    id: int
    unique_id: str
    change_type: str
    change_date: date
    changed_fields: Optional[str] = None
    summary: Optional[str] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class RawSnapshotResponse(BaseModel):
    """Raw snapshot response"""

    id: int
    snapshot_date: date
    unique_id: str
    raw_data: dict

    model_config = ConfigDict(from_attributes=True)


class SnapshotMetadataResponse(BaseModel):
    """Snapshot metadata response"""

    id: int
    snapshot_date: date
    source: str
    source_file: Optional[str] = None
    asset_count: int
    added_count: int
    updated_count: int
    removed_count: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class StatsResponse(BaseModel):
    """Overall statistics response"""

    total_assets_current: int
    total_asset_versions: int
    total_raw_snapshots: int
    total_change_events: int
    snapshots_count: int
    oldest_snapshot: Optional[date] = None
    newest_snapshot: Optional[date] = None
    assets_by_location: dict[str, int] = {}
    assets_by_category: dict[str, int] = {}


class ScrapeResponse(BaseModel):
    """Scrape operation response"""

    success: bool
    message: str
    stats: Optional[dict] = None


class PaginatedResponse(BaseModel):
    """Generic paginated response wrapper"""

    items: list
    total: int
    page: int
    page_size: int
    pages: int
