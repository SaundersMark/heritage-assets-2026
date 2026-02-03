"""SQLAlchemy models for Heritage Assets with SCD Type 2 support"""

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, DateTime, Index, Integer, String, Text, func, text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all models"""

    pass


class RawSnapshot(Base):
    """
    Raw snapshot storage - preserves exact HMRC data as scraped.
    Each row = one asset at one snapshot date.
    """

    __tablename__ = "raw_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    unique_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    raw_data: Mapped[dict] = mapped_column(JSON, nullable=False)

    __table_args__ = (
        Index("ix_raw_snapshots_date_unique", "snapshot_date", "unique_id", unique=True),
    )

    def __repr__(self) -> str:
        return f"<RawSnapshot({self.snapshot_date}, {self.unique_id})>"


class Asset(Base):
    """
    Tidied asset data with SCD Type 2 versioning.

    - valid_from: date this version became active
    - valid_until: date this version was superseded (NULL = current)

    To get current assets: WHERE valid_until IS NULL
    To get assets as of date X: WHERE valid_from <= X AND (valid_until IS NULL OR valid_until > X)
    """

    __tablename__ = "assets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    unique_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    owner_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)

    # Core fields
    description: Mapped[str] = mapped_column(Text, nullable=False)
    location: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(100), nullable=False, index=True)

    # Access info
    access_details: Mapped[Optional[str]] = mapped_column(Text)

    # Contact info (tidied)
    contact_name: Mapped[Optional[str]] = mapped_column(String(255))
    address_line1: Mapped[Optional[str]] = mapped_column(String(255))
    address_line2: Mapped[Optional[str]] = mapped_column(String(255))
    address_city: Mapped[Optional[str]] = mapped_column(String(100))
    address_postcode: Mapped[Optional[str]] = mapped_column(String(20))
    telephone: Mapped[Optional[str]] = mapped_column(String(50))
    fax: Mapped[Optional[str]] = mapped_column(String(50))
    email: Mapped[Optional[str]] = mapped_column(String(255))
    website: Mapped[Optional[str]] = mapped_column(String(500))

    # SCD Type 2 fields
    valid_from: Mapped[date] = mapped_column(Date, nullable=False)
    valid_until: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    # Audit
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_assets_unique_valid", "unique_id", "valid_until"),
        Index("ix_assets_current", "unique_id", "valid_until", postgresql_where="valid_until IS NULL"),
    )

    @property
    def is_current(self) -> bool:
        return self.valid_until is None

    def __repr__(self) -> str:
        status = "current" if self.is_current else f"until {self.valid_until}"
        return f"<Asset({self.unique_id}, {status})>"


class ChangeEvent(Base):
    """
    Human-readable change log for quick "what changed" queries.
    Summarizes changes without full SCD2 detail lookup.
    """

    __tablename__ = "change_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    unique_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    change_type: Mapped[str] = mapped_column(String(20), nullable=False, index=True)  # added, updated, removed
    change_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    changed_fields: Mapped[Optional[str]] = mapped_column(Text)  # comma-separated
    summary: Mapped[Optional[str]] = mapped_column(Text)  # human-readable summary
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (Index("ix_change_events_date", "change_date"),)

    def __repr__(self) -> str:
        return f"<ChangeEvent({self.unique_id}, {self.change_type}, {self.change_date})>"


class LandBuilding(Base):
    """
    Land, Buildings and Collections - separate HMRC database from Works of Art.
    These items have undertakings (legal text about public access obligations).

    item_type: 'land_building' or 'collection'
    """

    __tablename__ = "land_buildings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    unique_id: Mapped[str] = mapped_column(String(50), nullable=False, unique=True, index=True)
    item_type: Mapped[str] = mapped_column(String(20), nullable=False, index=True)  # land_building or collection

    # Core fields
    country: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)  # "Name of Property"
    description: Mapped[Optional[str]] = mapped_column(Text)
    access_details: Mapped[Optional[str]] = mapped_column(Text)
    os_grid_ref: Mapped[Optional[str]] = mapped_column(String(50))

    # Contact info
    contact_name: Mapped[Optional[str]] = mapped_column(String(255))
    contact_address: Mapped[Optional[str]] = mapped_column(Text)
    telephone: Mapped[Optional[str]] = mapped_column(String(50))
    fax: Mapped[Optional[str]] = mapped_column(String(50))
    email: Mapped[Optional[str]] = mapped_column(String(255))
    website: Mapped[Optional[str]] = mapped_column(String(500))

    # Undertakings - the legal text about public access obligations
    undertakings: Mapped[Optional[str]] = mapped_column(Text)

    # Map - whether a sketch map image exists at /images/{unique_id}.jpg
    has_map: Mapped[bool] = mapped_column(Integer, default=False)  # SQLite doesn't have bool

    # Audit
    scraped_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    def __repr__(self) -> str:
        return f"<LandBuilding({self.unique_id}, {self.item_type}, {self.name[:50] if self.name else ''})>"


class SnapshotMetadata(Base):
    """
    Metadata about each snapshot (scrape or import).
    """

    __tablename__ = "snapshot_metadata"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False, unique=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False)  # 'scrape' or 'import'
    source_file: Mapped[Optional[str]] = mapped_column(String(255))
    asset_count: Mapped[int] = mapped_column(Integer, nullable=False)
    added_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_count: Mapped[int] = mapped_column(Integer, default=0)
    removed_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    def __repr__(self) -> str:
        return f"<SnapshotMetadata({self.snapshot_date}, {self.source}, {self.asset_count} assets)>"


def create_tables(engine):
    """Create all tables"""
    Base.metadata.create_all(bind=engine)

    # Create FTS5 virtual table for fast text search (contentless - stores own data)
    with engine.connect() as conn:
        # Assets FTS
        result = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='assets_fts'")
        ).fetchone()
        if not result:
            conn.execute(
                text("""
                    CREATE VIRTUAL TABLE assets_fts USING fts5(
                        unique_id UNINDEXED,
                        description,
                        contact_name,
                        location,
                        category
                    )
                """)
            )
            conn.commit()

        # Land Buildings FTS
        result = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='land_buildings_fts'")
        ).fetchone()
        if not result:
            conn.execute(
                text("""
                    CREATE VIRTUAL TABLE land_buildings_fts USING fts5(
                        unique_id UNINDEXED,
                        name,
                        description,
                        country,
                        undertakings
                    )
                """)
            )
            conn.commit()


def rebuild_fts_index(engine):
    """Rebuild the FTS5 index from the assets table (current records only)"""
    with engine.connect() as conn:
        # Clear and rebuild
        conn.execute(text("DELETE FROM assets_fts"))
        conn.execute(
            text("""
                INSERT INTO assets_fts(unique_id, description, contact_name, location, category)
                SELECT unique_id, description, COALESCE(contact_name, ''), location, category
                FROM assets
                WHERE valid_until IS NULL
            """)
        )
        conn.commit()


def rebuild_land_buildings_fts_index(engine):
    """Rebuild the FTS5 index for land_buildings table"""
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM land_buildings_fts"))
        conn.execute(
            text("""
                INSERT INTO land_buildings_fts(unique_id, name, description, country, undertakings)
                SELECT unique_id, name, COALESCE(description, ''), country, COALESCE(undertakings, '')
                FROM land_buildings
            """)
        )
        conn.commit()
