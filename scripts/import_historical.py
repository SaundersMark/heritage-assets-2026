#!/usr/bin/env python3
"""
Import historical CSV snapshots into the database.

Loads the 3 existing snapshots chronologically:
1. Heritage_assets_downloaded_25_January_2023.csv
2. Heritage_assets_downloaded_30_September_2023.csv
3. Heritage_assets_downloaded_2_March_2024.csv

For each snapshot:
1. Store raw data in raw_snapshots table
2. Apply tidying
3. Compare to previous snapshot (if any) and build SCD2 history
"""

import json
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import engine, get_session
from app.models import Asset, ChangeEvent, RawSnapshot, SnapshotMetadata, create_tables
from app.tidying import TidiedAsset, compare_tidied_assets, tidy_raw_record


# Historical files with their snapshot dates
HISTORICAL_FILES = [
    ("Heritage_assets_downloaded_25_January_2023.csv", date(2023, 1, 25)),
    ("Heritage_assets_downloaded_30_September_2023.csv", date(2023, 9, 30)),
    ("Heritage_assets_downloaded_2_March_2024.csv", date(2024, 3, 2)),
]


def parse_filename_date(filename: str) -> date:
    """Extract date from filename like 'Heritage_assets_downloaded_25_January_2023.csv'"""
    match = re.search(r"(\d+)_(\w+)_(\d{4})", filename)
    if match:
        day = int(match.group(1))
        month_name = match.group(2)
        year = int(match.group(3))

        month_map = {
            "January": 1,
            "February": 2,
            "March": 3,
            "April": 4,
            "May": 5,
            "June": 6,
            "July": 7,
            "August": 8,
            "September": 9,
            "October": 10,
            "November": 11,
            "December": 12,
        }
        month = month_map.get(month_name, 1)
        return date(year, month, day)
    raise ValueError(f"Cannot parse date from filename: {filename}")


def load_csv_as_raw(filepath: Path, snapshot_date: date) -> list[dict]:
    """Load CSV file and return list of raw records"""
    print(f"  Loading {filepath.name}...")
    df = pd.read_csv(filepath)
    print(f"  Found {len(df)} records")

    records = []
    for _, row in df.iterrows():
        raw_data = {col: (None if pd.isna(row[col]) else row[col]) for col in df.columns}
        records.append(raw_data)

    return records


def store_raw_snapshot(records: list[dict], snapshot_date: date, session) -> int:
    """Store raw records in raw_snapshots table"""
    count = 0
    for raw in records:
        unique_id = str(raw.get("uniqueID", ""))
        if not unique_id:
            continue

        # Check if already exists
        existing = (
            session.query(RawSnapshot)
            .filter_by(snapshot_date=snapshot_date, unique_id=unique_id)
            .first()
        )
        if existing:
            continue

        snapshot = RawSnapshot(
            snapshot_date=snapshot_date,
            unique_id=unique_id,
            raw_data=raw,
        )
        session.add(snapshot)
        count += 1

    session.flush()
    return count


def get_current_assets(session) -> dict[str, Asset]:
    """Get current (valid_until IS NULL) assets indexed by unique_id"""
    assets = session.query(Asset).filter(Asset.valid_until.is_(None)).all()
    return {a.unique_id: a for a in assets}


def tidied_to_asset(tidied: TidiedAsset, valid_from: date) -> Asset:
    """Convert TidiedAsset to Asset model"""
    return Asset(
        unique_id=tidied.unique_id,
        owner_id=tidied.owner_id,
        description=tidied.description,
        location=tidied.location,
        category=tidied.category,
        access_details=tidied.access_details,
        contact_name=tidied.contact.contact_name,
        address_line1=tidied.contact.address_line1,
        address_line2=tidied.contact.address_line2,
        address_city=tidied.contact.address_city,
        address_postcode=tidied.contact.address_postcode,
        telephone=tidied.contact.telephone,
        fax=tidied.contact.fax,
        email=tidied.contact.email,
        website=tidied.contact.website,
        valid_from=valid_from,
        valid_until=None,
    )


def asset_to_tidied(asset: Asset) -> TidiedAsset:
    """Convert Asset model to TidiedAsset for comparison"""
    from app.tidying import TidiedContact

    return TidiedAsset(
        unique_id=asset.unique_id,
        owner_id=asset.owner_id,
        description=asset.description,
        location=asset.location,
        category=asset.category,
        access_details=asset.access_details,
        contact=TidiedContact(
            contact_name=asset.contact_name,
            address_line1=asset.address_line1,
            address_line2=asset.address_line2,
            address_city=asset.address_city,
            address_postcode=asset.address_postcode,
            telephone=asset.telephone,
            fax=asset.fax,
            email=asset.email,
            website=asset.website,
        ),
    )


def process_snapshot(
    records: list[dict], snapshot_date: date, session
) -> dict[str, int]:
    """
    Process a snapshot: tidy records, compare to current state, update SCD2.

    Returns: dict with counts of added, updated, removed
    """
    stats = {"added": 0, "updated": 0, "removed": 0}

    # Get current state
    current_assets = get_current_assets(session)
    current_ids = set(current_assets.keys())

    # Process new records
    new_ids = set()
    tidied_map: dict[str, TidiedAsset] = {}

    for raw in records:
        tidied = tidy_raw_record(raw)
        if tidied.unique_id:
            new_ids.add(tidied.unique_id)
            tidied_map[tidied.unique_id] = tidied

    # Handle additions (in new, not in current)
    added_ids = new_ids - current_ids
    for uid in added_ids:
        tidied = tidied_map[uid]
        asset = tidied_to_asset(tidied, snapshot_date)
        session.add(asset)
        stats["added"] += 1

        # Log change event
        session.add(
            ChangeEvent(
                unique_id=uid,
                change_type="added",
                change_date=snapshot_date,
                summary=f"Asset added: {tidied.description[:100]}...",
            )
        )

    # Handle removals (in current, not in new)
    removed_ids = current_ids - new_ids
    for uid in removed_ids:
        asset = current_assets[uid]
        # Close the current version
        asset.valid_until = snapshot_date
        stats["removed"] += 1

        # Log change event
        session.add(
            ChangeEvent(
                unique_id=uid,
                change_type="removed",
                change_date=snapshot_date,
                summary=f"Asset removed: {asset.description[:100]}...",
            )
        )

    # Handle updates (in both - check for changes)
    common_ids = current_ids & new_ids
    for uid in common_ids:
        old_asset = current_assets[uid]
        new_tidied = tidied_map[uid]
        old_tidied = asset_to_tidied(old_asset)

        changed_fields = compare_tidied_assets(old_tidied, new_tidied)
        if changed_fields:
            # Close old version
            old_asset.valid_until = snapshot_date

            # Create new version
            new_asset = tidied_to_asset(new_tidied, snapshot_date)
            session.add(new_asset)
            stats["updated"] += 1

            # Log change event
            session.add(
                ChangeEvent(
                    unique_id=uid,
                    change_type="updated",
                    change_date=snapshot_date,
                    changed_fields=",".join(changed_fields),
                    summary=f"Fields changed: {', '.join(changed_fields[:5])}",
                )
            )

    session.flush()
    return stats


def import_historical_data(data_dir: Path):
    """Main import function"""
    print("=" * 60)
    print("HERITAGE ASSETS HISTORICAL DATA IMPORT")
    print("=" * 60)

    # Create tables
    print("\nCreating database tables...")
    create_tables(engine)

    with get_session() as session:
        for filename, snapshot_date in HISTORICAL_FILES:
            filepath = data_dir / filename
            if not filepath.exists():
                print(f"\n  WARNING: File not found: {filepath}")
                continue

            print(f"\n{'=' * 60}")
            print(f"Processing: {filename}")
            print(f"Snapshot date: {snapshot_date}")
            print("=" * 60)

            # Check if already imported
            existing = (
                session.query(SnapshotMetadata)
                .filter_by(snapshot_date=snapshot_date)
                .first()
            )
            if existing:
                print(f"  Already imported, skipping...")
                continue

            # Load raw data
            records = load_csv_as_raw(filepath, snapshot_date)

            # Store raw snapshots
            print("  Storing raw snapshots...")
            raw_count = store_raw_snapshot(records, snapshot_date, session)
            print(f"  Stored {raw_count} raw records")

            # Process and build SCD2 history
            print("  Processing SCD2 changes...")
            stats = process_snapshot(records, snapshot_date, session)
            print(f"  Added: {stats['added']}")
            print(f"  Updated: {stats['updated']}")
            print(f"  Removed: {stats['removed']}")

            # Record metadata
            metadata = SnapshotMetadata(
                snapshot_date=snapshot_date,
                source="import",
                source_file=filename,
                asset_count=len(records),
                added_count=stats["added"],
                updated_count=stats["updated"],
                removed_count=stats["removed"],
            )
            session.add(metadata)

        # Final stats
        print("\n" + "=" * 60)
        print("IMPORT COMPLETE")
        print("=" * 60)

        total_raw = session.query(RawSnapshot).count()
        total_assets = session.query(Asset).count()
        current_assets = session.query(Asset).filter(Asset.valid_until.is_(None)).count()
        total_changes = session.query(ChangeEvent).count()
        total_snapshots = session.query(SnapshotMetadata).count()

        print(f"\nDatabase statistics:")
        print(f"  Raw snapshots: {total_raw}")
        print(f"  Asset versions (SCD2): {total_assets}")
        print(f"  Current assets: {current_assets}")
        print(f"  Change events: {total_changes}")
        print(f"  Snapshots processed: {total_snapshots}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Import historical heritage asset data")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("/home/mark/code/tax/heritage_assets/data/historic_downloads"),
        help="Directory containing historical CSV files",
    )
    args = parser.parse_args()

    if not args.data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}")
        sys.exit(1)

    import_historical_data(args.data_dir)


if __name__ == "__main__":
    main()
