#!/usr/bin/env python3
"""
Process raw snapshots into the Asset table with SCD2 tracking.

This script takes raw snapshots that have been stored (e.g., by incremental_scrape.py)
and processes them into the Asset table, handling:
- New assets (added)
- Deleted assets (removed - marked with valid_until)
- Changed assets (updated - old version closed, new version created)
- Change events logged for all changes
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import engine, get_session
from app.models import Asset, ChangeEvent, RawSnapshot, SnapshotMetadata, create_tables
from app.tidying import TidiedAsset, compare_tidied_assets, tidy_raw_record
from scripts.import_historical import asset_to_tidied, tidied_to_asset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def get_raw_records(session, snapshot_date: date) -> list[dict]:
    """Get raw records for a specific snapshot date"""
    snapshots = (
        session.query(RawSnapshot)
        .filter(RawSnapshot.snapshot_date == snapshot_date)
        .all()
    )
    return [s.raw_data for s in snapshots]


def get_current_assets(session) -> dict[str, Asset]:
    """Get current (valid_until IS NULL) assets indexed by unique_id"""
    assets = session.query(Asset).filter(Asset.valid_until.is_(None)).all()
    return {a.unique_id: a for a in assets}


def process_snapshot(session, snapshot_date: date, dry_run: bool = False) -> dict[str, int]:
    """
    Process raw snapshots for a date into the Asset table with SCD2 tracking.

    Returns: dict with counts of added, updated, removed
    """
    stats = {"added": 0, "updated": 0, "removed": 0, "unchanged": 0}

    # Get raw records for this date
    logger.info(f"Loading raw snapshots for {snapshot_date}...")
    records = get_raw_records(session, snapshot_date)
    if not records:
        logger.error(f"No raw snapshots found for {snapshot_date}")
        return stats
    logger.info(f"Found {len(records)} raw records")

    # Get current state
    logger.info("Loading current assets...")
    current_assets = get_current_assets(session)
    current_ids = set(current_assets.keys())
    logger.info(f"Found {len(current_ids)} current assets")

    # Tidy new records
    logger.info("Tidying records...")
    new_ids = set()
    tidied_map: dict[str, TidiedAsset] = {}

    for raw in records:
        tidied = tidy_raw_record(raw)
        if tidied.unique_id:
            new_ids.add(tidied.unique_id)
            tidied_map[tidied.unique_id] = tidied

    logger.info(f"Tidied {len(tidied_map)} records")

    # Calculate changes
    added_ids = new_ids - current_ids
    removed_ids = current_ids - new_ids
    common_ids = current_ids & new_ids

    logger.info(f"New assets: {len(added_ids)}")
    logger.info(f"Removed assets: {len(removed_ids)}")
    logger.info(f"Common assets: {len(common_ids)}")

    if dry_run:
        # Just count changes without making them
        for uid in common_ids:
            old_asset = current_assets[uid]
            new_tidied = tidied_map[uid]
            old_tidied = asset_to_tidied(old_asset)
            changed_fields = compare_tidied_assets(old_tidied, new_tidied)
            if changed_fields:
                stats["updated"] += 1
            else:
                stats["unchanged"] += 1
        stats["added"] = len(added_ids)
        stats["removed"] = len(removed_ids)
        return stats

    # Handle additions
    logger.info("Processing additions...")
    for uid in added_ids:
        tidied = tidied_map[uid]
        asset = tidied_to_asset(tidied, snapshot_date)
        session.add(asset)
        stats["added"] += 1

        session.add(
            ChangeEvent(
                unique_id=uid,
                change_type="added",
                change_date=snapshot_date,
                summary=f"Asset added: {tidied.description[:100] if tidied.description else 'No description'}",
            )
        )

    # Handle removals
    logger.info("Processing removals...")
    for uid in removed_ids:
        asset = current_assets[uid]
        asset.valid_until = snapshot_date
        stats["removed"] += 1

        session.add(
            ChangeEvent(
                unique_id=uid,
                change_type="removed",
                change_date=snapshot_date,
                summary=f"Asset removed: {asset.description[:100] if asset.description else 'No description'}",
            )
        )

    # Handle updates
    logger.info("Processing updates...")
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

            session.add(
                ChangeEvent(
                    unique_id=uid,
                    change_type="updated",
                    change_date=snapshot_date,
                    changed_fields=",".join(changed_fields),
                    summary=f"Fields changed: {', '.join(changed_fields[:5])}",
                )
            )
        else:
            stats["unchanged"] += 1

    session.flush()
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Process raw snapshots into Asset table with SCD2 tracking"
    )
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Snapshot date to process (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )

    args = parser.parse_args()

    # Parse date
    try:
        snapshot_date = date.fromisoformat(args.date)
    except ValueError:
        logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("PROCESS SNAPSHOT INTO ASSET TABLE")
    logger.info(f"Snapshot date: {snapshot_date}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("=" * 60)

    create_tables(engine)

    with get_session() as session:
        # Check if already processed
        existing = (
            session.query(SnapshotMetadata)
            .filter_by(snapshot_date=snapshot_date)
            .first()
        )
        if existing and not args.dry_run:
            logger.warning(f"Snapshot {snapshot_date} already processed!")
            logger.warning(f"  Source: {existing.source}")
            logger.warning(f"  Assets: {existing.asset_count}")
            logger.warning("Use --dry-run to see what would change, or delete the metadata to reprocess")
            sys.exit(1)

        # Process the snapshot
        stats = process_snapshot(session, snapshot_date, dry_run=args.dry_run)

        logger.info("=" * 60)
        logger.info("RESULTS")
        logger.info("=" * 60)
        logger.info(f"Added:     {stats['added']:,}")
        logger.info(f"Updated:   {stats['updated']:,}")
        logger.info(f"Removed:   {stats['removed']:,}")
        logger.info(f"Unchanged: {stats['unchanged']:,}")

        if args.dry_run:
            logger.info("\nDRY RUN - no changes made")
        else:
            # Get raw record count for metadata
            raw_count = (
                session.query(RawSnapshot)
                .filter(RawSnapshot.snapshot_date == snapshot_date)
                .count()
            )

            # Record metadata
            metadata = SnapshotMetadata(
                snapshot_date=snapshot_date,
                source="scrape",
                asset_count=raw_count,
                added_count=stats["added"],
                updated_count=stats["updated"],
                removed_count=stats["removed"],
            )
            session.add(metadata)

            logger.info("\nChanges committed to database")

            # Final stats
            total_assets = session.query(Asset).count()
            current_assets = session.query(Asset).filter(Asset.valid_until.is_(None)).count()

            logger.info(f"\nDatabase now has:")
            logger.info(f"  Total asset versions: {total_assets:,}")
            logger.info(f"  Current assets: {current_assets:,}")


if __name__ == "__main__":
    main()
