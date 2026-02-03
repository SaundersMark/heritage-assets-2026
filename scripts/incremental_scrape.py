#!/usr/bin/env python3
"""
Incremental HMRC Heritage Assets Scraper

Scrapes assets that haven't been scraped in the last N days.
Saves progress after each asset so it can resume after interruption.
"""

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import func

from app.database import get_session
from app.models import RawSnapshot, SnapshotMetadata, create_tables
from app.database import engine
from app.scraper import HMRCScraper
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(settings.logs_dir / "incremental_scrape.log"),
    ],
)
logger = logging.getLogger(__name__)


def get_recently_scraped_ids(session, days: int = 7) -> set[str]:
    """Get unique_ids that have been scraped in the last N days"""
    cutoff = date.today() - timedelta(days=days)

    recent = (
        session.query(RawSnapshot.unique_id)
        .filter(RawSnapshot.snapshot_date >= cutoff)
        .distinct()
        .all()
    )
    return {r[0] for r in recent}


def run_incremental_scrape(
    skip_days: int = 7,
    delay: float = None,
    limit: int = None,
    dry_run: bool = False,
):
    """
    Scrape assets not scraped in the last skip_days.

    Args:
        skip_days: Skip assets scraped within this many days
        delay: Override delay between requests (default: use config)
        limit: Maximum number of assets to scrape (for testing)
        dry_run: If True, just show what would be scraped
    """
    if delay is not None:
        settings.scrape_detail_delay = delay

    create_tables(engine)
    settings.logs_dir.mkdir(exist_ok=True)

    logger.info("=" * 60)
    logger.info("INCREMENTAL HERITAGE ASSETS SCRAPE")
    logger.info(f"Skip assets scraped in last {skip_days} days")
    logger.info(f"Delay between requests: {settings.scrape_detail_delay}s")
    logger.info("=" * 60)

    snapshot_date = date.today()

    # Get all summaries from HMRC
    logger.info("Fetching summaries from HMRC...")
    with HMRCScraper() as scraper:
        summaries = scraper.scrape_summaries()

    if not summaries:
        logger.error("No summaries fetched, aborting")
        return {"success": False, "error": "No summaries"}

    logger.info(f"Found {len(summaries)} total assets on HMRC")

    # Check which have been scraped recently
    with get_session() as session:
        recent_ids = get_recently_scraped_ids(session, skip_days)

    logger.info(f"Found {len(recent_ids)} assets scraped in last {skip_days} days")

    # Filter to those needing scrape
    to_scrape = [s for s in summaries if s.unique_id not in recent_ids]
    logger.info(f"Need to scrape {len(to_scrape)} assets")

    if limit:
        to_scrape = to_scrape[:limit]
        logger.info(f"Limited to {limit} assets")

    if dry_run:
        logger.info("DRY RUN - not actually scraping")
        for s in to_scrape[:20]:
            logger.info(f"  Would scrape: {s.unique_id} - {s.description[:60]}...")
        if len(to_scrape) > 20:
            logger.info(f"  ... and {len(to_scrape) - 20} more")
        return {"success": True, "would_scrape": len(to_scrape)}

    # Estimate time
    est_seconds = len(to_scrape) * settings.scrape_detail_delay
    est_hours = est_seconds / 3600
    logger.info(f"Estimated time: {est_hours:.1f} hours ({est_seconds:.0f} seconds)")

    # Scrape each asset and save immediately
    stats = {"scraped": 0, "errors": 0, "skipped": 0}
    start_time = time.time()

    with HMRCScraper() as scraper:
        for i, summary in enumerate(to_scrape):
            try:
                # Progress logging every 100 assets
                if i > 0 and i % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = i / elapsed
                    remaining = (len(to_scrape) - i) / rate if rate > 0 else 0
                    logger.info(
                        f"Progress: {i}/{len(to_scrape)} "
                        f"({i/len(to_scrape)*100:.1f}%) - "
                        f"Rate: {rate:.1f}/s - "
                        f"ETA: {remaining/60:.0f} min"
                    )

                # Fetch details
                details = scraper.scrape_details(summary.unique_id)

                if not details:
                    logger.warning(f"Failed to fetch details for {summary.unique_id}")
                    stats["errors"] += 1
                    continue

                # Build raw record
                raw_record = {
                    "uniqueID": summary.unique_id,
                    "description": summary.description,
                    "location": summary.location,
                    "category": summary.category,
                    "owner_id": details.owner_id,
                    "access_details": details.access_details,
                    "contact_name": details.contact_name,
                    "contact_address": details.contact_address,
                    "contact_reference": details.contact_reference,
                    "telephone_no": details.telephone_no,
                    "fax_no": details.fax_no,
                    "email": details.email,
                    "website": details.website,
                }

                # Save immediately
                with get_session() as session:
                    snapshot = RawSnapshot(
                        snapshot_date=snapshot_date,
                        unique_id=summary.unique_id,
                        raw_data=raw_record,
                    )
                    session.add(snapshot)

                stats["scraped"] += 1

            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error scraping {summary.unique_id}: {e}")
                stats["errors"] += 1

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Scrape complete in {elapsed/60:.1f} minutes")
    logger.info(f"Stats: {stats}")
    logger.info("=" * 60)

    return {"success": True, "stats": stats, "elapsed_seconds": elapsed}


def main():
    parser = argparse.ArgumentParser(description="Incremental HMRC scraper")
    parser.add_argument(
        "--skip-days", type=int, default=7,
        help="Skip assets scraped within this many days (default: 7)"
    )
    parser.add_argument(
        "--delay", type=float, default=None,
        help="Delay between requests in seconds (default: from config)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Maximum number of assets to scrape (for testing)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be scraped without actually scraping"
    )

    args = parser.parse_args()

    result = run_incremental_scrape(
        skip_days=args.skip_days,
        delay=args.delay,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    print(f"\nResult: {result}")


if __name__ == "__main__":
    main()
