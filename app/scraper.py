#!/usr/bin/env python3
"""
HMRC Heritage Assets Scraper

Scrapes current data from HMRC website, stores raw data,
applies tidying, and updates SCD2 asset records.
"""

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from config import settings

logger = logging.getLogger(__name__)


@dataclass
class ScrapedSummary:
    """Summary data from main listing page"""

    unique_id: str
    description: str
    location: str
    category: str


@dataclass
class ScrapedDetails:
    """Detailed data from individual asset page"""

    unique_id: str
    owner_id: Optional[str] = None
    access_details: Optional[str] = None
    contact_name: Optional[str] = None
    contact_address: Optional[str] = None
    contact_reference: Optional[str] = None
    telephone_no: Optional[str] = None
    fax_no: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None


class HMRCScraper:
    """Scraper for HMRC Heritage Assets website"""

    def __init__(self):
        self.client = httpx.Client(timeout=settings.scrape_timeout)
        self.stats = {
            "summaries_found": 0,
            "details_fetched": 0,
            "errors": 0,
        }

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()

    def _get_with_retry(self, url: str, max_retries: int = 3) -> Optional[httpx.Response]:
        """Make GET request with retry logic"""
        for attempt in range(max_retries):
            try:
                time.sleep(settings.scrape_delay)
                response = self.client.get(url)
                if response.status_code == 200:
                    return response
                logger.warning(f"HTTP {response.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                time.sleep(settings.scrape_delay * (attempt + 1))

        self.stats["errors"] += 1
        return None

    def scrape_summaries(self) -> list[ScrapedSummary]:
        """Scrape main listing page for all asset summaries"""
        logger.info("Fetching summary listing...")

        response = self._get_with_retry(settings.hmrc_summary_url)
        if not response:
            logger.error("Failed to fetch summary page")
            return []

        soup = BeautifulSoup(response.content, "html.parser")
        rows = soup.find_all("tr", {"align": "left", "valign": "top"})
        logger.info(f"Found {len(rows)} potential asset rows")

        summaries = []
        for row in rows:
            try:
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue

                href_element = cells[0].find("a")
                if not href_element or "href" not in href_element.attrs:
                    continue

                href = href_element["href"]
                if "ID=" not in href:
                    continue

                unique_id = href.split("ID=")[-1].split("&")[0]
                if not unique_id:
                    continue

                summary = ScrapedSummary(
                    unique_id=unique_id,
                    description=cells[1].get_text(strip=True),
                    location=cells[2].get_text(strip=True),
                    category=cells[3].get_text(strip=True),
                )
                summaries.append(summary)

            except Exception as e:
                logger.warning(f"Error parsing row: {e}")
                self.stats["errors"] += 1

        self.stats["summaries_found"] = len(summaries)
        logger.info(f"Extracted {len(summaries)} asset summaries")
        return summaries

    def scrape_details(self, unique_id: str) -> Optional[ScrapedDetails]:
        """Scrape detailed information for a single asset"""
        url = settings.hmrc_detail_url_template.format(unique_id=unique_id)
        time.sleep(settings.scrape_detail_delay)

        response = self._get_with_retry(url)
        if not response:
            return None

        try:
            soup = BeautifulSoup(response.content, "html.parser")

            # Extract owner_id from href
            owner_tag = soup.find(href=re.compile(r"Owner="))
            owner_id = "single owner"
            if owner_tag:
                owner_href = owner_tag.get("href", "")
                owner_match = re.search(r"Owner=([0-9.]+)&", owner_href)
                if owner_match:
                    owner_id = owner_match.group(1)

            def safe_extract(label: str) -> str:
                try:
                    element = soup.find(string=label)
                    if element:
                        return element.find_next("td").get_text(strip=True)
                except Exception:
                    pass
                return ""

            # Extract website from link
            website = ""
            website_tag = soup.find(string="Web Site(s):")
            if website_tag:
                website_link = website_tag.find_next("a")
                if website_link:
                    website = website_link.get("href", "").strip()

            details = ScrapedDetails(
                unique_id=unique_id,
                owner_id=owner_id,
                access_details=safe_extract("Access Details:"),
                contact_name=safe_extract("Contact Name:"),
                contact_address=safe_extract("Contact Address:"),
                contact_reference=safe_extract("Contact Reference:"),
                telephone_no=safe_extract("Telephone No:"),
                fax_no=safe_extract("Fax Number:"),
                email=safe_extract("Email:"),
                website=website,
            )

            self.stats["details_fetched"] += 1
            return details

        except Exception as e:
            logger.warning(f"Error extracting details for {unique_id}: {e}")
            self.stats["errors"] += 1
            return None

    def scrape_details_batch(
        self, unique_ids: list[str], max_workers: int = None
    ) -> dict[str, ScrapedDetails]:
        """Scrape details for multiple assets in parallel"""
        if max_workers is None:
            max_workers = settings.scrape_max_workers

        results = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id = {
                executor.submit(self.scrape_details, uid): uid for uid in unique_ids
            }

            for future in as_completed(future_to_id):
                uid = future_to_id[future]
                try:
                    details = future.result()
                    if details:
                        results[uid] = details
                except Exception as e:
                    logger.warning(f"Error fetching details for {uid}: {e}")

        return results

    def scrape_all(self) -> list[dict]:
        """
        Perform full scrape: summaries + details.

        Returns list of raw records (dicts) suitable for storage.
        """
        logger.info("Starting full HMRC scrape...")

        # Get summaries
        summaries = self.scrape_summaries()
        if not summaries:
            return []

        # Get details in batches
        logger.info(f"Fetching details for {len(summaries)} assets...")
        unique_ids = [s.unique_id for s in summaries]

        batch_size = settings.scrape_batch_size
        all_details = {}

        for i in range(0, len(unique_ids), batch_size):
            batch = unique_ids[i : i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}...")
            batch_details = self.scrape_details_batch(batch)
            all_details.update(batch_details)

        # Combine summaries and details into raw records
        records = []
        for summary in summaries:
            details = all_details.get(summary.unique_id)

            record = {
                "uniqueID": summary.unique_id,
                "description": summary.description,
                "location": summary.location,
                "category": summary.category,
                "owner_id": details.owner_id if details else None,
                "access_details": details.access_details if details else None,
                "contact_name": details.contact_name if details else None,
                "contact_address": details.contact_address if details else None,
                "contact_reference": details.contact_reference if details else None,
                "telephone_no": details.telephone_no if details else None,
                "fax_no": details.fax_no if details else None,
                "email": details.email if details else None,
                "website": details.website if details else None,
            }
            records.append(record)

        logger.info(f"Scrape complete: {len(records)} records")
        logger.info(f"Stats: {self.stats}")
        return records


def run_scrape_and_update():
    """
    Run a full scrape and update the database.

    This is the main entry point for scheduled scrapes.
    """
    import sys

    sys.path.insert(0, str(__file__).rsplit("/", 2)[0])

    from app.database import get_session
    from app.models import Asset, ChangeEvent, RawSnapshot, SnapshotMetadata, create_tables
    from app.database import engine
    from app.tidying import TidiedAsset, compare_tidied_assets, tidy_raw_record

    logger.info("=" * 60)
    logger.info("SCHEDULED HERITAGE ASSETS SCRAPE")
    logger.info("=" * 60)

    create_tables(engine)
    snapshot_date = date.today()

    with HMRCScraper() as scraper:
        records = scraper.scrape_all()

    if not records:
        logger.error("No records scraped, aborting")
        return {"success": False, "error": "No records scraped"}

    with get_session() as session:
        # Check if already scraped today
        existing = (
            session.query(SnapshotMetadata).filter_by(snapshot_date=snapshot_date).first()
        )
        if existing:
            logger.warning(f"Already scraped today ({snapshot_date}), skipping")
            return {"success": False, "error": "Already scraped today"}

        # Store raw snapshots
        logger.info("Storing raw snapshots...")
        raw_count = 0
        for raw in records:
            unique_id = str(raw.get("uniqueID", ""))
            if not unique_id:
                continue

            snapshot = RawSnapshot(
                snapshot_date=snapshot_date,
                unique_id=unique_id,
                raw_data=raw,
            )
            session.add(snapshot)
            raw_count += 1

        session.flush()
        logger.info(f"Stored {raw_count} raw records")

        # Get current assets
        current_assets = {
            a.unique_id: a
            for a in session.query(Asset).filter(Asset.valid_until.is_(None)).all()
        }
        current_ids = set(current_assets.keys())

        # Process new records
        stats = {"added": 0, "updated": 0, "removed": 0}
        new_ids = set()
        tidied_map = {}

        for raw in records:
            tidied = tidy_raw_record(raw)
            if tidied.unique_id:
                new_ids.add(tidied.unique_id)
                tidied_map[tidied.unique_id] = tidied

        # Import helper to avoid circular import
        from scripts.import_historical import asset_to_tidied, tidied_to_asset

        # Additions
        for uid in new_ids - current_ids:
            tidied = tidied_map[uid]
            asset = tidied_to_asset(tidied, snapshot_date)
            session.add(asset)
            stats["added"] += 1
            session.add(
                ChangeEvent(
                    unique_id=uid,
                    change_type="added",
                    change_date=snapshot_date,
                    summary=f"Asset added: {tidied.description[:100]}...",
                )
            )

        # Removals
        for uid in current_ids - new_ids:
            asset = current_assets[uid]
            asset.valid_until = snapshot_date
            stats["removed"] += 1
            session.add(
                ChangeEvent(
                    unique_id=uid,
                    change_type="removed",
                    change_date=snapshot_date,
                    summary=f"Asset removed: {asset.description[:100]}...",
                )
            )

        # Updates
        for uid in current_ids & new_ids:
            old_asset = current_assets[uid]
            new_tidied = tidied_map[uid]
            old_tidied = asset_to_tidied(old_asset)

            changed_fields = compare_tidied_assets(old_tidied, new_tidied)
            if changed_fields:
                old_asset.valid_until = snapshot_date
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

        # Record metadata
        session.add(
            SnapshotMetadata(
                snapshot_date=snapshot_date,
                source="scrape",
                asset_count=len(records),
                added_count=stats["added"],
                updated_count=stats["updated"],
                removed_count=stats["removed"],
            )
        )

    logger.info(f"Scrape complete: {stats}")
    return {"success": True, "stats": stats}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_scrape_and_update()
    print(result)
