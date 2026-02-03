#!/usr/bin/env python3
"""
Scrape Land & Buildings and Collections from HMRC heritage database.

These are separate databases from Works of Art and include undertakings
(legal text about public access obligations).
"""

import argparse
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import engine, get_session
from app.models import LandBuilding, create_tables, rebuild_land_buildings_fts_index

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BASE_URL = "http://www.visitukheritage.gov.uk"
REGIONS = list(range(1, 14))  # 1-13


def get_ids_for_region(client: httpx.Client, region: int, is_collection: bool) -> list[str]:
    """Get all item IDs from a region listing page."""
    colflag = "Y" if is_collection else "N"
    url = f"{BASE_URL}/servlet/com.eds.ir.cto.servlet.CtoLandDbQueryServlet?region={region}&colflag={colflag}"

    response = client.get(url, follow_redirects=True)
    response.raise_for_status()

    # Extract IDs from CtoLandDetailServlet links
    ids = re.findall(r"CtoLandDetailServlet\?ID=(\d+)", response.text)
    return list(set(ids))  # Deduplicate


def parse_detail_page(html: str) -> dict:
    """Parse a Land & Buildings detail page."""
    soup = BeautifulSoup(html, "html.parser")
    data = {}

    # Field mappings from page labels to dict keys
    field_map = {
        "Country:": "country",
        "Name of Property:": "name",
        "Description:": "description",
        "Access Details:": "access_details",
        "OS Grid Ref:": "os_grid_ref",
        "Contact Name:": "contact_name",
        "Contact Address:": "contact_address",
        "Telephone No:": "telephone",
        "Fax Number:": "fax",
        "Email:": "email",
    }

    # Parse table rows
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            label_cell = cells[1] if len(cells) > 2 else cells[0]
            value_cell = cells[2] if len(cells) > 2 else cells[1]

            label = label_cell.get_text(strip=True)
            for page_label, key in field_map.items():
                if page_label in label:
                    data[key] = value_cell.get_text(strip=True)
                    break

    # Extract website from link
    website_link = soup.find("a", href=lambda h: h and not h.startswith("/") and not h.startswith("javascript") and "hmrc.gov.uk" not in h)
    if website_link and website_link.get("href"):
        href = website_link.get("href")
        if href.startswith("http"):
            data["website"] = href

    return data


def parse_undertakings_page(html: str) -> str:
    """Parse the undertakings page to extract the legal text."""
    soup = BeautifulSoup(html, "html.parser")

    # Find the row with "Principal Undertakings:" label - content is in next cell
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 3:
            label = cells[1].get_text(strip=True)
            if "Principal Undertakings:" in label:
                # Content is in the third cell
                undertakings = cells[2].get_text(separator="\n", strip=True)
                # Clean up whitespace
                undertakings = re.sub(r"\n\s*\n", "\n\n", undertakings)
                return undertakings

    return ""


def check_map_exists(client: httpx.Client, item_id: str) -> bool:
    """Check if a map image exists for this item."""
    map_url = f"{BASE_URL}/images/{item_id}.jpg"
    try:
        response = client.head(map_url, follow_redirects=True)
        return response.status_code == 200
    except Exception:
        return False


def scrape_item(client: httpx.Client, item_id: str, delay: float) -> tuple[dict, str, bool]:
    """Scrape detail page, undertakings, and check for map for one item."""
    # Fetch detail page
    detail_url = f"{BASE_URL}/servlet/com.eds.ir.cto.servlet.CtoLandDetailServlet?ID={item_id}"
    response = client.get(detail_url, follow_redirects=True)
    response.raise_for_status()
    data = parse_detail_page(response.text)

    time.sleep(delay)

    # Fetch undertakings page
    undertakings_url = f"{BASE_URL}/servlet/com.eds.ir.cto.servlet.CtoLandPrinUnderServlet?ID={item_id}"
    response = client.get(undertakings_url, follow_redirects=True)
    response.raise_for_status()
    undertakings = parse_undertakings_page(response.text)

    time.sleep(delay)

    # Check if map exists (HEAD request, no delay needed - very fast)
    has_map = check_map_exists(client, item_id)

    return data, undertakings, has_map


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Land & Buildings and Collections from HMRC"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between requests in seconds (default: 0.5)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Just count items, don't scrape",
    )
    parser.add_argument(
        "--type",
        choices=["land_building", "collection", "both"],
        default="both",
        help="Type to scrape (default: both)",
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("SCRAPE LAND & BUILDINGS / COLLECTIONS")
    logger.info(f"Delay: {args.delay}s")
    logger.info(f"Type: {args.type}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("=" * 60)

    create_tables(engine)

    client = httpx.Client(timeout=30.0)

    # Collect all IDs first
    all_items = []  # [(id, is_collection), ...]

    types_to_scrape = []
    if args.type in ("land_building", "both"):
        types_to_scrape.append(("Land & Buildings", False))
    if args.type in ("collection", "both"):
        types_to_scrape.append(("Collections", True))

    for type_name, is_collection in types_to_scrape:
        logger.info(f"\nCollecting {type_name} IDs...")
        type_ids = []
        for region in REGIONS:
            ids = get_ids_for_region(client, region, is_collection)
            type_ids.extend(ids)
            logger.info(f"  Region {region}: {len(ids)} items")
            time.sleep(0.2)  # Small delay between region requests

        # Deduplicate
        type_ids = list(set(type_ids))
        logger.info(f"Total {type_name}: {len(type_ids)} unique items")

        for item_id in type_ids:
            all_items.append((item_id, is_collection))

    logger.info(f"\nTotal items to scrape: {len(all_items)}")

    if args.dry_run:
        estimated_time = len(all_items) * args.delay * 2  # 2 requests per item
        logger.info(f"Estimated scrape time: {estimated_time / 60:.1f} minutes")
        logger.info("DRY RUN - no scraping performed")
        return

    # Check existing items
    with get_session() as session:
        existing_ids = set(
            row[0] for row in session.query(LandBuilding.unique_id).all()
        )
    logger.info(f"Already in database: {len(existing_ids)} items")

    # Filter to new items only
    new_items = [(id, is_col) for id, is_col in all_items if id not in existing_ids]
    logger.info(f"New items to scrape: {len(new_items)}")

    if not new_items:
        logger.info("Nothing new to scrape!")
        return

    # Scrape
    scraped = 0
    errors = 0
    start_time = datetime.now()

    with get_session() as session:
        for i, (item_id, is_collection) in enumerate(new_items):
            item_type = "collection" if is_collection else "land_building"
            try:
                data, undertakings, has_map = scrape_item(client, item_id, args.delay)

                lb = LandBuilding(
                    unique_id=item_id,
                    item_type=item_type,
                    country=data.get("country", "Unknown"),
                    name=data.get("name", ""),
                    description=data.get("description"),
                    access_details=data.get("access_details"),
                    os_grid_ref=data.get("os_grid_ref"),
                    contact_name=data.get("contact_name"),
                    contact_address=data.get("contact_address"),
                    telephone=data.get("telephone"),
                    fax=data.get("fax"),
                    email=data.get("email"),
                    website=data.get("website"),
                    undertakings=undertakings if undertakings else None,
                    has_map=has_map,
                )
                session.add(lb)
                scraped += 1

                # Commit every 50 items
                if scraped % 50 == 0:
                    session.commit()
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = scraped / elapsed * 60
                    remaining = (len(new_items) - i - 1) / rate if rate > 0 else 0
                    logger.info(
                        f"Progress: {i + 1}/{len(new_items)} ({rate:.1f}/min, ~{remaining:.1f} min remaining)"
                    )

            except Exception as e:
                logger.error(f"Error scraping {item_id}: {e}")
                errors += 1
                continue

        # Final commit
        session.commit()

    logger.info("=" * 60)
    logger.info("RESULTS")
    logger.info("=" * 60)
    logger.info(f"Scraped: {scraped}")
    logger.info(f"Errors: {errors}")

    # Rebuild FTS index
    logger.info("Rebuilding FTS index...")
    rebuild_land_buildings_fts_index(engine)
    logger.info("Done!")


if __name__ == "__main__":
    main()
