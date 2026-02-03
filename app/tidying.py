"""
Data tidying/normalization for Heritage Assets.

Handles:
- Extracting phone numbers embedded in contact_address
- Normalizing phone number formats
- Parsing addresses into components
- Deduplicating phone numbers between fields
"""

import re
from dataclasses import dataclass
from typing import Optional

# UK postcode regex pattern
UK_POSTCODE_PATTERN = re.compile(
    r"([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})",
    re.IGNORECASE,
)

# Phone number pattern - UK landline/mobile
# Matches: 01onal 234567, 0207 123 4567, 07123 456789, +44 etc
PHONE_PATTERN = re.compile(
    r"(?:(?:\+44|0044)\s*)?(?:0?\d{2,5}[\s\-]?\d{3,4}[\s\-]?\d{3,4})",
    re.IGNORECASE,
)


@dataclass
class TidiedContact:
    """Tidied contact information"""

    contact_name: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    address_city: Optional[str] = None
    address_postcode: Optional[str] = None
    telephone: Optional[str] = None
    fax: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None


@dataclass
class TidiedAsset:
    """Fully tidied asset record"""

    unique_id: str
    owner_id: Optional[str]
    description: str
    location: str
    category: str
    access_details: Optional[str]
    contact: TidiedContact


def normalize_phone(phone: str) -> str:
    """
    Normalize a phone number to consistent format.

    - Remove all spaces
    - Convert +44 to 0
    - Result: 02071234567 or 01onal234567
    """
    if not phone:
        return ""

    # Remove all whitespace and hyphens
    phone = re.sub(r"[\s\-]", "", phone)

    # Convert +44 or 0044 to 0
    phone = re.sub(r"^\+44", "0", phone)
    phone = re.sub(r"^0044", "0", phone)

    # Remove any non-digit characters except leading +
    phone = re.sub(r"[^\d]", "", phone)

    return phone


def extract_phone_from_address(address: str) -> tuple[str, Optional[str]]:
    """
    Extract phone number from end of address string.

    Returns: (address_without_phone, extracted_phone or None)

    Examples:
        "LONDON, EC4A 1LT, 0207 831 9222" -> ("LONDON, EC4A 1LT", "02078319222")
        "BASINGSTOKE, RG21 4EQ, 01256 406300 or 0207 236 4232" -> ("BASINGSTOKE, RG21 4EQ", "01256406300")
    """
    if not address:
        return address, None

    # Find postcode position
    postcode_match = None
    for match in UK_POSTCODE_PATTERN.finditer(address):
        postcode_match = match  # Get last postcode match

    if not postcode_match:
        # No postcode found - try to extract phone from end anyway
        phone_match = PHONE_PATTERN.search(address)
        if phone_match and address.endswith(phone_match.group()):
            phone = normalize_phone(phone_match.group())
            clean_address = address[: phone_match.start()].rstrip(", ")
            return clean_address, phone
        return address, None

    # Look for phone number after postcode
    after_postcode = address[postcode_match.end() :]

    # Handle "or" case - take first number
    phone_matches = list(PHONE_PATTERN.finditer(after_postcode))
    if phone_matches:
        first_phone = normalize_phone(phone_matches[0].group())
        # Clean address: everything up to and including postcode
        clean_address = address[: postcode_match.end()].rstrip(", ")
        return clean_address, first_phone

    return address, None


def parse_address(address: str) -> dict:
    """
    Parse a UK address into components.

    Input format typically: "ORG, BUILDING, STREET, CITY, COUNTY, POSTCODE"

    Returns dict with:
        - line1: First line (org/building)
        - line2: Street address
        - city: City/town
        - postcode: Postcode
    """
    result = {
        "line1": None,
        "line2": None,
        "city": None,
        "postcode": None,
    }

    if not address:
        return result

    # First extract postcode
    postcode_match = UK_POSTCODE_PATTERN.search(address)
    if postcode_match:
        result["postcode"] = postcode_match.group().upper().strip()
        # Remove postcode from address for further parsing
        address = address[: postcode_match.start()].rstrip(", ")

    # Split by comma
    parts = [p.strip() for p in address.split(",") if p.strip()]

    if not parts:
        return result

    if len(parts) == 1:
        result["line1"] = parts[0]
    elif len(parts) == 2:
        result["line1"] = parts[0]
        result["city"] = parts[1]
    elif len(parts) == 3:
        result["line1"] = parts[0]
        result["line2"] = parts[1]
        result["city"] = parts[2]
    elif len(parts) >= 4:
        # Join first parts as line1, take second-to-last as street, last as city
        # But if last part looks like a county, use second-to-last as city
        county_indicators = [
            "SHIRE",
            "YORKSHIRE",
            "LANCASHIRE",
            "CORNWALL",
            "DEVON",
            "DORSET",
            "SUFFOLK",
            "NORFOLK",
            "SUSSEX",
            "KENT",
            "ESSEX",
            "SURREY",
            "BERKSHIRE",
            "HAMPSHIRE",
            "WILTSHIRE",
            "SOMERSET",
            "GLOUCESTERSHIRE",
        ]

        last_part = parts[-1].upper()
        is_county = any(county in last_part for county in county_indicators)

        if is_county and len(parts) >= 4:
            # Last is county, second-to-last is city
            result["line1"] = parts[0]
            result["line2"] = ", ".join(parts[1:-2])
            result["city"] = parts[-2]
        else:
            result["line1"] = parts[0]
            result["line2"] = ", ".join(parts[1:-1])
            result["city"] = parts[-1]

    return result


def dedupe_phone(
    address_phone: Optional[str],
    telephone_field: Optional[str],
    access_phone: Optional[str],
) -> Optional[str]:
    """
    Deduplicate phone numbers from multiple sources.

    Priority: telephone_field > access_phone > address_phone
    But return normalized version.
    """
    candidates = [telephone_field, access_phone, address_phone]
    normalized = [normalize_phone(p) for p in candidates if p]

    if not normalized:
        return None

    # Return first non-empty normalized phone
    for phone in normalized:
        if phone and len(phone) >= 10:  # Valid UK phone is 10-11 digits
            return phone

    return normalized[0] if normalized else None


def tidy_raw_record(raw: dict) -> TidiedAsset:
    """
    Transform a raw HMRC record into a tidied asset.

    Expected raw keys (from CSV):
        uniqueID, description, location, category, access_details,
        appointment, opening_times, contact_name, contact_address,
        access_phone, telephone_no, fax_no, email, website

    Some records may also have: owner_id (added during scraping)
    """
    # Extract phone from address
    contact_address = raw.get("contact_address", "") or ""
    clean_address, address_phone = extract_phone_from_address(contact_address)

    # Parse address into components
    parsed_address = parse_address(clean_address)

    # Deduplicate phone numbers
    telephone = dedupe_phone(
        address_phone,
        raw.get("telephone_no"),
        raw.get("access_phone"),
    )

    # Normalize fax
    fax = normalize_phone(raw.get("fax_no", "") or "")

    # Build tidied contact
    contact = TidiedContact(
        contact_name=_clean_string(raw.get("contact_name")),
        address_line1=parsed_address["line1"],
        address_line2=parsed_address["line2"],
        address_city=parsed_address["city"],
        address_postcode=parsed_address["postcode"],
        telephone=telephone if telephone else None,
        fax=fax if fax else None,
        email=_clean_string(raw.get("email")),
        website=_clean_string(raw.get("website")),
    )

    # Build tidied asset
    return TidiedAsset(
        unique_id=str(raw.get("uniqueID", raw.get("unique_id", ""))),
        owner_id=_clean_string(raw.get("owner_id")),
        description=_clean_string(raw.get("description")) or "",
        location=_clean_string(raw.get("location")) or "",
        category=_clean_string(raw.get("category")) or "",
        access_details=_clean_string(raw.get("access_details")),
        contact=contact,
    )


def _clean_string(value) -> Optional[str]:
    """Clean a string value - strip whitespace, convert empty to None"""
    if value is None:
        return None
    if isinstance(value, float):
        # Handle pandas NaN
        import math

        if math.isnan(value):
            return None
        return str(int(value)) if value == int(value) else str(value)
    s = str(value).strip()
    return s if s else None


def compare_tidied_assets(old: TidiedAsset, new: TidiedAsset) -> list[str]:
    """
    Compare two tidied assets and return list of changed field names.

    Returns empty list if assets are identical.
    """
    changed = []

    # Compare main fields
    simple_fields = ["owner_id", "description", "location", "category", "access_details"]
    for field in simple_fields:
        old_val = getattr(old, field)
        new_val = getattr(new, field)
        if old_val != new_val:
            changed.append(field)

    # Compare contact fields
    contact_fields = [
        "contact_name",
        "address_line1",
        "address_line2",
        "address_city",
        "address_postcode",
        "telephone",
        "fax",
        "email",
        "website",
    ]
    for field in contact_fields:
        old_val = getattr(old.contact, field)
        new_val = getattr(new.contact, field)
        if old_val != new_val:
            changed.append(field)

    return changed
