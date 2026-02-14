"""
Shopify Store Scraper — Output Module
=======================================
Merge scrape results with verification results,
generate sorted CSV lead list.

Sorting priority:
  1. has_whatsapp = True + email_verified = safe (highest value leads)
  2. has_whatsapp = True + any email
  3. email_verified = safe (no WhatsApp)
  4. Everything else

Usage:
    from output import generate_csv, merge_results
    generate_csv(scrape_results, verify_results, country="ZA")
"""

import csv
import json
import os
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict

from config import (
    OUTPUT_DIR,
    OUTPUT_CSV_FILENAME,
    CSV_COLUMNS,
)
from scraper import ScrapeResult
from verifier import VerifyResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Merge scrape + verification results
# ---------------------------------------------------------------------------

def merge_results(
    scrape_results: List[ScrapeResult],
    verify_results: Optional[List[VerifyResult]] = None,
) -> List[Dict]:
    """
    Merge scrape results with verification results into flat dicts.
    
    Matches on email address. If no verify_results provided,
    email_verified defaults to empty string.
    
    Returns list of dicts ready for CSV writing.
    """
    # Build verification lookup by email
    verify_map: Dict[str, VerifyResult] = {}
    if verify_results:
        for vr in verify_results:
            verify_map[vr.email.lower()] = vr

    merged = []
    for sr in scrape_results:
        row = {
            "domain": sr.domain,
            "store_name": sr.store_name,
            "country": "",  # Filled by caller
            "email": sr.email or "",
            "email_priority": sr.email_priority if sr.email else "",
            "email_is_free_provider": sr.email_is_free_provider if sr.email else "",
            "has_whatsapp": sr.has_whatsapp,
            "whatsapp_confidence": sr.whatsapp_confidence,
            "whatsapp_phone": sr.whatsapp_phone or "",
            "email_verified": "",
            "scrape_status": sr.scrape_status,
            "discovered_at": "",
            "scraped_at": sr.scraped_at,
        }

        # Merge verification if available
        if sr.email and sr.email.lower() in verify_map:
            vr = verify_map[sr.email.lower()]
            row["email_verified"] = vr.status

        merged.append(row)

    return merged


# ---------------------------------------------------------------------------
# Sorting
# ---------------------------------------------------------------------------

def _lead_sort_key(row: Dict) -> tuple:
    """
    Sort key for lead prioritization.
    
    Priority (lower = better):
      0: WhatsApp + verified safe email
      1: WhatsApp + any email
      2: WhatsApp + no email
      3: Verified safe email (no WhatsApp)
      4: Any email (no WhatsApp)
      5: No email, no WhatsApp
      
    Within each tier, sort by email priority (lower = better).
    """
    has_wa = row.get("has_whatsapp", False)
    has_email = bool(row.get("email"))
    is_verified = row.get("email_verified") == "safe"
    email_priority = row.get("email_priority", 99)

    if isinstance(email_priority, str):
        try:
            email_priority = int(email_priority) if email_priority else 99
        except ValueError:
            email_priority = 99

    if has_wa and has_email and is_verified:
        tier = 0
    elif has_wa and has_email:
        tier = 1
    elif has_wa:
        tier = 2
    elif has_email and is_verified:
        tier = 3
    elif has_email:
        tier = 4
    else:
        tier = 5

    return (tier, email_priority, row.get("domain", ""))


def sort_leads(rows: List[Dict]) -> List[Dict]:
    """Sort leads by priority — WhatsApp + verified first."""
    return sorted(rows, key=_lead_sort_key)


# ---------------------------------------------------------------------------
# CSV generation
# ---------------------------------------------------------------------------

def generate_csv(
    scrape_results: List[ScrapeResult],
    verify_results: Optional[List[VerifyResult]] = None,
    country: str = "ZA",
) -> str:
    """
    Generate the final CSV lead list.
    
    Args:
        scrape_results: Results from scraper.py
        verify_results: Results from verifier.py (optional)
        country: Country code for filename and column
        
    Returns:
        Filepath of the generated CSV
    """
    # Merge
    rows = merge_results(scrape_results, verify_results)

    # Set country
    for row in rows:
        row["country"] = country

    # Filter to successful scrapes only
    rows = [r for r in rows if r["scrape_status"] == "success"]

    # Sort
    rows = sort_leads(rows)

    # Generate filename
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = OUTPUT_CSV_FILENAME.format(country=country.lower(), date=date_str)
    filepath = os.path.join(OUTPUT_DIR, filename)

    # Write CSV
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"Generated CSV: {filepath} ({len(rows)} leads)")

    return filepath


# ---------------------------------------------------------------------------
# Summary stats
# ---------------------------------------------------------------------------

def print_summary(rows: List[Dict]):
    """Print a human-readable summary of the lead list."""
    total = len(rows)
    with_email = sum(1 for r in rows if r.get("email"))
    with_wa = sum(1 for r in rows if r.get("has_whatsapp"))
    with_both = sum(1 for r in rows if r.get("email") and r.get("has_whatsapp"))
    verified_safe = sum(1 for r in rows if r.get("email_verified") == "safe")
    tier_0 = sum(1 for r in rows
                 if r.get("has_whatsapp") and r.get("email") and r.get("email_verified") == "safe")

    print(f"\n{'='*50}")
    print(f"  LEAD SUMMARY")
    print(f"{'='*50}")
    print(f"  Total leads:              {total}")
    print(f"  With email:               {with_email}")
    print(f"  With WhatsApp:            {with_wa}")
    print(f"  With both:                {with_both}")
    print(f"  Email verified (safe):    {verified_safe}")
    print(f"  ★ Top tier (WA+verified): {tier_0}")
    print(f"{'='*50}\n")


# ---------------------------------------------------------------------------
# Load from existing CSV
# ---------------------------------------------------------------------------

def load_csv(filepath: str) -> List[Dict]:
    """Load leads from an existing CSV file."""
    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert string booleans back
            if row.get("has_whatsapp"):
                row["has_whatsapp"] = row["has_whatsapp"].lower() == "true"
            if row.get("email_is_free_provider"):
                row["email_is_free_provider"] = row["email_is_free_provider"].lower() == "true"
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    import argparse
    parser = argparse.ArgumentParser(description="View or re-sort a lead CSV")
    parser.add_argument("csv_file", nargs="?", help="Path to existing CSV file")
    parser.add_argument("--summary", action="store_true", help="Print summary stats")
    args = parser.parse_args()

    if args.csv_file:
        rows = load_csv(args.csv_file)
        if args.summary:
            print_summary(rows)
        else:
            for row in rows[:10]:
                wa = "📱" if row.get("has_whatsapp") else "  "
                email = row.get("email", "—")
                verified = row.get("email_verified", "")
                v_icon = {"safe": "✓", "risky": "⚠", "invalid": "✗"}.get(verified, " ")
                print(f"  {wa} {v_icon} {row['domain']:40s} {email:30s} {row.get('whatsapp_confidence', '')}")
            if len(rows) > 10:
                print(f"  ... and {len(rows) - 10} more")
    else:
        print("Usage: python output.py leads_za_20260214.csv --summary")
