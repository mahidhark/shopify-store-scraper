"""
Shopify Store Scraper — Main Pipeline
=======================================
Orchestrates the full pipeline:
  1. Discover stores (Google dorking)
  2. Scrape stores (email + WhatsApp extraction)
  3. Verify emails (Reacher)
  4. Generate CSV (sorted lead list)

Can run full pipeline or individual steps.

Usage:
    python main.py                          # Full pipeline
    python main.py discover                 # Step 1 only
    python main.py scrape                   # Step 2 only
    python main.py verify                   # Step 3 only
    python main.py export                   # Step 4 only
    python main.py --dry-run                # Preview without executing
    python main.py --country ZA             # Target country
    python main.py --batch-size 5           # Queries per batch
    python main.py --no-playwright          # Skip Playwright fallback
    python main.py --skip-verify            # Skip email verification
"""

import json
import os
import sys
import logging
from datetime import datetime, timezone
from typing import List, Optional

from config import (
    get_enabled_countries,
    DATA_DIR,
    OUTPUT_DIR,
    DORK_BATCH_SIZE,
    DISCOVERED_STORES_FILE,
)
from discovery import discover_stores
from scraper import scrape_store, scrape_stores_batch, ScrapeResult
from verifier import verify_emails_batch, VerifyResult
from output import generate_csv, merge_results, print_summary

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def step_discover(
    batch_size: int = DORK_BATCH_SIZE,
    include_niche: bool = True,
    dry_run: bool = False,
) -> List[str]:
    """
    Step 1: Discover Shopify store domains via Google dorking.
    Returns list of newly discovered domains.
    """
    logger.info("=" * 60)
    logger.info("STEP 1: DISCOVER STORES")
    logger.info("=" * 60)

    new_domains = discover_stores(
        max_queries=batch_size,
        include_niche=include_niche,
        dry_run=dry_run,
    )

    logger.info(f"Discovered {len(new_domains)} new domains")
    return new_domains


def step_scrape(
    domains: Optional[List[str]] = None,
    use_playwright: bool = True,
    country: str = "ZA",
) -> List[ScrapeResult]:
    """
    Step 2: Scrape store domains for email + WhatsApp.
    
    If no domains provided, loads unscraped domains from discovered stores file.
    Returns list of ScrapeResults.
    """
    logger.info("=" * 60)
    logger.info("STEP 2: SCRAPE STORES")
    logger.info("=" * 60)

    if domains is None:
        domains = _load_unscraped_domains(country)

    if not domains:
        logger.info("No domains to scrape")
        return []

    logger.info(f"Scraping {len(domains)} domains...")
    results = scrape_stores_batch(domains, use_playwright_fallback=use_playwright)

    # Mark domains as scraped
    _mark_domains_scraped(domains, country)

    return results


def step_verify(
    scrape_results: Optional[List[ScrapeResult]] = None,
    emails: Optional[List[str]] = None,
) -> List[VerifyResult]:
    """
    Step 3: Verify emails using Reacher.
    
    Accepts either ScrapeResults (extracts emails) or a direct email list.
    Returns list of VerifyResults.
    """
    logger.info("=" * 60)
    logger.info("STEP 3: VERIFY EMAILS")
    logger.info("=" * 60)

    if emails is None and scrape_results is not None:
        emails = [r.email for r in scrape_results if r.email and r.scrape_status == "success"]

    if not emails:
        logger.info("No emails to verify")
        return []

    # Deduplicate
    unique_emails = list(dict.fromkeys(emails))
    logger.info(f"Verifying {len(unique_emails)} unique emails...")

    return verify_emails_batch(unique_emails)


def step_export(
    scrape_results: List[ScrapeResult],
    verify_results: Optional[List[VerifyResult]] = None,
    country: str = "ZA",
) -> str:
    """
    Step 4: Generate sorted CSV lead list.
    Returns filepath of generated CSV.
    """
    logger.info("=" * 60)
    logger.info("STEP 4: EXPORT CSV")
    logger.info("=" * 60)

    filepath = generate_csv(scrape_results, verify_results, country=country)

    # Print summary
    rows = merge_results(scrape_results, verify_results)
    rows = [r for r in rows if r["scrape_status"] == "success"]
    print_summary(rows)

    return filepath


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    batch_size: int = DORK_BATCH_SIZE,
    include_niche: bool = True,
    use_playwright: bool = True,
    skip_verify: bool = False,
    dry_run: bool = False,
    country: str = "ZA",
    domains: Optional[List[str]] = None,
) -> Optional[str]:
    """
    Run the full pipeline: discover -> scrape -> verify -> export.
    
    Args:
        batch_size: Number of Google dork queries per batch
        include_niche: Include niche-specific dork queries
        use_playwright: Use Playwright fallback for email extraction
        skip_verify: Skip email verification step
        dry_run: Preview without executing
        country: Target country code
        domains: Override with specific domains (skip discovery)
        
    Returns:
        Filepath of generated CSV, or None if dry run
    """
    start_time = datetime.now(timezone.utc)
    logger.info(f"Pipeline started at {start_time.strftime('%H:%M:%S UTC')}")
    logger.info(f"Country: {country} | Batch size: {batch_size} | Playwright: {use_playwright}")

    # Step 1: Discover (skip if domains provided)
    if domains:
        logger.info(f"Using {len(domains)} provided domains (skipping discovery)")
    else:
        new_domains = step_discover(
            batch_size=batch_size,
            include_niche=include_niche,
            dry_run=dry_run,
        )
        if dry_run:
            logger.info("Dry run complete")
            return None
        domains = new_domains if new_domains else _load_unscraped_domains(country)

    if not domains:
        logger.info("No domains to process. Run discovery first or provide domains.")
        return None

    # Step 2: Scrape
    scrape_results = step_scrape(
        domains=domains,
        use_playwright=use_playwright,
        country=country,
    )

    if not scrape_results:
        logger.info("No scrape results. Nothing to export.")
        return None

    # Step 3: Verify (optional)
    verify_results = None
    if not skip_verify:
        verify_results = step_verify(scrape_results=scrape_results)
    else:
        logger.info("Skipping email verification (--skip-verify)")

    # Step 4: Export
    filepath = step_export(scrape_results, verify_results, country=country)

    # Done
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info(f"Pipeline complete in {elapsed:.1f}s")
    logger.info(f"Output: {filepath}")

    return filepath


# ---------------------------------------------------------------------------
# Helper: load/mark unscraped domains
# ---------------------------------------------------------------------------

def _load_unscraped_domains(country: str = "ZA") -> List[str]:
    """Load domains that haven't been scraped yet from discovered stores file."""
    filepath = os.path.join(OUTPUT_DIR, f"discovered_stores_{country.lower()}.json")

    if not os.path.exists(filepath):
        logger.info(f"No discovered stores file found: {filepath}")
        return []

    try:
        with open(filepath, "r") as f:
            stores = json.load(f)
    except (json.JSONDecodeError, ValueError):
        logger.warning(f"Corrupted stores file: {filepath}")
        return []

    unscraped = [s["domain"] for s in stores if not s.get("scraped", False)]
    logger.info(f"Found {len(unscraped)} unscraped domains (of {len(stores)} total)")
    return unscraped


def _mark_domains_scraped(domains: List[str], country: str = "ZA"):
    """Mark domains as scraped in the discovered stores file."""
    filepath = os.path.join(OUTPUT_DIR, f"discovered_stores_{country.lower()}.json")

    if not os.path.exists(filepath):
        return

    try:
        with open(filepath, "r") as f:
            stores = json.load(f)
    except (json.JSONDecodeError, ValueError):
        return

    scraped_set = set(domains)
    for store in stores:
        if store["domain"] in scraped_set:
            store["scraped"] = True

    with open(filepath, "w") as f:
        json.dump(stores, f, indent=2)

    logger.debug(f"Marked {len(scraped_set)} domains as scraped")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    import argparse
    parser = argparse.ArgumentParser(
        description="Shopify Store Scraper — Find leads with WhatsApp + email",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                        Full pipeline (discover+scrape+verify+export)
  python main.py discover --dry-run     Preview dork queries
  python main.py discover               Run discovery batch
  python main.py scrape                 Scrape unscraped domains
  python main.py verify                 Verify emails from last scrape
  python main.py export                 Re-export CSV from existing data
  python main.py --domains store1.co.za store2.co.za   Scrape specific stores
  python main.py --skip-verify          Skip Reacher verification
        """,
    )

    parser.add_argument("step", nargs="?", default="all",
                        choices=["all", "discover", "scrape", "verify", "export"],
                        help="Pipeline step to run (default: all)")
    parser.add_argument("--batch-size", type=int, default=DORK_BATCH_SIZE,
                        help=f"Dork queries per batch (default: {DORK_BATCH_SIZE})")
    parser.add_argument("--no-niche", action="store_true",
                        help="Skip niche-specific dork queries")
    parser.add_argument("--no-playwright", action="store_true",
                        help="Disable Playwright fallback")
    parser.add_argument("--skip-verify", action="store_true",
                        help="Skip email verification")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without executing")
    parser.add_argument("--country", type=str, default="ZA",
                        help="Target country code (default: ZA)")
    parser.add_argument("--domains", nargs="*",
                        help="Specific domains to scrape (skip discovery)")
    parser.add_argument("--from-file", type=str,
                        help="Read domains from file (one per line)")

    args = parser.parse_args()

    # Collect domains from args
    domains = None
    if args.domains:
        domains = args.domains
    if args.from_file:
        domains = domains or []
        with open(args.from_file) as f:
            domains.extend(line.strip() for line in f if line.strip())

    # Run appropriate step
    if args.step == "all":
        run_pipeline(
            batch_size=args.batch_size,
            include_niche=not args.no_niche,
            use_playwright=not args.no_playwright,
            skip_verify=args.skip_verify,
            dry_run=args.dry_run,
            country=args.country,
            domains=domains,
        )

    elif args.step == "discover":
        step_discover(
            batch_size=args.batch_size,
            include_niche=not args.no_niche,
            dry_run=args.dry_run,
        )

    elif args.step == "scrape":
        step_scrape(
            domains=domains,
            use_playwright=not args.no_playwright,
            country=args.country,
        )

    elif args.step == "verify":
        # Load last scrape results — for now just load emails from CSV
        logger.info("Loading emails from discovered stores...")
        unscraped = _load_unscraped_domains(args.country)
        if not unscraped:
            logger.info("No domains found. Run scrape first.")
        # Placeholder — in practice, you'd load from the last scrape output

    elif args.step == "export":
        logger.info("Re-export not yet implemented. Run full pipeline instead.")


if __name__ == "__main__":
    main()
