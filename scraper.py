"""
Shopify Store Scraper — Scraper Module
=======================================
Visit discovered store domains, extract contact emails,
and detect WhatsApp presence.

Hybrid approach:
  1. Fast requests + BeautifulSoup first pass
  2. Playwright headless browser retry for stores with no email

Usage:
    from scraper import scrape_store, scrape_stores_batch
    result = scrape_store("mystore.co.za")
    results = scrape_stores_batch(["store1.co.za", "store2.co.za"])
"""

import re
import random
import time
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from config import (
    CONTACT_PAGE_PATHS,
    SCRAPE_DELAY_MIN,
    SCRAPE_DELAY_MAX,
    SCRAPE_MAX_RETRIES,
    SCRAPE_TIMEOUT,
    PLAYWRIGHT_TIMEOUT,
    PLAYWRIGHT_ENABLED,
    USER_AGENTS,
    EMAIL_REGEX,
    JUNK_EMAIL_PATTERNS,
    FREE_EMAIL_PROVIDERS,
    EMAIL_PRIORITY,
    DEFAULT_EMAIL_PRIORITY,
    WHATSAPP_DEFINITIVE_PATTERNS,
    WHATSAPP_WIDGET_PATTERNS,
    WHATSAPP_WEAK_PATTERNS,
    WHATSAPP_PHONE_REGEX,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes for scrape results
# ---------------------------------------------------------------------------

@dataclass
class WhatsAppResult:
    """WhatsApp detection result."""
    found: bool = False
    confidence: str = "none"    # "definitive" | "widget" | "maybe" | "none"
    phone: Optional[str] = None


@dataclass
class EmailResult:
    """Email extraction result."""
    email: Optional[str] = None
    priority: int = 99
    is_free_provider: bool = False
    source_page: str = ""       # Which page the email was found on


@dataclass
class ScrapeResult:
    """Complete scrape result for a single store."""
    domain: str
    store_name: str = ""
    email: Optional[str] = None
    email_priority: int = 99
    email_is_free_provider: bool = False
    has_whatsapp: bool = False
    whatsapp_confidence: str = "none"
    whatsapp_phone: Optional[str] = None
    scrape_status: str = "pending"   # "success" | "failed" | "skipped"
    error: str = ""
    scraped_at: str = ""


# ---------------------------------------------------------------------------
# Email extraction
# ---------------------------------------------------------------------------

def _is_junk_email(email: str) -> bool:
    """Check if an email matches junk patterns or is a false positive."""
    email_lower = email.lower()

    # Filter out image/asset filenames matched by email regex
    # e.g. "logo_100x@2x.png", "photo_580x@2x.webp"
    image_extensions = ('.png', '.jpg', '.jpeg', '.webp', '.gif', '.svg', '.css', '.js')
    if any(email_lower.endswith(ext) for ext in image_extensions):
        return True

    # Filter Shopify image sizing patterns: @2x, _100x, _580x, etc.
    if '@2x.' in email_lower or re.search(r'_\d+x@', email_lower) or re.search(r'_\d+x\d*\.', email_lower):
        return True

    # Filter placeholder/test emails
    junk_exact = ('xxx@xxx.xxx', 'name@email.com', 'monique@email.com', 'admin@example.com', 'email@example.com', 'your@email.com')
    if email_lower in junk_exact:
        return True

    for pattern in JUNK_EMAIL_PATTERNS:
        if pattern.lower() in email_lower:
            return True
    return False


def _is_free_provider(email: str) -> bool:
    """Check if email is from a free provider (gmail, yahoo, etc)."""
    domain = email.lower().split("@")[-1]
    return domain in FREE_EMAIL_PROVIDERS


def _get_email_priority(email: str) -> int:
    """
    Score an email by its local part prefix.
    Lower number = higher priority.
    owner@ > hello@ > contact@ > info@ > sales@ > support@ > admin@
    """
    local_part = email.lower().split("@")[0]
    for prefix, priority in EMAIL_PRIORITY.items():
        if prefix in local_part:
            return priority
    return DEFAULT_EMAIL_PRIORITY


def extract_emails(html: str) -> List[EmailResult]:
    """
    Extract and rank emails from HTML content.
    
    - Finds all email patterns via regex
    - Filters out junk emails
    - Ranks by priority (owner > hello > info > support)
    - Flags free providers
    - Returns sorted list (best email first)
    """
    raw_emails = re.findall(EMAIL_REGEX, html)

    # Deduplicate (case-insensitive)
    seen = set()
    unique_emails = []
    for email in raw_emails:
        lower = email.lower()
        if lower not in seen:
            seen.add(lower)
            unique_emails.append(email)

    # Filter and rank
    results = []
    for email in unique_emails:
        if _is_junk_email(email):
            logger.debug(f"  Filtered junk email: {email}")
            continue

        results.append(EmailResult(
            email=email.lower(),
            priority=_get_email_priority(email),
            is_free_provider=_is_free_provider(email),
        ))

    # Sort by priority (lowest number first), then prefer non-free providers
    results.sort(key=lambda r: (r.priority, r.is_free_provider))

    return results


# ---------------------------------------------------------------------------
# WhatsApp detection
# ---------------------------------------------------------------------------

def detect_whatsapp(html: str) -> WhatsAppResult:
    """
    Detect WhatsApp presence in HTML with 3-tier confidence.
    
    Tier 1 (definitive): wa.me/ links, api.whatsapp.com/send
    Tier 2 (widget): Known widget class names / widget providers
    Tier 3 (maybe): Just the word "whatsapp" somewhere
    
    Also extracts phone number from wa.me links if found.
    """
    html_lower = html.lower()

    # Tier 1: Definitive patterns
    for pattern in WHATSAPP_DEFINITIVE_PATTERNS:
        if pattern.lower() in html_lower:
            # Try to extract phone number
            phone_match = re.search(WHATSAPP_PHONE_REGEX, html)
            phone = phone_match.group(1) if phone_match else None
            logger.debug(f"  WhatsApp DEFINITIVE: pattern='{pattern}', phone={phone}")
            return WhatsAppResult(found=True, confidence="definitive", phone=phone)

    # Tier 2: Widget patterns
    for pattern in WHATSAPP_WIDGET_PATTERNS:
        if pattern.lower() in html_lower:
            logger.debug(f"  WhatsApp WIDGET: pattern='{pattern}'")
            return WhatsAppResult(found=True, confidence="widget")

    # Tier 3: Weak signal
    for pattern in WHATSAPP_WEAK_PATTERNS:
        if pattern.lower() in html_lower:
            logger.debug(f"  WhatsApp MAYBE: pattern='{pattern}'")
            return WhatsAppResult(found=True, confidence="maybe")

    return WhatsAppResult(found=False, confidence="none")


# ---------------------------------------------------------------------------
# Page fetching
# ---------------------------------------------------------------------------

def _get_random_headers() -> dict:
    """Return request headers with a random user agent."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }


def _fetch_page_requests(url: str) -> Optional[str]:
    """
    Fetch a page using requests library.
    Returns HTML string or None on failure.
    """
    try:
        resp = requests.get(
            url,
            headers=_get_random_headers(),
            timeout=SCRAPE_TIMEOUT,
            allow_redirects=True,
        )

        # Skip non-OK responses
        if resp.status_code != 200:
            logger.debug(f"  HTTP {resp.status_code} for {url}")
            return None

        return resp.text
    except requests.RequestException as e:
        logger.debug(f"  Request failed for {url}: {e}")
        return None


def _fetch_page_playwright(url: str) -> Optional[str]:
    """
    Fetch a page using Playwright headless browser.
    Used as fallback when requests doesn't find an email.
    Returns rendered HTML string or None on failure.
    """
    if not PLAYWRIGHT_ENABLED:
        return None

    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=random.choice(USER_AGENTS))
            page.goto(url, timeout=PLAYWRIGHT_TIMEOUT, wait_until="networkidle")
            html = page.content()
            browser.close()
            return html
    except ImportError:
        logger.warning("Playwright not installed. Skipping JS rendering.")
        return None
    except Exception as e:
        logger.debug(f"  Playwright failed for {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Store name extraction
# ---------------------------------------------------------------------------

def _extract_store_name(html: str) -> str:
    """Extract store name from page title or og:site_name."""
    soup = BeautifulSoup(html, "html.parser")

    # Try og:site_name first (most reliable for Shopify)
    og_tag = soup.find("meta", property="og:site_name")
    if og_tag and og_tag.get("content"):
        return og_tag["content"].strip()

    # Fall back to <title> tag
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
        # Remove common suffixes
        for suffix in [" – Powered by Shopify", " - Powered by Shopify",
                       " | Powered by Shopify", " – Home", " - Home"]:
            if title.endswith(suffix):
                title = title[:-len(suffix)]
        return title.strip()

    return ""


# ---------------------------------------------------------------------------
# Shopify detection (Edge Case 5: skip non-Shopify / dead stores)
# ---------------------------------------------------------------------------

def _is_shopify_store(html: str) -> bool:
    """Check if the page is actually a Shopify store."""
    indicators = [
        "cdn.shopify.com",
        "shopify.com/s/",
        "Shopify.theme",
        "myshopify.com",
        "shopify-section",
    ]
    html_lower = html.lower()
    return any(ind.lower() in html_lower for ind in indicators)


def _is_password_protected(html: str) -> bool:
    """Check if store is password-protected (Edge Case 5)."""
    indicators = [
        "password-page",
        "storefront-password",
        "opening soon",
        "store is not available",
    ]
    html_lower = html.lower()
    return any(ind in html_lower for ind in indicators)


# ---------------------------------------------------------------------------
# Main scrape function for a single store
# ---------------------------------------------------------------------------

def scrape_store(domain: str, use_playwright_fallback: bool = True) -> ScrapeResult:
    """
    Scrape a single Shopify store for email and WhatsApp.
    
    Strategy:
      1. Fetch homepage with requests
      2. Check if it's a real Shopify store (skip if not)
      3. Check if password-protected (skip if yes)
      4. Extract store name, emails, WhatsApp from homepage
      5. Fetch contact pages for more emails
      6. If no email found and Playwright enabled, retry with headless browser
      7. Return best email + WhatsApp result
    
    Args:
        domain: Clean domain string (e.g., "mystore.co.za")
        use_playwright_fallback: Try Playwright if requests finds no email
        
    Returns:
        ScrapeResult with all extracted data
    """
    result = ScrapeResult(
        domain=domain,
        scraped_at=datetime.now(timezone.utc).isoformat(),
    )

    base_url = f"https://{domain}"
    all_html_parts = []       # Collect HTML from all pages
    best_emails: List[EmailResult] = []
    best_whatsapp = WhatsAppResult()

    # --- Step 1: Fetch homepage ---
    logger.info(f"Scraping {domain}...")

    homepage_html = None
    for attempt in range(SCRAPE_MAX_RETRIES):
        homepage_html = _fetch_page_requests(base_url)
        if homepage_html:
            break
        logger.debug(f"  Retry {attempt + 1}/{SCRAPE_MAX_RETRIES} for {base_url}")
        time.sleep(1)

    if not homepage_html:
        logger.warning(f"  Failed to fetch homepage for {domain}")
        result.scrape_status = "failed"
        result.error = "homepage_unreachable"
        return result

    # --- Step 2: Shopify check ---
    if not _is_shopify_store(homepage_html):
        logger.info(f"  Not a Shopify store: {domain}")
        result.scrape_status = "skipped"
        result.error = "not_shopify"
        return result

    # --- Step 3: Password check ---
    if _is_password_protected(homepage_html):
        logger.info(f"  Password-protected: {domain}")
        result.scrape_status = "skipped"
        result.error = "password_protected"
        return result

    # --- Step 4: Extract from homepage ---
    result.store_name = _extract_store_name(homepage_html)
    all_html_parts.append(homepage_html)

    # --- Step 5: Fetch contact pages ---
    for path in CONTACT_PAGE_PATHS:
        contact_url = urljoin(base_url, path)
        html = _fetch_page_requests(contact_url)
        if html:
            all_html_parts.append(html)
            logger.debug(f"  Found contact page: {path}")

        # Small delay between contact page requests
        time.sleep(0.3)

    # --- Step 6: Extract emails and WhatsApp from all collected HTML ---
    combined_html = "\n".join(all_html_parts)

    best_emails = extract_emails(combined_html)
    best_whatsapp = detect_whatsapp(combined_html)

    # --- Step 7: Playwright fallback if no email found ---
    if not best_emails and use_playwright_fallback and PLAYWRIGHT_ENABLED:
        logger.info(f"  No email via requests, trying Playwright for {domain}...")

        # Try homepage + first contact page with Playwright
        playwright_urls = [base_url]
        if CONTACT_PAGE_PATHS:
            playwright_urls.append(urljoin(base_url, CONTACT_PAGE_PATHS[0]))

        for pw_url in playwright_urls:
            pw_html = _fetch_page_playwright(pw_url)
            if pw_html:
                pw_emails = extract_emails(pw_html)
                if pw_emails:
                    best_emails = pw_emails
                    logger.info(f"  Playwright found email on {pw_url}")
                    break

                # Also check for WhatsApp in Playwright-rendered HTML
                if not best_whatsapp.found:
                    pw_wa = detect_whatsapp(pw_html)
                    if pw_wa.found:
                        best_whatsapp = pw_wa

    # --- Build result ---
    if best_emails:
        top_email = best_emails[0]
        result.email = top_email.email
        result.email_priority = top_email.priority
        result.email_is_free_provider = top_email.is_free_provider

    result.has_whatsapp = best_whatsapp.found
    result.whatsapp_confidence = best_whatsapp.confidence
    result.whatsapp_phone = best_whatsapp.phone
    result.scrape_status = "success"

    logger.info(
        f"  Done: email={result.email}, "
        f"whatsapp={result.has_whatsapp} ({result.whatsapp_confidence}), "
        f"name='{result.store_name}'"
    )

    return result


# ---------------------------------------------------------------------------
# Batch scraping
# ---------------------------------------------------------------------------

def scrape_stores_batch(
    domains: List[str],
    use_playwright_fallback: bool = True,
) -> List[ScrapeResult]:
    """
    Scrape a list of store domains.
    
    Adds random delays between stores (Edge Case 7).
    Returns list of ScrapeResults.
    """
    results = []
    total = len(domains)

    for i, domain in enumerate(domains):
        logger.info(f"[{i+1}/{total}] {domain}")

        result = scrape_store(domain, use_playwright_fallback=use_playwright_fallback)
        results.append(result)

        # Delay between stores (not after last one)
        if i < total - 1:
            delay = random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX)
            logger.debug(f"  Waiting {delay:.1f}s before next store...")
            time.sleep(delay)

    # Summary
    success = sum(1 for r in results if r.scrape_status == "success")
    failed = sum(1 for r in results if r.scrape_status == "failed")
    skipped = sum(1 for r in results if r.scrape_status == "skipped")
    with_email = sum(1 for r in results if r.email)
    with_wa = sum(1 for r in results if r.has_whatsapp)

    logger.info(
        f"Batch complete: {success} success, {failed} failed, {skipped} skipped | "
        f"{with_email} emails, {with_wa} WhatsApp"
    )

    return results


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
    parser = argparse.ArgumentParser(description="Scrape Shopify stores for email + WhatsApp")
    parser.add_argument("domains", nargs="*", help="Domains to scrape")
    parser.add_argument("--no-playwright", action="store_true",
                        help="Disable Playwright fallback")
    parser.add_argument("--from-file", type=str,
                        help="Read domains from file (one per line)")
    args = parser.parse_args()

    domains = list(args.domains)
    if args.from_file:
        with open(args.from_file) as f:
            domains.extend(line.strip() for line in f if line.strip())

    if not domains:
        print("No domains provided. Use: python scraper.py store1.co.za store2.co.za")
        print("Or: python scraper.py --from-file domains.txt")
        exit(1)

    results = scrape_stores_batch(
        domains,
        use_playwright_fallback=not args.no_playwright,
    )

    for r in results:
        status_icon = {"success": "✓", "failed": "✗", "skipped": "⊘"}.get(r.scrape_status, "?")
        wa_icon = "📱" if r.has_whatsapp else "  "
        print(f"  {status_icon} {wa_icon} {r.domain:40s} email={r.email or '—':30s} wa={r.whatsapp_confidence}")
