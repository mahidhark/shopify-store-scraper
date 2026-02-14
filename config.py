"""
Shopify Store Scraper — Configuration
======================================
Central config for all modules. Edit this file to add countries,
tweak rate limits, or update detection patterns.
"""

from dataclasses import dataclass
from typing import List, Dict


# ---------------------------------------------------------------------------
# Target Countries (Phase 1: South Africa only)
# ---------------------------------------------------------------------------

@dataclass
class CountryConfig:
    """Configuration for a target country."""
    code: str                  # ISO 3166-1 alpha-2
    name: str
    tlds: List[str]            # Country TLDs to search
    whatsapp_penetration: str  # "high" | "very_high"
    enabled: bool = True


COUNTRIES: Dict[str, CountryConfig] = {
    "ZA": CountryConfig(
        code="ZA",
        name="South Africa",
        tlds=[".co.za", ".za"],
        whatsapp_penetration="high",
        enabled=True,   # Phase 1
    ),
    "BR": CountryConfig(
        code="BR",
        name="Brazil",
        tlds=[".com.br", ".br"],
        whatsapp_penetration="very_high",
        enabled=False,  # Phase 3
    ),
    "DE": CountryConfig(
        code="DE",
        name="Germany",
        tlds=[".de"],
        whatsapp_penetration="high",
        enabled=False,
    ),
    "NG": CountryConfig(
        code="NG",
        name="Nigeria",
        tlds=[".ng", ".com.ng"],
        whatsapp_penetration="high",
        enabled=False,
    ),
    "ID": CountryConfig(
        code="ID",
        name="Indonesia",
        tlds=[".id", ".co.id"],
        whatsapp_penetration="very_high",
        enabled=False,
    ),
    "MX": CountryConfig(
        code="MX",
        name="Mexico",
        tlds=[".mx", ".com.mx"],
        whatsapp_penetration="very_high",
        enabled=False,
    ),
}


def get_enabled_countries() -> List[CountryConfig]:
    """Return only countries that are enabled for scraping."""
    return [c for c in COUNTRIES.values() if c.enabled]


# ---------------------------------------------------------------------------
# Google Dorking Configuration
# ---------------------------------------------------------------------------

# Query templates — {tld} gets replaced per country
DORK_QUERY_TEMPLATES: List[str] = [
    'site:{tld} "powered by shopify"',
    'site:{tld} "cdn.shopify.com"',
    'site:{tld} inurl:myshopify',
    'site:{tld} "shopify" "add to cart"',
    'site:{tld} "checkout" "shopify"',
]

# Niche-specific dork templates for more targeted discovery
NICHE_DORK_TEMPLATES: List[str] = [
    'site:{tld} "powered by shopify" "fashion"',
    'site:{tld} "powered by shopify" "beauty"',
    'site:{tld} "powered by shopify" "electronics"',
    'site:{tld} "powered by shopify" "food"',
    'site:{tld} "powered by shopify" "home"',
    'site:{tld} "powered by shopify" "jewellery" OR "jewelry"',
    'site:{tld} "powered by shopify" "clothing"',
    'site:{tld} "powered by shopify" "health"',
    'site:{tld} "powered by shopify" "pet"',
    'site:{tld} "powered by shopify" "sport"',
]

# Rate limiting — smaller batches, build up over time (Edge Case 1)
DORK_DELAY_MIN: float = 15.0      # Seconds between queries
DORK_DELAY_MAX: float = 30.0
DORK_RESULTS_PER_QUERY: int = 50   # Max results per query
DORK_BATCH_SIZE: int = 10          # Queries per batch before long pause
DORK_BATCH_PAUSE: float = 120.0   # Pause between batches (seconds)

# User agents for rotation
USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


# ---------------------------------------------------------------------------
# Store Scraping Configuration
# ---------------------------------------------------------------------------

# Contact page paths to check (including Afrikaans — Edge Case 6)
CONTACT_PAGE_PATHS: List[str] = [
    "/pages/contact",
    "/pages/contact-us",
    "/pages/about",
    "/pages/about-us",
    "/pages/kontak",        # Afrikaans
    "/pages/oor-ons",       # Afrikaans "about us"
    "/pages/get-in-touch",
    "/pages/support",
]

# Rate limits (Edge Case 7: 1-2s delay, max 3 retries, then skip)
SCRAPE_DELAY_MIN: float = 1.0
SCRAPE_DELAY_MAX: float = 2.0
SCRAPE_MAX_RETRIES: int = 3
SCRAPE_TIMEOUT: int = 15          # Request timeout seconds

# Playwright settings (hybrid approach — Edge Case 2)
PLAYWRIGHT_TIMEOUT: int = 20000   # ms — wait for JS render
PLAYWRIGHT_ENABLED: bool = True


# ---------------------------------------------------------------------------
# Email Extraction Configuration
# ---------------------------------------------------------------------------

EMAIL_REGEX: str = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'

# Junk emails to filter out
JUNK_EMAIL_PATTERNS: List[str] = [
    "noreply@",
    "no-reply@",
    "support@shopify.com",
    "help@shopify.com",
    "@sentry.io",
    "@tally.so",
    "@bento.me",
    "@example.com",
    "@wixpress.com",
    "@wix.com",
    "@shopify.com",
    "@klaviyo.com",
    "@mailchimp.com",
    "@hubspot.com",
    "@zendesk.com",
]

# Free email providers — keep but flag as lower quality
FREE_EMAIL_PROVIDERS: List[str] = [
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "aol.com",
    "icloud.com",
    "protonmail.com",
]

# Email priority ranking (lower number = higher priority)
EMAIL_PRIORITY: Dict[str, int] = {
    "owner": 1,
    "founder": 1,
    "ceo": 1,
    "hello": 2,
    "hi": 2,
    "contact": 3,
    "info": 4,
    "enquiries": 4,
    "enquiry": 4,
    "sales": 5,
    "support": 6,
    "admin": 7,
}
DEFAULT_EMAIL_PRIORITY: int = 3


# ---------------------------------------------------------------------------
# WhatsApp Detection Configuration
# ---------------------------------------------------------------------------

# Definitive signals -> has_whatsapp = True
WHATSAPP_DEFINITIVE_PATTERNS: List[str] = [
    "wa.me/",
    "api.whatsapp.com/send",
    "web.whatsapp.com/send",
]

# Strong signals -> has_whatsapp = True
WHATSAPP_WIDGET_PATTERNS: List[str] = [
    "wa-chat-box",
    "whatsapp-widget",
    "elfsight.com/whatsapp",
    "whatsapp-chat-widget",
    "wa-automate",
    "wati.io",
]

# Weak signals -> has_whatsapp = "maybe"
WHATSAPP_WEAK_PATTERNS: List[str] = [
    "whatsapp",
    "WhatsApp",
]

# WhatsApp phone number regex (from wa.me links)
WHATSAPP_PHONE_REGEX: str = r'wa\.me/(\+?\d{7,15})'


# ---------------------------------------------------------------------------
# Output Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR: str = "output"
OUTPUT_CSV_FILENAME: str = "leads_{country}_{date}.csv"
DISCOVERED_STORES_FILE: str = "discovered_stores_{country}.json"

CSV_COLUMNS: List[str] = [
    "domain",
    "store_name",
    "country",
    "email",
    "email_priority",
    "email_is_free_provider",
    "has_whatsapp",
    "whatsapp_confidence",   # "definitive" | "widget" | "maybe"
    "whatsapp_phone",
    "email_verified",        # Filled by verifier.py later
    "scrape_status",         # "success" | "failed" | "skipped"
    "discovered_at",
    "scraped_at",
]


# ---------------------------------------------------------------------------
# File Paths
# ---------------------------------------------------------------------------

DATA_DIR: str = "data"
STATE_FILE: str = "data/scraper_state.json"     # Track progress across runs
DEDUP_FILE: str = "data/seen_domains.json"       # Deduplication (Edge Case 4)
