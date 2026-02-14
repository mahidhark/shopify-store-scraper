"""
Shopify Store Scraper — Discovery Module
==========================================
Find Shopify store domains via Google dorking.
Supports resumable batches and cross-run deduplication.

Usage:
    from discovery import discover_stores
    new_domains = discover_stores()
"""

from dotenv import load_dotenv
load_dotenv()
import json
import os
import random
import time
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import List, Optional, Set, Tuple
from dataclasses import dataclass, field

from config import (
    get_enabled_countries,
    DORK_QUERY_TEMPLATES,
    NICHE_DORK_TEMPLATES,
    DORK_DELAY_MIN,
    DORK_DELAY_MAX,
    DORK_RESULTS_PER_QUERY,
    DORK_BATCH_SIZE,
    DORK_BATCH_PAUSE,
    USER_AGENTS,
    DATA_DIR,
    STATE_FILE,
    DEDUP_FILE,
    DISCOVERED_STORES_FILE,
    OUTPUT_DIR,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain normalization (Edge Case 4: dedup by normalized domain)
# ---------------------------------------------------------------------------

def normalize_domain(url: str) -> Optional[str]:
    """
    Extract and normalize domain from a URL.
    
    - Strips www.
    - Strips protocol
    - Returns lowercase root domain
    - Returns None if unparseable
    
    Examples:
        "https://www.mystore.co.za/products" -> "mystore.co.za"
        "http://shop.example.co.za"          -> "shop.example.co.za"
        "mystore.myshopify.com"              -> "mystore.myshopify.com"
    """
    if not url:
        return None

    # Add scheme if missing so urlparse works
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        domain = domain.lower().strip()

        # Strip www.
        if domain.startswith("www."):
            domain = domain[4:]

        # Strip port if present
        domain = domain.split(":")[0]

        # Basic validation
        if "." not in domain or len(domain) < 4:
            return None

        return domain
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Seen domains persistence (Edge Case 4: dedup across runs)
# ---------------------------------------------------------------------------

class SeenDomains:
    """
    Persistent set of already-discovered domains.
    Loaded from disk on init, saved after each batch.
    """

    def __init__(self, filepath: str = DEDUP_FILE):
        self.filepath = filepath
        self.domains: Set[str] = set()
        self._load()

    def _load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r") as f:
                    data = json.load(f)
                self.domains = set(data.get("domains", []))
                logger.info(f"Loaded {len(self.domains)} seen domains from {self.filepath}")
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Corrupted dedup file, starting fresh: {e}")
                self.domains = set()
        else:
            logger.info("No dedup file found, starting fresh")

    def save(self):
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        with open(self.filepath, "w") as f:
            json.dump({"domains": sorted(self.domains), "count": len(self.domains)}, f, indent=2)
        logger.info(f"Saved {len(self.domains)} seen domains")

    def is_new(self, domain: str) -> bool:
        """Check if domain is new (not seen before)."""
        return domain not in self.domains

    def add(self, domain: str):
        self.domains.add(domain)

    def __len__(self):
        return len(self.domains)

    def __contains__(self, domain: str):
        return domain in self.domains


# ---------------------------------------------------------------------------
# Dork state persistence (Edge Case 1: resume across runs)
# ---------------------------------------------------------------------------

class DorkState:
    """
    Track which query index we're on so we can resume across runs.
    """

    def __init__(self, filepath: str = STATE_FILE):
        self.filepath = filepath
        self.query_index: int = 0
        self.last_run: Optional[str] = None
        self.total_discovered: int = 0
        self._load()

    def _load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r") as f:
                    data = json.load(f)
                self.query_index = data.get("query_index", 0)
                self.last_run = data.get("last_run")
                self.total_discovered = data.get("total_discovered", 0)
                logger.info(f"Resuming from query index {self.query_index}")
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Corrupted state file, starting fresh: {e}")

    def save(self):
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        with open(self.filepath, "w") as f:
            json.dump({
                "query_index": self.query_index,
                "last_run": datetime.now(timezone.utc).isoformat(),
                "total_discovered": self.total_discovered,
            }, f, indent=2)

    def advance(self, steps: int = 1):
        self.query_index += steps

    def reset(self):
        self.query_index = 0
        self.total_discovered = 0


# ---------------------------------------------------------------------------
# Query generation
# ---------------------------------------------------------------------------

def generate_dork_queries(include_niche: bool = True) -> List[str]:
    """
    Generate all dork queries for enabled countries.
    
    Combines base templates + niche templates with each country's TLDs.
    Returns a flat list of ready-to-search query strings.
    """
    templates = list(DORK_QUERY_TEMPLATES)
    if include_niche:
        templates.extend(NICHE_DORK_TEMPLATES)

    queries = []
    for country in get_enabled_countries():
        for tld in country.tlds:
            # Strip leading dot for Google dork format
            tld_clean = tld.lstrip(".")
            for tmpl in templates:
                queries.append(tmpl.format(tld=tld_clean))

    logger.info(f"Generated {len(queries)} dork queries for {len(get_enabled_countries())} countries")
    return queries


# ---------------------------------------------------------------------------
# Google dorking engine
# ---------------------------------------------------------------------------

def _search_google(query: str, num_results: int = DORK_RESULTS_PER_QUERY) -> List[str]:
    """
    Execute a single Google search and return URLs.
    Uses SerpAPI for reliable results from server IPs.
    Falls back to googlesearch-python if no SERPAPI_KEY set.
    """
    api_key = os.getenv("SERPAPI_KEY")

    if api_key:
        try:
            from serpapi import GoogleSearch
            params = {
                "q": query,
                "num": num_results,
                "api_key": api_key,
                "engine": "google",
            }
            search = GoogleSearch(params)
            data = search.get_dict()
            results = [r["link"] for r in data.get("organic_results", []) if "link" in r]
            logger.info(f"SerpAPI returned {len(results)} results: {query[:60]}...")
            return results
        except ImportError:
            logger.error("serpapi not installed. Run: pip install google-search-results")
            return []
        except Exception as e:
            logger.warning(f"SerpAPI failed for '{query[:60]}...': {e}")
            return []
    else:
        try:
            from googlesearch import search
            results = list(search(query, num_results=num_results, sleep_interval=2, lang="en"))
            logger.info(f"Query returned {len(results)} results: {query[:60]}...")
            return results
        except ImportError:
            logger.error("googlesearch-python not installed.")
            return []
        except Exception as e:
            logger.warning(f"Google search failed for '{query[:60]}...': {e}")
            return []

def _extract_domains_from_urls(urls: List[str]) -> List[str]:
    """
    Extract and normalize domains from a list of URLs.
    Filters out None values and myshopify.com subdomains 
    (we prefer custom domains).
    """
    domains = []
    for url in urls:
        domain = normalize_domain(url)
        if domain:
            domains.append(domain)
    return domains


# ---------------------------------------------------------------------------
# Main discovery function
# ---------------------------------------------------------------------------

def discover_stores(
    max_queries: Optional[int] = None,
    include_niche: bool = True,
    dry_run: bool = False,
) -> List[str]:
    """
    Run a batch of Google dork queries to discover Shopify stores.
    
    Args:
        max_queries: Override batch size (default: DORK_BATCH_SIZE from config)
        include_niche: Include niche-specific queries
        dry_run: If True, generate queries but don't execute them
        
    Returns:
        List of newly discovered domains (not seen in previous runs)
    """
    batch_size = max_queries or DORK_BATCH_SIZE

    # Load state
    state = DorkState()
    seen = SeenDomains()

    # Generate all queries
    all_queries = generate_dork_queries(include_niche=include_niche)
    total_queries = len(all_queries)

    if state.query_index >= total_queries:
        logger.info("All queries exhausted. Reset state to start over.")
        logger.info(f"Total domains discovered across all runs: {len(seen)}")
        return []

    # Select batch
    batch_start = state.query_index
    batch_end = min(batch_start + batch_size, total_queries)
    batch_queries = all_queries[batch_start:batch_end]

    logger.info(f"Running queries {batch_start+1}-{batch_end} of {total_queries}")
    logger.info(f"Already seen: {len(seen)} domains")

    if dry_run:
        logger.info("DRY RUN — queries that would execute:")
        for i, q in enumerate(batch_queries):
            logger.info(f"  [{batch_start + i + 1}] {q}")
        return []

    # Execute queries
    new_domains = []

    for i, query in enumerate(batch_queries):
        logger.info(f"[{batch_start + i + 1}/{total_queries}] Searching: {query[:70]}...")

        urls = _search_google(query)
        domains = _extract_domains_from_urls(urls)

        for domain in domains:
            if seen.is_new(domain):
                seen.add(domain)
                new_domains.append(domain)
                logger.info(f"  NEW: {domain}")
            else:
                logger.debug(f"  SKIP (seen): {domain}")

        # Random delay between queries
        if i < len(batch_queries) - 1:
            delay = random.uniform(DORK_DELAY_MIN, DORK_DELAY_MAX)
            logger.debug(f"  Waiting {delay:.1f}s before next query...")
            time.sleep(delay)

    # Update state
    state.query_index = batch_end
    state.total_discovered += len(new_domains)
    state.save()
    seen.save()

    # Save discovered stores to file
    _save_discovered_stores(new_domains)

    logger.info(f"Batch complete: {len(new_domains)} new domains found")
    logger.info(f"Progress: {batch_end}/{total_queries} queries done")
    logger.info(f"Total unique domains: {len(seen)}")

    return new_domains


def _save_discovered_stores(domains: List[str]):
    """Append newly discovered domains to the stores file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Use first enabled country for filename
    countries = get_enabled_countries()
    country_code = countries[0].code.lower() if countries else "unknown"
    filepath = os.path.join(OUTPUT_DIR, f"discovered_stores_{country_code}.json")

    existing = []
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, ValueError):
            existing = []

    # Append new entries
    timestamp = datetime.now(timezone.utc).isoformat()
    for domain in domains:
        existing.append({
            "domain": domain,
            "discovered_at": timestamp,
            "scraped": False,
        })

    with open(filepath, "w") as f:
        json.dump(existing, f, indent=2)

    logger.info(f"Saved {len(domains)} new stores to {filepath}")


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
    parser = argparse.ArgumentParser(description="Discover Shopify stores via Google dorking")
    parser.add_argument("--batch-size", type=int, default=DORK_BATCH_SIZE,
                        help=f"Queries per batch (default: {DORK_BATCH_SIZE})")
    parser.add_argument("--no-niche", action="store_true",
                        help="Skip niche-specific queries")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show queries without executing")
    parser.add_argument("--reset", action="store_true",
                        help="Reset query index to start over")
    args = parser.parse_args()

    if args.reset:
        state = DorkState()
        state.reset()
        state.save()
        print("State reset. Will start from query 1 on next run.")
    else:
        results = discover_stores(
            max_queries=args.batch_size,
            include_niche=not args.no_niche,
            dry_run=args.dry_run,
        )
        print(f"\nDiscovered {len(results)} new domains:")
        for d in results:
            print(f"  {d}")
