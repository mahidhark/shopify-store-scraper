# Shopify Store Scraper

Discover Shopify stores in target markets, extract contact emails and detect WhatsApp presence. Built for B2B cold outreach to Shopify merchants in WhatsApp-heavy markets.

## Architecture
```
discovery.py → scraper.py → verifier.py → output.py
     ↑              ↑             ↑            ↑
     └──────── main.py (orchestrator) ─────────┘
```

**Pipeline steps:**
1. **Discover** — Find Shopify stores via Google dorking (SerpAPI)
2. **Scrape** — Visit each store, extract emails from homepage + contact + policy pages, detect WhatsApp
3. **Verify** — SMTP verification via Reacher CLI (self-hosted, free)
4. **Export** — Sorted CSV with leads ranked by WhatsApp + verified email

## Quick Start
```bash
# Clone and setup
git clone https://github.com/mahidhark/shopify-store-scraper.git
cd shopify-store-scraper
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure API keys
cp .env.example .env
# Edit .env with your SERPAPI_KEY

# Smoke test on a known store
python main.py --domains fromhere.co.za --skip-verify --no-playwright

# Discover stores and run full pipeline
python main.py --batch-size 5

# Discovery only (dry run)
python main.py discover --dry-run
```

## Files

| File | Purpose |
|------|---------|
| `config.py` | Countries, dork queries, email filters, WhatsApp patterns, rate limits |
| `discovery.py` | Google dorking via SerpAPI, resumable batches, cross-run dedup |
| `scraper.py` | Email extraction (regex + contact/policy pages), WhatsApp detection (3-tier) |
| `verifier.py` | Reacher CLI integration for SMTP email verification |
| `output.py` | Merge results, sort leads, generate CSV |
| `main.py` | Pipeline orchestrator + CLI |

## CLI Usage
```bash
# Full pipeline (discover + scrape + verify + export)
python main.py

# Scrape specific domains
python main.py --domains store1.co.za store2.co.za

# Skip email verification
python main.py --skip-verify

# Disable Playwright fallback
python main.py --no-playwright

# Discovery only
python main.py discover --batch-size 10
python main.py discover --dry-run
python main.py discover --reset
```

## Email Extraction

Checks multiple pages per store:
- Homepage
- Contact pages (/pages/contact, /pages/contact-us, /pages/about, etc.)
- Afrikaans variants (/pages/kontak, /pages/oor-ons)
- Policy pages (/policies/privacy-policy, /policies/terms-of-service, etc.)
- Playwright headless browser fallback for JS-rendered emails

Filters out junk: noreply@, Shopify internals, image filenames (@2x.png), placeholders.

Ranks by priority: owner@ > hello@ > contact@ > info@ > support@ > admin@

## WhatsApp Detection (3-Tier)

| Tier | Confidence | Patterns |
|------|------------|----------|
| Definitive | definitive | wa.me/ links, api.whatsapp.com/send |
| Widget | widget | Elfsight, wa-chat-box, WhatsApp widget classes |
| Weak | maybe | "whatsapp" keyword in page source |

Extracts phone numbers from wa.me links when found.

## Email Verification

Uses Reacher CLI (self-hosted, free).

Results: safe (mailbox exists) | risky (catch-all) | invalid (doesn't exist) | unknown (check failed)

Note: Requires outbound port 25 (SMTP). DigitalOcean blocks this by default — submit a support ticket to unblock.

## Target Countries

| Country | Code | TLDs | WhatsApp Penetration | Status |
|---------|------|------|---------------------|--------|
| South Africa | ZA | .co.za, .za | High | Active |
| Brazil | BR | .com.br, .br | Very High | Planned |
| Germany | DE | .de | High | Planned |
| Nigeria | NG | .com.ng, .ng | High | Planned |
| Mexico | MX | .com.mx, .mx | Very High | Planned |
| Indonesia | ID | .co.id, .id | Very High | Planned |

## Output CSV Columns

domain, store_name, country, email, email_priority, email_is_free_provider, has_whatsapp, whatsapp_confidence, whatsapp_phone, email_verified, scrape_status, discovered_at, scraped_at

## Tests
```bash
python -m pytest tests/ -v       # 192 tests
```

## Tech Stack

| Component | Tool | Cost |
|-----------|------|------|
| Discovery | SerpAPI (free tier: 100 searches/month) | Free |
| Scraping | requests + BeautifulSoup + Playwright | Free |
| Verification | Reacher CLI (self-hosted) | Free |
| Language | Python 3.12 | Free |

## Rate Limiting

- Google dorking: Configurable delays between queries
- Store scraping: 1-2s random delay between stores, max 3 retries
- Email verification: 1s between checks, batch pauses every 20

## License

Private — built for [WhatsScale](https://trywhatsscale.com)
