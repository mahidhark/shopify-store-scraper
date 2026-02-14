"""Tests for config.py — validate all configuration is sane."""

import re
import pytest
from config import (
    COUNTRIES,
    CountryConfig,
    get_enabled_countries,
    DORK_QUERY_TEMPLATES,
    NICHE_DORK_TEMPLATES,
    DORK_DELAY_MIN,
    DORK_DELAY_MAX,
    DORK_BATCH_SIZE,
    USER_AGENTS,
    CONTACT_PAGE_PATHS,
    SCRAPE_DELAY_MIN,
    SCRAPE_DELAY_MAX,
    SCRAPE_MAX_RETRIES,
    SCRAPE_TIMEOUT,
    EMAIL_REGEX,
    JUNK_EMAIL_PATTERNS,
    FREE_EMAIL_PROVIDERS,
    EMAIL_PRIORITY,
    DEFAULT_EMAIL_PRIORITY,
    WHATSAPP_DEFINITIVE_PATTERNS,
    WHATSAPP_WIDGET_PATTERNS,
    WHATSAPP_WEAK_PATTERNS,
    WHATSAPP_PHONE_REGEX,
    CSV_COLUMNS,
)


# ---------------------------------------------------------------------------
# Country configuration tests
# ---------------------------------------------------------------------------

class TestCountries:
    def test_south_africa_is_enabled(self):
        """Phase 1: Only South Africa should be enabled."""
        enabled = get_enabled_countries()
        assert len(enabled) == 1
        assert enabled[0].code == "ZA"

    def test_all_countries_have_required_fields(self):
        for code, country in COUNTRIES.items():
            assert isinstance(country, CountryConfig)
            assert country.code == code
            assert len(country.name) > 0
            assert len(country.tlds) > 0
            assert country.whatsapp_penetration in ("high", "very_high")

    def test_tlds_start_with_dot(self):
        for code, country in COUNTRIES.items():
            for tld in country.tlds:
                assert tld.startswith("."), f"{code} TLD '{tld}' missing leading dot"

    def test_six_countries_defined(self):
        """We defined 6 target markets."""
        assert len(COUNTRIES) == 6

    def test_disabled_countries_not_in_enabled(self):
        enabled_codes = [c.code for c in get_enabled_countries()]
        for code, country in COUNTRIES.items():
            if not country.enabled:
                assert code not in enabled_codes


# ---------------------------------------------------------------------------
# Google dorking tests
# ---------------------------------------------------------------------------

class TestDorkConfig:
    def test_all_templates_have_tld_placeholder(self):
        for tmpl in DORK_QUERY_TEMPLATES + NICHE_DORK_TEMPLATES:
            assert "{tld}" in tmpl, f"Template missing {{tld}}: {tmpl}"

    def test_templates_produce_valid_queries(self):
        """Substituting a real TLD should produce a usable query."""
        for tmpl in DORK_QUERY_TEMPLATES:
            query = tmpl.format(tld=".co.za")
            assert ".co.za" in query
            assert "{tld}" not in query

    def test_delay_range_is_sane(self):
        assert DORK_DELAY_MIN > 0
        assert DORK_DELAY_MAX >= DORK_DELAY_MIN

    def test_batch_size_positive(self):
        assert DORK_BATCH_SIZE > 0

    def test_user_agents_not_empty(self):
        assert len(USER_AGENTS) >= 3


# ---------------------------------------------------------------------------
# Store scraping tests
# ---------------------------------------------------------------------------

class TestScrapeConfig:
    def test_contact_paths_start_with_slash(self):
        for path in CONTACT_PAGE_PATHS:
            assert path.startswith("/"), f"Path missing leading slash: {path}"

    def test_afrikaans_paths_included(self):
        """Edge Case 6: Afrikaans contact page variants."""
        paths_str = " ".join(CONTACT_PAGE_PATHS)
        assert "kontak" in paths_str
        assert "oor-ons" in paths_str

    def test_scrape_delay_range(self):
        assert 0 < SCRAPE_DELAY_MIN <= SCRAPE_DELAY_MAX

    def test_max_retries_is_three(self):
        """Edge Case 7: Max 3 retries then skip."""
        assert SCRAPE_MAX_RETRIES == 3

    def test_timeout_reasonable(self):
        assert 5 <= SCRAPE_TIMEOUT <= 30


# ---------------------------------------------------------------------------
# Email extraction tests
# ---------------------------------------------------------------------------

class TestEmailConfig:
    def test_email_regex_matches_valid_emails(self):
        valid = [
            "owner@store.co.za",
            "hello@my-shop.com",
            "info@example.co.za",
            "a.b+tag@domain.org",
        ]
        for email in valid:
            assert re.search(EMAIL_REGEX, email), f"Regex missed: {email}"

    def test_email_regex_rejects_garbage(self):
        invalid = [
            "not-an-email",
            "@nodomain",
            "spaces in@email.com",
        ]
        for text in invalid:
            match = re.fullmatch(EMAIL_REGEX, text)
            assert match is None, f"Regex false positive: {text}"

    def test_junk_patterns_catch_noreply(self):
        assert any("noreply" in p for p in JUNK_EMAIL_PATTERNS)
        assert any("shopify.com" in p for p in JUNK_EMAIL_PATTERNS)

    def test_free_providers_include_major_ones(self):
        assert "gmail.com" in FREE_EMAIL_PROVIDERS
        assert "yahoo.com" in FREE_EMAIL_PROVIDERS
        assert "outlook.com" in FREE_EMAIL_PROVIDERS

    def test_email_priority_ranking(self):
        """owner/founder/ceo should be highest priority (lowest number)."""
        assert EMAIL_PRIORITY["owner"] < EMAIL_PRIORITY["support"]
        assert EMAIL_PRIORITY["founder"] < EMAIL_PRIORITY["info"]
        assert EMAIL_PRIORITY["hello"] < EMAIL_PRIORITY["admin"]

    def test_default_priority_is_middle(self):
        priorities = list(EMAIL_PRIORITY.values())
        assert min(priorities) <= DEFAULT_EMAIL_PRIORITY <= max(priorities)


# ---------------------------------------------------------------------------
# WhatsApp detection tests
# ---------------------------------------------------------------------------

class TestWhatsAppConfig:
    def test_definitive_patterns_include_wa_me(self):
        assert any("wa.me" in p for p in WHATSAPP_DEFINITIVE_PATTERNS)

    def test_definitive_patterns_include_api(self):
        assert any("api.whatsapp.com" in p for p in WHATSAPP_DEFINITIVE_PATTERNS)

    def test_phone_regex_extracts_number(self):
        test_cases = [
            ("https://wa.me/27821234567", "27821234567"),
            ("wa.me/+5511999887766", "+5511999887766"),
        ]
        for text, expected in test_cases:
            match = re.search(WHATSAPP_PHONE_REGEX, text)
            assert match is not None, f"Phone regex missed: {text}"
            assert match.group(1) == expected

    def test_three_tier_detection(self):
        """Edge Case 3: We have definitive, widget, and weak tiers."""
        assert len(WHATSAPP_DEFINITIVE_PATTERNS) > 0
        assert len(WHATSAPP_WIDGET_PATTERNS) > 0
        assert len(WHATSAPP_WEAK_PATTERNS) > 0


# ---------------------------------------------------------------------------
# Output tests
# ---------------------------------------------------------------------------

class TestOutputConfig:
    def test_csv_has_required_columns(self):
        required = ["domain", "email", "has_whatsapp", "scrape_status"]
        for col in required:
            assert col in CSV_COLUMNS, f"Missing CSV column: {col}"

    def test_csv_has_whatsapp_confidence(self):
        """Edge Case 3: Confidence level column exists."""
        assert "whatsapp_confidence" in CSV_COLUMNS

    def test_csv_has_email_verified(self):
        """Reacher integration column exists (filled later)."""
        assert "email_verified" in CSV_COLUMNS


class TestContactPagePathsPolicies:
    def test_policy_pages_included(self):
        """Bug fix: policy pages often contain emails."""
        paths_str = " ".join(CONTACT_PAGE_PATHS)
        assert "privacy-policy" in paths_str
        assert "terms-of-service" in paths_str

    def test_shipping_policy_included(self):
        assert "/policies/shipping-policy" in CONTACT_PAGE_PATHS

    def test_refund_policy_included(self):
        assert "/policies/refund-policy" in CONTACT_PAGE_PATHS
