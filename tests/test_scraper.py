"""Tests for scraper.py — email extraction, WhatsApp detection, store scraping."""

import pytest
from unittest.mock import patch, MagicMock

from scraper import (
    extract_emails,
    detect_whatsapp,
    _is_junk_email,
    _is_free_provider,
    _get_email_priority,
    _extract_store_name,
    _is_shopify_store,
    _is_password_protected,
    scrape_store,
    scrape_stores_batch,
    WhatsAppResult,
    EmailResult,
    ScrapeResult,
)


# ---------------------------------------------------------------------------
# Email junk/priority/provider tests
# ---------------------------------------------------------------------------

class TestEmailHelpers:
    def test_junk_noreply(self):
        assert _is_junk_email("noreply@store.co.za") is True

    def test_junk_shopify(self):
        assert _is_junk_email("help@shopify.com") is True

    def test_junk_klaviyo(self):
        assert _is_junk_email("track@klaviyo.com") is True

    def test_not_junk_real_email(self):
        assert _is_junk_email("hello@mystore.co.za") is False

    def test_free_provider_gmail(self):
        assert _is_free_provider("owner@gmail.com") is True

    def test_free_provider_yahoo(self):
        assert _is_free_provider("shop@yahoo.com") is True

    def test_not_free_provider_custom(self):
        assert _is_free_provider("hello@mystore.co.za") is False

    def test_priority_owner_is_highest(self):
        assert _get_email_priority("owner@store.co.za") == 1

    def test_priority_hello(self):
        assert _get_email_priority("hello@store.co.za") == 2

    def test_priority_info(self):
        assert _get_email_priority("info@store.co.za") == 4

    def test_priority_support_is_low(self):
        assert _get_email_priority("support@store.co.za") == 6

    def test_priority_unknown_gets_default(self):
        p = _get_email_priority("random@store.co.za")
        assert p == 3  # DEFAULT_EMAIL_PRIORITY


# ---------------------------------------------------------------------------
# Email extraction tests
# ---------------------------------------------------------------------------

class TestExtractEmails:
    def test_finds_email_in_html(self):
        html = '<p>Contact us at hello@mystore.co.za</p>'
        results = extract_emails(html)
        assert len(results) == 1
        assert results[0].email == "hello@mystore.co.za"

    def test_finds_mailto_link(self):
        html = '<a href="mailto:info@shop.co.za">Email us</a>'
        results = extract_emails(html)
        assert len(results) == 1
        assert results[0].email == "info@shop.co.za"

    def test_finds_multiple_emails(self):
        html = '''
        <p>Email: hello@store.co.za</p>
        <p>Support: support@store.co.za</p>
        '''
        results = extract_emails(html)
        assert len(results) == 2

    def test_deduplicates_emails(self):
        html = '''
        <p>hello@store.co.za</p>
        <p>HELLO@STORE.CO.ZA</p>
        <a href="mailto:hello@store.co.za">email</a>
        '''
        results = extract_emails(html)
        assert len(results) == 1

    def test_filters_junk_emails(self):
        html = '''
        <p>noreply@store.co.za</p>
        <p>hello@store.co.za</p>
        <p>support@shopify.com</p>
        '''
        results = extract_emails(html)
        assert len(results) == 1
        assert results[0].email == "hello@store.co.za"

    def test_sorts_by_priority(self):
        html = '''
        <p>support@store.co.za</p>
        <p>owner@store.co.za</p>
        <p>info@store.co.za</p>
        '''
        results = extract_emails(html)
        assert results[0].email == "owner@store.co.za"
        assert results[-1].email == "support@store.co.za"

    def test_flags_free_providers(self):
        html = '<p>myshop@gmail.com</p>'
        results = extract_emails(html)
        assert len(results) == 1
        assert results[0].is_free_provider is True

    def test_prefers_custom_over_free(self):
        html = '''
        <p>shop@gmail.com</p>
        <p>hello@mystore.co.za</p>
        '''
        results = extract_emails(html)
        # hello@ (priority 2, not free) should rank above shop@gmail (priority 3, free)
        assert results[0].email == "hello@mystore.co.za"

    def test_no_emails_returns_empty(self):
        html = '<p>No contact info here</p>'
        results = extract_emails(html)
        assert results == []

    def test_handles_empty_html(self):
        assert extract_emails("") == []


# ---------------------------------------------------------------------------
# WhatsApp detection tests
# ---------------------------------------------------------------------------

class TestDetectWhatsApp:
    def test_detects_wa_me_link(self):
        html = '<a href="https://wa.me/27821234567">Chat</a>'
        result = detect_whatsapp(html)
        assert result.found is True
        assert result.confidence == "definitive"
        assert result.phone == "27821234567"

    def test_detects_api_whatsapp(self):
        html = '<a href="https://api.whatsapp.com/send?phone=27821234567">Chat</a>'
        result = detect_whatsapp(html)
        assert result.found is True
        assert result.confidence == "definitive"

    def test_detects_widget_pattern(self):
        html = '<div class="wa-chat-box">WhatsApp Chat</div>'
        result = detect_whatsapp(html)
        assert result.found is True
        assert result.confidence == "widget"

    def test_detects_elfsight_widget(self):
        html = '<script src="https://elfsight.com/whatsapp-widget"></script>'
        result = detect_whatsapp(html)
        assert result.found is True
        assert result.confidence == "widget"

    def test_weak_signal_maybe(self):
        html = '<p>Contact us via WhatsApp for inquiries</p>'
        result = detect_whatsapp(html)
        assert result.found is True
        assert result.confidence == "maybe"

    def test_no_whatsapp(self):
        html = '<p>Contact us via email only</p>'
        result = detect_whatsapp(html)
        assert result.found is False
        assert result.confidence == "none"

    def test_extracts_phone_from_wa_me(self):
        html = 'Visit https://wa.me/+5511999887766 to chat'
        result = detect_whatsapp(html)
        assert result.phone == "+5511999887766"

    def test_definitive_beats_widget(self):
        """If both definitive and widget patterns present, should return definitive."""
        html = '''
        <a href="https://wa.me/27821234567">Chat</a>
        <div class="wa-chat-box">Widget</div>
        '''
        result = detect_whatsapp(html)
        assert result.confidence == "definitive"

    def test_handles_empty_html(self):
        result = detect_whatsapp("")
        assert result.found is False


# ---------------------------------------------------------------------------
# Store name extraction tests
# ---------------------------------------------------------------------------

class TestExtractStoreName:
    def test_extracts_from_og_site_name(self):
        html = '<meta property="og:site_name" content="Cool Store ZA">'
        assert _extract_store_name(html) == "Cool Store ZA"

    def test_extracts_from_title(self):
        html = '<html><head><title>My Awesome Shop</title></head></html>'
        assert _extract_store_name(html) == "My Awesome Shop"

    def test_strips_shopify_suffix(self):
        html = '<html><head><title>My Shop – Powered by Shopify</title></head></html>'
        assert _extract_store_name(html) == "My Shop"

    def test_og_takes_priority_over_title(self):
        html = '''
        <meta property="og:site_name" content="OG Name">
        <title>Title Name</title>
        '''
        assert _extract_store_name(html) == "OG Name"

    def test_returns_empty_when_no_name(self):
        html = '<html><head></head><body>No name</body></html>'
        assert _extract_store_name(html) == ""


# ---------------------------------------------------------------------------
# Shopify detection tests
# ---------------------------------------------------------------------------

class TestIsShopifyStore:
    def test_detects_cdn_shopify(self):
        html = '<link rel="stylesheet" href="https://cdn.shopify.com/s/files/theme.css">'
        assert _is_shopify_store(html) is True

    def test_detects_shopify_section(self):
        html = '<div class="shopify-section">Content</div>'
        assert _is_shopify_store(html) is True

    def test_not_shopify(self):
        html = '<html><body>Just a regular website</body></html>'
        assert _is_shopify_store(html) is False


class TestIsPasswordProtected:
    def test_detects_password_page(self):
        html = '<div id="password-page">Opening soon</div>'
        assert _is_password_protected(html) is True

    def test_detects_storefront_password(self):
        html = '<form class="storefront-password-form">Enter password</form>'
        assert _is_password_protected(html) is True

    def test_not_password_protected(self):
        html = '<html><body>Normal store</body></html>'
        assert _is_password_protected(html) is False


# ---------------------------------------------------------------------------
# Full scrape_store tests (mocked HTTP)
# ---------------------------------------------------------------------------

class TestScrapeStore:
    SHOPIFY_HTML = '''
    <html>
    <head>
        <title>Test Store – Powered by Shopify</title>
        <meta property="og:site_name" content="Test Store">
        <link href="https://cdn.shopify.com/s/files/theme.css">
    </head>
    <body>
        <footer>
            <p>Contact: hello@teststore.co.za</p>
            <a href="https://wa.me/27821234567">WhatsApp Us</a>
        </footer>
    </body>
    </html>
    '''

    PASSWORD_HTML = '''
    <html><head><link href="https://cdn.shopify.com/s/files/theme.css"></head>
    <body><div id="password-page">Opening soon</div></body></html>
    '''

    NON_SHOPIFY_HTML = '''
    <html><head><title>WordPress Blog</title></head>
    <body><p>Just a blog</p></body></html>
    '''

    NO_EMAIL_HTML = '''
    <html><head><link href="https://cdn.shopify.com/s/files/theme.css">
    <meta property="og:site_name" content="No Email Store"></head>
    <body><p>No contact info here</p></body></html>
    '''

    @patch("scraper._fetch_page_requests")
    def test_successful_scrape(self, mock_fetch):
        """Full successful scrape with email + WhatsApp."""
        mock_fetch.return_value = self.SHOPIFY_HTML

        result = scrape_store("teststore.co.za", use_playwright_fallback=False)

        assert result.scrape_status == "success"
        assert result.store_name == "Test Store"
        assert result.email == "hello@teststore.co.za"
        assert result.has_whatsapp is True
        assert result.whatsapp_confidence == "definitive"
        assert result.whatsapp_phone == "27821234567"

    @patch("scraper._fetch_page_requests")
    def test_skips_non_shopify(self, mock_fetch):
        """Should skip non-Shopify stores."""
        mock_fetch.return_value = self.NON_SHOPIFY_HTML

        result = scrape_store("blog.co.za", use_playwright_fallback=False)

        assert result.scrape_status == "skipped"
        assert result.error == "not_shopify"

    @patch("scraper._fetch_page_requests")
    def test_skips_password_protected(self, mock_fetch):
        """Should skip password-protected stores (Edge Case 5)."""
        mock_fetch.return_value = self.PASSWORD_HTML

        result = scrape_store("locked.co.za", use_playwright_fallback=False)

        assert result.scrape_status == "skipped"
        assert result.error == "password_protected"

    @patch("scraper._fetch_page_requests")
    def test_handles_unreachable_store(self, mock_fetch):
        """Should mark failed when homepage unreachable."""
        mock_fetch.return_value = None

        result = scrape_store("dead.co.za", use_playwright_fallback=False)

        assert result.scrape_status == "failed"
        assert result.error == "homepage_unreachable"

    @patch("scraper._fetch_page_playwright")
    @patch("scraper._fetch_page_requests")
    def test_playwright_fallback_finds_email(self, mock_requests, mock_playwright):
        """Playwright should find email when requests misses it."""
        mock_requests.return_value = self.NO_EMAIL_HTML
        mock_playwright.return_value = '''
        <html><head><link href="https://cdn.shopify.com/s/files/theme.css"></head>
        <body><p>Email: found@playwright.co.za</p></body></html>
        '''

        result = scrape_store("nocontact.co.za", use_playwright_fallback=True)

        assert result.scrape_status == "success"
        assert result.email == "found@playwright.co.za"

    @patch("scraper.time.sleep")
    @patch("scraper._fetch_page_requests")
    def test_retries_on_homepage_failure(self, mock_fetch, mock_sleep):
        """Should retry up to SCRAPE_MAX_RETRIES times."""
        # Fail twice, succeed third time
        mock_fetch.side_effect = [None, None, self.SHOPIFY_HTML,
                                  # Contact pages return None
                                  None, None, None, None, None, None, None, None, None, None, None, None]

        result = scrape_store("flaky.co.za", use_playwright_fallback=False)

        assert result.scrape_status == "success"
        # Should have been called 3 times for homepage + contact pages
        assert mock_fetch.call_count >= 3


# ---------------------------------------------------------------------------
# Batch scraping tests
# ---------------------------------------------------------------------------

class TestScrapeStoresBatch:
    @patch("scraper.time.sleep")
    @patch("scraper.scrape_store")
    def test_scrapes_all_domains(self, mock_scrape, mock_sleep):
        mock_scrape.return_value = ScrapeResult(
            domain="test.co.za", scrape_status="success"
        )

        results = scrape_stores_batch(["a.co.za", "b.co.za", "c.co.za"])

        assert len(results) == 3
        assert mock_scrape.call_count == 3

    @patch("scraper.time.sleep")
    @patch("scraper.scrape_store")
    def test_adds_delay_between_stores(self, mock_scrape, mock_sleep):
        mock_scrape.return_value = ScrapeResult(
            domain="test.co.za", scrape_status="success"
        )

        scrape_stores_batch(["a.co.za", "b.co.za", "c.co.za"])

        # Should sleep between stores (2 sleeps for 3 stores)
        assert mock_sleep.call_count == 2

    @patch("scraper.time.sleep")
    @patch("scraper.scrape_store")
    def test_handles_empty_list(self, mock_scrape, mock_sleep):
        results = scrape_stores_batch([])
        assert results == []
        mock_scrape.assert_not_called()


# ---------------------------------------------------------------------------
# Image filename / false positive email tests (bug fix)
# ---------------------------------------------------------------------------

class TestJunkEmailImageFilter:
    def test_filters_png_filenames(self):
        assert _is_junk_email("logo_100x@2x.png") is True

    def test_filters_jpg_filenames(self):
        assert _is_junk_email("product_580x@2x.jpg") is True

    def test_filters_webp_filenames(self):
        assert _is_junk_email("banner_1400px_580x@2x.webp") is True

    def test_filters_css_filenames(self):
        assert _is_junk_email("vendors@layout.theme.css") is True

    def test_filters_js_filenames(self):
        assert _is_junk_email("ecom-swiper@11.js") is True

    def test_filters_shopify_image_pattern(self):
        assert _is_junk_email("la-rocheposay-antishine-1400px_580x@2x.webp") is True

    def test_filters_xxx_placeholder(self):
        assert _is_junk_email("xxx@xxx.xxx") is True

    def test_does_not_filter_real_emails(self):
        assert _is_junk_email("hello@store.co.za") is False
        assert _is_junk_email("admin@myshop.com") is False
        assert _is_junk_email("owner@gmail.com") is False

    def test_extract_emails_skips_image_filenames(self):
        html = '''
        <p>hello@store.co.za</p>
        <img src="logo_100x@2x.png">
        <img src="product_580x@2x.webp">
        '''
        results = extract_emails(html)
        assert len(results) == 1
        assert results[0].email == "hello@store.co.za"

    def test_extract_emails_skips_xxx_placeholder(self):
        html = '<p>xxx@xxx.xxx</p><p>real@store.co.za</p>'
        results = extract_emails(html)
        assert len(results) == 1
        assert results[0].email == "real@store.co.za"
