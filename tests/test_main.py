"""Tests for main.py — pipeline orchestration."""

import json
import os
import pytest
from unittest.mock import patch, MagicMock

from main import (
    step_discover,
    step_scrape,
    step_verify,
    step_export,
    run_pipeline,
    _load_unscraped_domains,
    _mark_domains_scraped,
)
from scraper import ScrapeResult
from verifier import VerifyResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_scrape_result(domain, email=None, has_wa=False, status="success"):
    return ScrapeResult(
        domain=domain,
        store_name=domain.split(".")[0].title(),
        email=email,
        email_priority=3,
        has_whatsapp=has_wa,
        whatsapp_confidence="definitive" if has_wa else "none",
        scrape_status=status,
        scraped_at="2026-02-14T10:00:00+00:00",
    )


def _make_verify_result(email, status="safe"):
    return VerifyResult(email=email, status=status)


# ---------------------------------------------------------------------------
# Load/mark unscraped domains tests
# ---------------------------------------------------------------------------

class TestLoadUnscrapedDomains:
    def test_loads_unscraped(self, tmp_path):
        stores = [
            {"domain": "store1.co.za", "scraped": False, "discovered_at": "2026-02-14"},
            {"domain": "store2.co.za", "scraped": True, "discovered_at": "2026-02-14"},
            {"domain": "store3.co.za", "scraped": False, "discovered_at": "2026-02-14"},
        ]
        filepath = tmp_path / "discovered_stores_za.json"
        filepath.write_text(json.dumps(stores))

        with patch("main.OUTPUT_DIR", str(tmp_path)):
            domains = _load_unscraped_domains("ZA")

        assert len(domains) == 2
        assert "store1.co.za" in domains
        assert "store3.co.za" in domains
        assert "store2.co.za" not in domains

    def test_handles_missing_file(self, tmp_path):
        with patch("main.OUTPUT_DIR", str(tmp_path)):
            domains = _load_unscraped_domains("ZA")
        assert domains == []

    def test_handles_corrupted_file(self, tmp_path):
        filepath = tmp_path / "discovered_stores_za.json"
        filepath.write_text("BROKEN JSON{{{")

        with patch("main.OUTPUT_DIR", str(tmp_path)):
            domains = _load_unscraped_domains("ZA")
        assert domains == []

    def test_all_scraped_returns_empty(self, tmp_path):
        stores = [
            {"domain": "store1.co.za", "scraped": True},
            {"domain": "store2.co.za", "scraped": True},
        ]
        filepath = tmp_path / "discovered_stores_za.json"
        filepath.write_text(json.dumps(stores))

        with patch("main.OUTPUT_DIR", str(tmp_path)):
            domains = _load_unscraped_domains("ZA")
        assert domains == []


class TestMarkDomainsScraped:
    def test_marks_domains(self, tmp_path):
        stores = [
            {"domain": "store1.co.za", "scraped": False},
            {"domain": "store2.co.za", "scraped": False},
            {"domain": "store3.co.za", "scraped": False},
        ]
        filepath = tmp_path / "discovered_stores_za.json"
        filepath.write_text(json.dumps(stores))

        with patch("main.OUTPUT_DIR", str(tmp_path)):
            _mark_domains_scraped(["store1.co.za", "store3.co.za"], "ZA")

        updated = json.loads(filepath.read_text())
        assert updated[0]["scraped"] is True
        assert updated[1]["scraped"] is False
        assert updated[2]["scraped"] is True

    def test_handles_missing_file(self, tmp_path):
        """Should not crash if file doesn't exist."""
        with patch("main.OUTPUT_DIR", str(tmp_path)):
            _mark_domains_scraped(["store1.co.za"], "ZA")  # No error


# ---------------------------------------------------------------------------
# Step tests
# ---------------------------------------------------------------------------

class TestStepDiscover:
    @patch("main.discover_stores")
    def test_calls_discover(self, mock_discover):
        mock_discover.return_value = ["store1.co.za", "store2.co.za"]

        result = step_discover(batch_size=5)

        assert len(result) == 2
        mock_discover.assert_called_once_with(
            max_queries=5, include_niche=True, dry_run=False
        )

    @patch("main.discover_stores")
    def test_dry_run(self, mock_discover):
        mock_discover.return_value = []

        result = step_discover(dry_run=True)

        assert result == []
        mock_discover.assert_called_once_with(
            max_queries=10, include_niche=True, dry_run=True
        )


class TestStepScrape:
    @patch("main._mark_domains_scraped")
    @patch("main.scrape_stores_batch")
    def test_scrapes_provided_domains(self, mock_batch, mock_mark):
        mock_batch.return_value = [
            _make_scrape_result("store1.co.za", email="hi@store1.co.za")
        ]

        results = step_scrape(domains=["store1.co.za"])

        assert len(results) == 1
        mock_batch.assert_called_once()

    @patch("main._load_unscraped_domains")
    @patch("main._mark_domains_scraped")
    @patch("main.scrape_stores_batch")
    def test_loads_unscraped_if_no_domains(self, mock_batch, mock_mark, mock_load):
        mock_load.return_value = ["auto1.co.za", "auto2.co.za"]
        mock_batch.return_value = [
            _make_scrape_result("auto1.co.za"),
            _make_scrape_result("auto2.co.za"),
        ]

        results = step_scrape(domains=None)

        mock_load.assert_called_once()
        assert len(results) == 2

    @patch("main._mark_domains_scraped")
    @patch("main.scrape_stores_batch")
    def test_empty_domains_returns_empty(self, mock_batch, mock_mark):
        results = step_scrape(domains=[])
        assert results == []
        mock_batch.assert_not_called()


class TestStepVerify:
    @patch("main.verify_emails_batch")
    def test_extracts_emails_from_scrape_results(self, mock_verify):
        mock_verify.return_value = [
            _make_verify_result("a@a.com", "safe"),
            _make_verify_result("b@b.com", "invalid"),
        ]

        scrapes = [
            _make_scrape_result("a.co.za", email="a@a.com"),
            _make_scrape_result("b.co.za", email="b@b.com"),
            _make_scrape_result("c.co.za", email=None),  # No email
            _make_scrape_result("d.co.za", email="d@d.com", status="failed"),  # Failed
        ]

        results = step_verify(scrape_results=scrapes)

        assert len(results) == 2
        # Should only verify emails from successful scrapes with emails
        mock_verify.assert_called_once_with(["a@a.com", "b@b.com"])

    @patch("main.verify_emails_batch")
    def test_deduplicates_emails(self, mock_verify):
        mock_verify.return_value = []

        scrapes = [
            _make_scrape_result("a.co.za", email="same@store.co.za"),
            _make_scrape_result("b.co.za", email="same@store.co.za"),
        ]

        step_verify(scrape_results=scrapes)

        # Should only verify once despite two stores having same email
        mock_verify.assert_called_once_with(["same@store.co.za"])

    def test_no_emails_returns_empty(self):
        results = step_verify(scrape_results=[])
        assert results == []


class TestStepExport:
    def test_generates_csv(self, tmp_path):
        scrapes = [
            _make_scrape_result("store.co.za", email="hi@store.co.za", has_wa=True),
        ]
        verifies = [_make_verify_result("hi@store.co.za", "safe")]

        with patch("main.generate_csv") as mock_csv, \
             patch("main.merge_results") as mock_merge, \
             patch("main.print_summary"):
            mock_csv.return_value = str(tmp_path / "test.csv")
            mock_merge.return_value = [{"scrape_status": "success"}]

            filepath = step_export(scrapes, verifies, country="ZA")

        mock_csv.assert_called_once_with(scrapes, verifies, country="ZA")


# ---------------------------------------------------------------------------
# Full pipeline tests
# ---------------------------------------------------------------------------

class TestRunPipeline:
    @patch("main.step_export")
    @patch("main.step_verify")
    @patch("main.step_scrape")
    @patch("main.step_discover")
    def test_full_pipeline(self, mock_discover, mock_scrape, mock_verify, mock_export):
        mock_discover.return_value = ["store1.co.za"]
        mock_scrape.return_value = [
            _make_scrape_result("store1.co.za", email="hi@store1.co.za")
        ]
        mock_verify.return_value = [
            _make_verify_result("hi@store1.co.za", "safe")
        ]
        mock_export.return_value = "/output/leads.csv"

        result = run_pipeline(batch_size=1)

        assert result == "/output/leads.csv"
        mock_discover.assert_called_once()
        mock_scrape.assert_called_once()
        mock_verify.assert_called_once()
        mock_export.assert_called_once()

    @patch("main.step_export")
    @patch("main.step_verify")
    @patch("main.step_scrape")
    def test_pipeline_with_provided_domains(self, mock_scrape, mock_verify, mock_export):
        """Should skip discovery when domains provided."""
        mock_scrape.return_value = [
            _make_scrape_result("custom.co.za", email="hi@custom.co.za")
        ]
        mock_verify.return_value = []
        mock_export.return_value = "/output/leads.csv"

        result = run_pipeline(domains=["custom.co.za"])

        assert result == "/output/leads.csv"
        mock_scrape.assert_called_once()

    @patch("main.step_export")
    @patch("main.step_scrape")
    @patch("main.step_discover")
    def test_pipeline_skip_verify(self, mock_discover, mock_scrape, mock_export):
        mock_discover.return_value = ["store.co.za"]
        mock_scrape.return_value = [_make_scrape_result("store.co.za")]
        mock_export.return_value = "/output/leads.csv"

        result = run_pipeline(skip_verify=True)

        mock_export.assert_called_once()
        # verify_results should be None
        _, call_args = mock_export.call_args
        assert call_args.get("verify_results") is None or mock_export.call_args[0][1] is None

    @patch("main.step_discover")
    def test_dry_run_stops_early(self, mock_discover):
        mock_discover.return_value = []

        result = run_pipeline(dry_run=True)

        assert result is None

    @patch("main._load_unscraped_domains")
    @patch("main.step_discover")
    def test_no_domains_returns_none(self, mock_discover, mock_load):
        mock_discover.return_value = []
        mock_load.return_value = []

        result = run_pipeline()

        assert result is None
