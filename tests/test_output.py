"""Tests for output.py — merge results, sort leads, generate CSV."""

import csv
import os
import tempfile
import pytest

from output import (
    merge_results,
    sort_leads,
    generate_csv,
    load_csv,
    print_summary,
    _lead_sort_key,
)
from scraper import ScrapeResult
from verifier import VerifyResult


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

def _make_scrape_result(domain, email=None, has_wa=False, wa_conf="none",
                        wa_phone=None, status="success", priority=3):
    return ScrapeResult(
        domain=domain,
        store_name=domain.split(".")[0].title(),
        email=email,
        email_priority=priority,
        email_is_free_provider=False,
        has_whatsapp=has_wa,
        whatsapp_confidence=wa_conf,
        whatsapp_phone=wa_phone,
        scrape_status=status,
        scraped_at="2026-02-14T10:00:00+00:00",
    )


def _make_verify_result(email, status="safe"):
    return VerifyResult(email=email, status=status)


# ---------------------------------------------------------------------------
# Merge tests
# ---------------------------------------------------------------------------

class TestMergeResults:
    def test_merge_scrape_only(self):
        scrapes = [
            _make_scrape_result("store1.co.za", email="hi@store1.co.za"),
            _make_scrape_result("store2.co.za", email="hi@store2.co.za"),
        ]
        rows = merge_results(scrapes)
        assert len(rows) == 2
        assert rows[0]["domain"] == "store1.co.za"
        assert rows[0]["email"] == "hi@store1.co.za"
        assert rows[0]["email_verified"] == ""  # No verification

    def test_merge_with_verification(self):
        scrapes = [
            _make_scrape_result("store1.co.za", email="hi@store1.co.za"),
        ]
        verifies = [
            _make_verify_result("hi@store1.co.za", status="safe"),
        ]
        rows = merge_results(scrapes, verifies)
        assert rows[0]["email_verified"] == "safe"

    def test_merge_unmatched_verification_ignored(self):
        """Verification for email not in scrape results is ignored."""
        scrapes = [
            _make_scrape_result("store1.co.za", email="hi@store1.co.za"),
        ]
        verifies = [
            _make_verify_result("other@other.com", status="safe"),
        ]
        rows = merge_results(scrapes, verifies)
        assert rows[0]["email_verified"] == ""

    def test_merge_no_email_store(self):
        scrapes = [_make_scrape_result("noemail.co.za", email=None)]
        rows = merge_results(scrapes)
        assert rows[0]["email"] == ""
        assert rows[0]["email_verified"] == ""

    def test_merge_case_insensitive_match(self):
        scrapes = [
            _make_scrape_result("store.co.za", email="Hello@Store.co.za"),
        ]
        verifies = [
            _make_verify_result("hello@store.co.za", status="safe"),
        ]
        rows = merge_results(scrapes, verifies)
        assert rows[0]["email_verified"] == "safe"

    def test_merge_preserves_whatsapp_data(self):
        scrapes = [
            _make_scrape_result("wa.co.za", email="hi@wa.co.za",
                                has_wa=True, wa_conf="definitive", wa_phone="27821234567"),
        ]
        rows = merge_results(scrapes)
        assert rows[0]["has_whatsapp"] is True
        assert rows[0]["whatsapp_confidence"] == "definitive"
        assert rows[0]["whatsapp_phone"] == "27821234567"


# ---------------------------------------------------------------------------
# Sort tests
# ---------------------------------------------------------------------------

class TestSortLeads:
    def test_whatsapp_verified_first(self):
        rows = [
            {"domain": "c.co.za", "email": "c@c.com", "has_whatsapp": False,
             "email_verified": "safe", "email_priority": 3},
            {"domain": "a.co.za", "email": "a@a.com", "has_whatsapp": True,
             "email_verified": "safe", "email_priority": 3},
            {"domain": "b.co.za", "email": "b@b.com", "has_whatsapp": True,
             "email_verified": "", "email_priority": 3},
        ]
        sorted_rows = sort_leads(rows)
        assert sorted_rows[0]["domain"] == "a.co.za"  # WA + verified
        assert sorted_rows[1]["domain"] == "b.co.za"  # WA + email
        assert sorted_rows[2]["domain"] == "c.co.za"  # verified only

    def test_no_email_last(self):
        rows = [
            {"domain": "noemail.co.za", "email": "", "has_whatsapp": False,
             "email_verified": "", "email_priority": 99},
            {"domain": "hasemail.co.za", "email": "hi@h.com", "has_whatsapp": False,
             "email_verified": "", "email_priority": 3},
        ]
        sorted_rows = sort_leads(rows)
        assert sorted_rows[0]["domain"] == "hasemail.co.za"
        assert sorted_rows[1]["domain"] == "noemail.co.za"

    def test_within_tier_sorts_by_email_priority(self):
        rows = [
            {"domain": "support.co.za", "email": "support@s.com", "has_whatsapp": True,
             "email_verified": "", "email_priority": 6},
            {"domain": "owner.co.za", "email": "owner@o.com", "has_whatsapp": True,
             "email_verified": "", "email_priority": 1},
        ]
        sorted_rows = sort_leads(rows)
        assert sorted_rows[0]["domain"] == "owner.co.za"

    def test_whatsapp_no_email_beats_email_only(self):
        """WA without email should rank above plain email without WA."""
        rows = [
            {"domain": "emailonly.co.za", "email": "hi@e.com", "has_whatsapp": False,
             "email_verified": "", "email_priority": 3},
            {"domain": "waonly.co.za", "email": "", "has_whatsapp": True,
             "email_verified": "", "email_priority": 99},
        ]
        sorted_rows = sort_leads(rows)
        assert sorted_rows[0]["domain"] == "waonly.co.za"

    def test_empty_list(self):
        assert sort_leads([]) == []

    def test_handles_string_email_priority(self):
        """CSV reload may return priority as string."""
        rows = [
            {"domain": "a.co.za", "email": "a@a.com", "has_whatsapp": False,
             "email_verified": "", "email_priority": "3"},
        ]
        sorted_rows = sort_leads(rows)
        assert len(sorted_rows) == 1


# ---------------------------------------------------------------------------
# CSV generation tests
# ---------------------------------------------------------------------------

class TestGenerateCSV:
    def test_generates_csv_file(self, tmp_path):
        scrapes = [
            _make_scrape_result("store1.co.za", email="hi@store1.co.za",
                                has_wa=True, wa_conf="definitive"),
            _make_scrape_result("store2.co.za", email="hi@store2.co.za"),
        ]

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("output.OUTPUT_DIR", str(tmp_path))
            filepath = generate_csv(scrapes, country="ZA")

        assert os.path.exists(filepath)
        assert filepath.endswith(".csv")

        # Read and verify content
        with open(filepath) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["country"] == "ZA"

    def test_filters_failed_scrapes(self, tmp_path):
        scrapes = [
            _make_scrape_result("good.co.za", email="hi@good.co.za", status="success"),
            _make_scrape_result("bad.co.za", status="failed"),
            _make_scrape_result("skip.co.za", status="skipped"),
        ]

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("output.OUTPUT_DIR", str(tmp_path))
            filepath = generate_csv(scrapes)

        with open(filepath) as f:
            rows = list(csv.DictReader(f))

        assert len(rows) == 1
        assert rows[0]["domain"] == "good.co.za"

    def test_sorts_whatsapp_first(self, tmp_path):
        scrapes = [
            _make_scrape_result("nowa.co.za", email="hi@nowa.co.za"),
            _make_scrape_result("haswa.co.za", email="hi@haswa.co.za",
                                has_wa=True, wa_conf="definitive"),
        ]

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("output.OUTPUT_DIR", str(tmp_path))
            filepath = generate_csv(scrapes)

        with open(filepath) as f:
            rows = list(csv.DictReader(f))

        assert rows[0]["domain"] == "haswa.co.za"

    def test_includes_verification_status(self, tmp_path):
        scrapes = [
            _make_scrape_result("store.co.za", email="hi@store.co.za"),
        ]
        verifies = [
            _make_verify_result("hi@store.co.za", status="safe"),
        ]

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("output.OUTPUT_DIR", str(tmp_path))
            filepath = generate_csv(scrapes, verifies)

        with open(filepath) as f:
            rows = list(csv.DictReader(f))

        assert rows[0]["email_verified"] == "safe"

    def test_empty_results_creates_header_only(self, tmp_path):
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("output.OUTPUT_DIR", str(tmp_path))
            filepath = generate_csv([])

        with open(filepath) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 0
        # But header should exist
        with open(filepath) as f:
            header = f.readline()
        assert "domain" in header


# ---------------------------------------------------------------------------
# Load CSV tests
# ---------------------------------------------------------------------------

class TestLoadCSV:
    def test_roundtrip(self, tmp_path):
        """Generate then load should return same data."""
        scrapes = [
            _make_scrape_result("store.co.za", email="hi@store.co.za",
                                has_wa=True, wa_conf="definitive", wa_phone="27821234567"),
        ]

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("output.OUTPUT_DIR", str(tmp_path))
            filepath = generate_csv(scrapes, country="ZA")

        rows = load_csv(filepath)
        assert len(rows) == 1
        assert rows[0]["domain"] == "store.co.za"
        assert rows[0]["email"] == "hi@store.co.za"
        assert rows[0]["has_whatsapp"] is True
        assert rows[0]["whatsapp_phone"] == "27821234567"

    def test_boolean_conversion(self, tmp_path):
        """String 'True'/'False' should convert back to bool."""
        scrapes = [
            _make_scrape_result("a.co.za", email="a@a.co.za", has_wa=True),
            _make_scrape_result("b.co.za", email="b@b.co.za", has_wa=False),
        ]

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("output.OUTPUT_DIR", str(tmp_path))
            filepath = generate_csv(scrapes)

        rows = load_csv(filepath)
        assert rows[0]["has_whatsapp"] is True
        assert rows[1]["has_whatsapp"] is False


# ---------------------------------------------------------------------------
# Summary tests
# ---------------------------------------------------------------------------

class TestPrintSummary:
    def test_runs_without_error(self, capsys):
        rows = [
            {"email": "a@a.com", "has_whatsapp": True, "email_verified": "safe"},
            {"email": "b@b.com", "has_whatsapp": False, "email_verified": "invalid"},
            {"email": "", "has_whatsapp": True, "email_verified": ""},
        ]
        print_summary(rows)
        captured = capsys.readouterr()
        assert "Total leads:" in captured.out
        assert "3" in captured.out

    def test_empty_rows(self, capsys):
        print_summary([])
        captured = capsys.readouterr()
        assert "Total leads:" in captured.out
        assert "0" in captured.out
