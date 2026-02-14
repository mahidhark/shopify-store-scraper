"""Tests for discovery.py — domain normalization, dedup, state, query generation."""

import json
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from discovery import (
    normalize_domain,
    SeenDomains,
    DorkState,
    generate_dork_queries,
    _extract_domains_from_urls,
    discover_stores,
)


# ---------------------------------------------------------------------------
# Domain normalization tests
# ---------------------------------------------------------------------------

class TestNormalizeDomain:
    def test_basic_url(self):
        assert normalize_domain("https://mystore.co.za") == "mystore.co.za"

    def test_strips_www(self):
        assert normalize_domain("https://www.mystore.co.za") == "mystore.co.za"

    def test_strips_path(self):
        assert normalize_domain("https://mystore.co.za/products/shoes") == "mystore.co.za"

    def test_strips_protocol(self):
        assert normalize_domain("http://mystore.co.za") == "mystore.co.za"

    def test_adds_protocol_if_missing(self):
        assert normalize_domain("mystore.co.za") == "mystore.co.za"

    def test_lowercase(self):
        assert normalize_domain("https://MyStore.CO.ZA") == "mystore.co.za"

    def test_strips_port(self):
        assert normalize_domain("https://mystore.co.za:8080") == "mystore.co.za"

    def test_myshopify_subdomain(self):
        assert normalize_domain("https://coolshop.myshopify.com") == "coolshop.myshopify.com"

    def test_returns_none_for_empty(self):
        assert normalize_domain("") is None
        assert normalize_domain(None) is None

    def test_returns_none_for_garbage(self):
        assert normalize_domain("not-a-url") is None
        assert normalize_domain("ftp://x") is None

    def test_subdomain_preserved(self):
        assert normalize_domain("https://shop.example.co.za") == "shop.example.co.za"

    def test_query_params_stripped(self):
        assert normalize_domain("https://mystore.co.za?ref=google") == "mystore.co.za"


# ---------------------------------------------------------------------------
# SeenDomains tests
# ---------------------------------------------------------------------------

class TestSeenDomains:
    def test_new_domain_is_new(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            seen = SeenDomains(filepath=path)
            assert seen.is_new("store.co.za")
        finally:
            os.unlink(path) if os.path.exists(path) else None

    def test_added_domain_is_not_new(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            seen = SeenDomains(filepath=path)
            seen.add("store.co.za")
            assert not seen.is_new("store.co.za")
        finally:
            os.unlink(path) if os.path.exists(path) else None

    def test_persistence_across_instances(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            seen1 = SeenDomains(filepath=path)
            seen1.add("store1.co.za")
            seen1.add("store2.co.za")
            seen1.save()

            seen2 = SeenDomains(filepath=path)
            assert not seen2.is_new("store1.co.za")
            assert not seen2.is_new("store2.co.za")
            assert seen2.is_new("store3.co.za")
            assert len(seen2) == 2
        finally:
            os.unlink(path) if os.path.exists(path) else None

    def test_handles_missing_file(self):
        seen = SeenDomains(filepath="/tmp/nonexistent_dedup_test.json")
        assert len(seen) == 0
        assert seen.is_new("anything.co.za")

    def test_handles_corrupted_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("NOT VALID JSON{{{")
            path = f.name
        try:
            seen = SeenDomains(filepath=path)
            assert len(seen) == 0  # Should recover gracefully
        finally:
            os.unlink(path)

    def test_contains_operator(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            seen = SeenDomains(filepath=path)
            seen.add("test.co.za")
            assert "test.co.za" in seen
            assert "other.co.za" not in seen
        finally:
            os.unlink(path) if os.path.exists(path) else None


# ---------------------------------------------------------------------------
# DorkState tests
# ---------------------------------------------------------------------------

class TestDorkState:
    def test_fresh_state_starts_at_zero(self):
        state = DorkState(filepath="/tmp/nonexistent_state_test.json")
        assert state.query_index == 0

    def test_advance_increments(self):
        state = DorkState(filepath="/tmp/nonexistent_state_test.json")
        state.advance(5)
        assert state.query_index == 5

    def test_persistence_across_instances(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            state1 = DorkState(filepath=path)
            state1.query_index = 15
            state1.total_discovered = 42
            state1.save()

            state2 = DorkState(filepath=path)
            assert state2.query_index == 15
            assert state2.total_discovered == 42
        finally:
            os.unlink(path)

    def test_reset_clears_state(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            state = DorkState(filepath=path)
            state.query_index = 20
            state.total_discovered = 100
            state.reset()
            assert state.query_index == 0
            assert state.total_discovered == 0
        finally:
            os.unlink(path) if os.path.exists(path) else None

    def test_handles_corrupted_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("BROKEN")
            path = f.name
        try:
            state = DorkState(filepath=path)
            assert state.query_index == 0  # Graceful recovery
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Query generation tests
# ---------------------------------------------------------------------------

class TestGenerateDorkQueries:
    def test_generates_queries_for_enabled_countries(self):
        queries = generate_dork_queries(include_niche=False)
        # ZA has 2 TLDs, 5 base templates = 10 queries minimum
        assert len(queries) >= 10

    def test_all_queries_contain_real_tld(self):
        queries = generate_dork_queries(include_niche=False)
        for q in queries:
            assert "{tld}" not in q, f"Unresolved placeholder: {q}"

    def test_queries_target_south_africa(self):
        queries = generate_dork_queries(include_niche=False)
        # At least some queries should target .co.za
        za_queries = [q for q in queries if "co.za" in q]
        assert len(za_queries) > 0

    def test_niche_queries_add_more(self):
        base = generate_dork_queries(include_niche=False)
        with_niche = generate_dork_queries(include_niche=True)
        assert len(with_niche) > len(base)

    def test_no_duplicate_queries(self):
        queries = generate_dork_queries(include_niche=True)
        assert len(queries) == len(set(queries)), "Duplicate queries found"


# ---------------------------------------------------------------------------
# URL extraction tests
# ---------------------------------------------------------------------------

class TestExtractDomains:
    def test_extracts_from_valid_urls(self):
        urls = [
            "https://www.store1.co.za/products",
            "https://store2.co.za",
            "http://shop.store3.co.za/cart",
        ]
        domains = _extract_domains_from_urls(urls)
        assert "store1.co.za" in domains
        assert "store2.co.za" in domains
        assert "shop.store3.co.za" in domains

    def test_filters_out_garbage(self):
        urls = ["https://valid.co.za", "", None, "garbage"]
        domains = _extract_domains_from_urls(urls)
        assert "valid.co.za" in domains
        assert len(domains) == 1  # Only the valid one


# ---------------------------------------------------------------------------
# Main discover_stores tests (mocked Google)
# ---------------------------------------------------------------------------

class TestDiscoverStores:
    @patch("discovery._search_google")
    def test_dry_run_returns_empty(self, mock_search):
        """Dry run should not execute any searches."""
        result = discover_stores(dry_run=True, max_queries=2)
        assert result == []
        mock_search.assert_not_called()

    @patch("discovery.time.sleep")
    @patch("discovery._search_google")
    def test_discovers_new_domains(self, mock_search, mock_sleep, tmp_path):
        """Should return new domains from search results."""
        mock_search.return_value = [
            "https://www.store1.co.za/products",
            "https://store2.co.za",
        ]

        data_dir = str(tmp_path / "data")
        os.makedirs(data_dir, exist_ok=True)
        state_file = os.path.join(data_dir, "state.json")
        dedup_file = os.path.join(data_dir, "dedup.json")
        output_dir = str(tmp_path / "output")

        orig_dork_init = DorkState.__init__
        orig_seen_init = SeenDomains.__init__

        def patched_dork_init(self, filepath=state_file):
            orig_dork_init(self, filepath=filepath)

        def patched_seen_init(self, filepath=dedup_file):
            orig_seen_init(self, filepath=filepath)

        with patch.object(DorkState, "__init__", patched_dork_init), \
             patch.object(SeenDomains, "__init__", patched_seen_init), \
             patch("discovery.OUTPUT_DIR", output_dir):
            result = discover_stores(max_queries=1)

        assert len(result) >= 1
        # All returned domains should be normalized
        for d in result:
            assert "www." not in d
            assert d == d.lower()

    @patch("discovery.time.sleep")
    @patch("discovery._search_google")
    def test_deduplicates_across_calls(self, mock_search, mock_sleep, tmp_path):
        """Second call should not return already-seen domains."""
        mock_search.return_value = [
            "https://store1.co.za",
            "https://store2.co.za",
        ]

        data_dir = str(tmp_path / "data")
        os.makedirs(data_dir, exist_ok=True)
        state_file = os.path.join(data_dir, "state.json")
        dedup_file = os.path.join(data_dir, "dedup.json")
        output_dir = str(tmp_path / "output")

        orig_dork_init = DorkState.__init__
        orig_seen_init = SeenDomains.__init__

        def patched_dork_init(self, filepath=state_file):
            orig_dork_init(self, filepath=filepath)

        def patched_seen_init(self, filepath=dedup_file):
            orig_seen_init(self, filepath=filepath)

        with patch.object(DorkState, "__init__", patched_dork_init), \
             patch.object(SeenDomains, "__init__", patched_seen_init), \
             patch("discovery.OUTPUT_DIR", output_dir):

            # First run
            result1 = discover_stores(max_queries=1)

            # Reset state so it runs same query again
            with open(state_file, "w") as f:
                json.dump({"query_index": 0, "total_discovered": 0}, f)

            # Second run — same results from Google
            result2 = discover_stores(max_queries=1)
            # Should find 0 new domains (all seen)
            assert len(result2) == 0

    @patch("discovery._search_google")
    def test_state_advances_after_batch(self, mock_search, tmp_path):
        """Query index should advance after a batch."""
        mock_search.return_value = []

        data_dir = str(tmp_path / "data")
        os.makedirs(data_dir, exist_ok=True)
        state_file = os.path.join(data_dir, "state.json")
        dedup_file = os.path.join(data_dir, "dedup.json")
        output_dir = str(tmp_path / "output")

        orig_dork_init = DorkState.__init__
        orig_seen_init = SeenDomains.__init__

        def patched_dork_init(self, filepath=state_file):
            orig_dork_init(self, filepath=filepath)

        def patched_seen_init(self, filepath=dedup_file):
            orig_seen_init(self, filepath=filepath)

        with patch.object(DorkState, "__init__", patched_dork_init), \
             patch.object(SeenDomains, "__init__", patched_seen_init), \
             patch("discovery.OUTPUT_DIR", output_dir):

            discover_stores(max_queries=3)

            with open(state_file) as f:
                saved = json.load(f)
            assert saved["query_index"] == 3
