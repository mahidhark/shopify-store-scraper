"""Tests for verifier.py — email verification with mocked Reacher API."""

import pytest
from unittest.mock import patch, MagicMock

from verifier import (
    verify_email,
    verify_emails_batch,
    _parse_reacher_response,
    _check_reacher_health,
    VerifyResult,
)


# ---------------------------------------------------------------------------
# Sample Reacher API responses
# ---------------------------------------------------------------------------

REACHER_SAFE_RESPONSE = {
    "input": "hello@store.co.za",
    "is_reachable": "safe",
    "misc": {"is_disposable": False, "is_role_account": False},
    "mx": {"accepts_mail": True, "records": ["mx1.store.co.za"]},
    "smtp": {"can_connect_smtp": True, "is_deliverable": True},
}

REACHER_INVALID_RESPONSE = {
    "input": "fake@store.co.za",
    "is_reachable": "invalid",
    "misc": {"is_disposable": False, "is_role_account": False},
    "mx": {"accepts_mail": True, "records": ["mx1.store.co.za"]},
    "smtp": {"can_connect_smtp": True, "is_deliverable": False},
}

REACHER_RISKY_RESPONSE = {
    "input": "info@store.co.za",
    "is_reachable": "risky",
    "misc": {"is_disposable": False, "is_role_account": True},
    "mx": {"accepts_mail": True, "records": ["mx1.store.co.za"]},
    "smtp": {"can_connect_smtp": True, "is_deliverable": False},
}

REACHER_DISPOSABLE_RESPONSE = {
    "input": "temp@tempmail.com",
    "is_reachable": "invalid",
    "misc": {"is_disposable": True, "is_role_account": False},
    "mx": {"accepts_mail": True, "records": []},
    "smtp": {"can_connect_smtp": False, "is_deliverable": False},
}

REACHER_UNKNOWN_RESPONSE = {
    "input": "mystery@store.co.za",
    "is_reachable": "unknown",
    "misc": {"is_disposable": False, "is_role_account": False},
    "mx": {"accepts_mail": False, "records": []},
    "smtp": {"can_connect_smtp": False, "is_deliverable": False},
}


# ---------------------------------------------------------------------------
# Parse response tests
# ---------------------------------------------------------------------------

class TestParseReacherResponse:
    def test_safe_email(self):
        result = _parse_reacher_response("hello@store.co.za", REACHER_SAFE_RESPONSE)
        assert result.status == "safe"
        assert result.is_reachable == "safe"
        assert result.mx_found is True
        assert result.smtp_success is True
        assert result.is_disposable is False
        assert result.is_role_account is False

    def test_invalid_email(self):
        result = _parse_reacher_response("fake@store.co.za", REACHER_INVALID_RESPONSE)
        assert result.status == "invalid"
        assert result.is_reachable == "invalid"
        assert result.smtp_success is False

    def test_risky_email(self):
        result = _parse_reacher_response("info@store.co.za", REACHER_RISKY_RESPONSE)
        assert result.status == "risky"
        assert result.is_role_account is True

    def test_disposable_email(self):
        result = _parse_reacher_response("temp@tempmail.com", REACHER_DISPOSABLE_RESPONSE)
        assert result.status == "invalid"
        assert result.is_disposable is True

    def test_unknown_email(self):
        result = _parse_reacher_response("mystery@store.co.za", REACHER_UNKNOWN_RESPONSE)
        assert result.status == "unknown"

    def test_handles_empty_response(self):
        result = _parse_reacher_response("test@test.com", {})
        assert result.status == "unknown"
        assert result.mx_found is False

    def test_handles_missing_fields(self):
        """Gracefully handle partial response."""
        partial = {"is_reachable": "safe"}
        result = _parse_reacher_response("test@test.com", partial)
        assert result.status == "safe"
        assert result.is_disposable is False  # Default


# ---------------------------------------------------------------------------
# verify_email tests (mocked HTTP)
# ---------------------------------------------------------------------------

class TestVerifyEmail:
    @patch("verifier.requests.post")
    def test_successful_verification(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = REACHER_SAFE_RESPONSE
        mock_post.return_value = mock_resp

        result = verify_email("hello@store.co.za")

        assert result.status == "safe"
        assert result.email == "hello@store.co.za"
        mock_post.assert_called_once()

    @patch("verifier.requests.post")
    def test_invalid_email_verification(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = REACHER_INVALID_RESPONSE
        mock_post.return_value = mock_resp

        result = verify_email("fake@store.co.za")
        assert result.status == "invalid"

    @patch("verifier.requests.post")
    def test_reacher_connection_error(self, mock_post):
        """Should return unknown when Reacher is unavailable."""
        import requests as req
        mock_post.side_effect = req.ConnectionError("Connection refused")

        result = verify_email("hello@store.co.za")

        assert result.status == "unknown"
        assert result.error == "reacher_unavailable"

    @patch("verifier.time.sleep")
    @patch("verifier.requests.post")
    def test_reacher_timeout_retries(self, mock_post, mock_sleep):
        """Should retry on timeout."""
        import requests as req
        mock_post.side_effect = req.Timeout("Timed out")

        result = verify_email("slow@store.co.za")

        assert result.status == "unknown"
        assert result.error == "max_retries_exceeded"
        # Should have tried VERIFY_MAX_RETRIES + 1 times
        assert mock_post.call_count == 3  # 1 initial + 2 retries

    @patch("verifier.time.sleep")
    @patch("verifier.requests.post")
    def test_retry_then_success(self, mock_post, mock_sleep):
        """Should succeed after retry."""
        import requests as req

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = REACHER_SAFE_RESPONSE

        # First call times out, second succeeds
        mock_post.side_effect = [req.Timeout("Timed out"), mock_resp]

        result = verify_email("retry@store.co.za")

        assert result.status == "safe"
        assert mock_post.call_count == 2

    @patch("verifier.requests.post")
    def test_reacher_http_error(self, mock_post):
        """Should handle non-200 responses."""
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_post.return_value = mock_resp

        result = verify_email("error@store.co.za")
        assert result.status == "unknown"


# ---------------------------------------------------------------------------
# Batch verification tests
# ---------------------------------------------------------------------------

class TestVerifyEmailsBatch:
    @patch("verifier.time.sleep")
    @patch("verifier._check_reacher_health", return_value=True)
    @patch("verifier.verify_email")
    def test_verifies_all_emails(self, mock_verify, mock_health, mock_sleep):
        mock_verify.return_value = VerifyResult(email="test@t.com", status="safe")

        results = verify_emails_batch(["a@t.com", "b@t.com", "c@t.com"])

        assert len(results) == 3
        assert mock_verify.call_count == 3

    @patch("verifier._check_reacher_health", return_value=False)
    def test_skips_all_when_reacher_down(self, mock_health):
        """Should return unknown for all when Reacher unavailable."""
        results = verify_emails_batch(["a@t.com", "b@t.com"])

        assert len(results) == 2
        assert all(r.status == "unknown" for r in results)
        assert all(r.error == "reacher_unavailable" for r in results)

    def test_empty_list_returns_empty(self):
        results = verify_emails_batch([])
        assert results == []

    @patch("verifier.time.sleep")
    @patch("verifier._check_reacher_health", return_value=True)
    @patch("verifier.verify_email")
    def test_adds_delay_between_emails(self, mock_verify, mock_health, mock_sleep):
        mock_verify.return_value = VerifyResult(email="t@t.com", status="safe")

        verify_emails_batch(["a@t.com", "b@t.com", "c@t.com"])

        # Should sleep between emails (2 sleeps for 3 emails)
        assert mock_sleep.call_count >= 2


# ---------------------------------------------------------------------------
# Health check tests
# ---------------------------------------------------------------------------

class TestHealthCheck:
    @patch("verifier.requests.get")
    def test_healthy_reacher(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp

        assert _check_reacher_health() is True

    @patch("verifier.requests.get")
    def test_unreachable_reacher(self, mock_get):
        import requests as req
        mock_get.side_effect = req.ConnectionError("Refused")

        assert _check_reacher_health() is False

    @patch("verifier.requests.get")
    def test_reacher_error_status(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_get.return_value = mock_resp

        assert _check_reacher_health() is False
