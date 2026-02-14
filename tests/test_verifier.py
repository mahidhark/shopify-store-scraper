"""Tests for verifier.py — email verification with mocked Reacher CLI."""

import json
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
# Sample Reacher CLI JSON outputs
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
        partial = {"is_reachable": "safe"}
        result = _parse_reacher_response("test@test.com", partial)
        assert result.status == "safe"
        assert result.is_disposable is False


# ---------------------------------------------------------------------------
# verify_email tests (mocked subprocess)
# ---------------------------------------------------------------------------

class TestVerifyEmail:
    @patch("verifier.subprocess.run")
    def test_successful_verification(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps(REACHER_SAFE_RESPONSE),
            stderr="",
        )

        result = verify_email("hello@store.co.za")

        assert result.status == "safe"
        assert result.email == "hello@store.co.za"
        mock_run.assert_called_once()

    @patch("verifier.subprocess.run")
    def test_invalid_email_verification(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps(REACHER_INVALID_RESPONSE),
            stderr="",
        )

        result = verify_email("fake@store.co.za")
        assert result.status == "invalid"

    @patch("verifier.subprocess.run")
    def test_reacher_not_installed(self, mock_run):
        mock_run.side_effect = FileNotFoundError("reacher not found")

        result = verify_email("hello@store.co.za")

        assert result.status == "unknown"
        assert result.error == "reacher_not_installed"

    @patch("verifier.time.sleep")
    @patch("verifier.subprocess.run")
    def test_reacher_timeout_retries(self, mock_run, mock_sleep):
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="reacher", timeout=30)

        result = verify_email("slow@store.co.za")

        assert result.status == "unknown"
        assert result.error == "max_retries_exceeded"
        assert mock_run.call_count == 3  # 1 initial + 2 retries

    @patch("verifier.time.sleep")
    @patch("verifier.subprocess.run")
    def test_retry_then_success(self, mock_run, mock_sleep):
        import subprocess

        success = MagicMock(
            returncode=0,
            stdout=json.dumps(REACHER_SAFE_RESPONSE),
            stderr="",
        )

        mock_run.side_effect = [
            subprocess.TimeoutExpired(cmd="reacher", timeout=30),
            success,
        ]

        result = verify_email("retry@store.co.za")

        assert result.status == "safe"
        assert mock_run.call_count == 2

    @patch("verifier.subprocess.run")
    def test_reacher_cli_error(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Error: something went wrong",
        )

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

        assert mock_sleep.call_count >= 2


# ---------------------------------------------------------------------------
# Health check tests
# ---------------------------------------------------------------------------

class TestHealthCheck:
    @patch("verifier.subprocess.run")
    def test_healthy_reacher(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        assert _check_reacher_health() is True

    @patch("verifier.subprocess.run")
    def test_reacher_not_installed(self, mock_run):
        mock_run.side_effect = FileNotFoundError("not found")
        assert _check_reacher_health() is False

    @patch("verifier.subprocess.run")
    def test_reacher_timeout(self, mock_run):
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="reacher", timeout=5)
        assert _check_reacher_health() is False
