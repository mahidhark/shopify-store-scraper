"""
Shopify Store Scraper — Email Verifier Module
===============================================
Verify extracted emails using Reacher CLI (self-hosted).
SMTP-level verification — checks if mailbox exists without sending.

Prerequisites:
    Reacher CLI installed at /usr/local/bin/reacher

Usage:
    from verifier import verify_email, verify_emails_batch
    result = verify_email("hello@store.co.za")
    results = verify_emails_batch(["a@store.co.za", "b@shop.co.za"])
"""

import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REACHER_FROM_EMAIL: str = os.environ.get("REACHER_FROM_EMAIL", "mahi@whatsscale.com")
REACHER_HELLO_NAME: str = os.environ.get("REACHER_HELLO_NAME", "whatsscale.com")

# Rate limiting — be gentle with target SMTP servers
VERIFY_DELAY: float = 1.0
VERIFY_TIMEOUT: int = 30
VERIFY_MAX_RETRIES: int = 2
VERIFY_BATCH_PAUSE: float = 5.0
VERIFY_BATCH_SIZE: int = 20


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class VerifyResult:
    """Email verification result."""
    email: str
    status: str = "unknown"
    is_reachable: str = "unknown"
    is_disposable: bool = False
    is_role_account: bool = False
    mx_found: bool = False
    smtp_success: bool = False
    error: str = ""


# ---------------------------------------------------------------------------
# Single email verification
# ---------------------------------------------------------------------------

def verify_email(email: str) -> VerifyResult:
    """
    Verify a single email address using Reacher CLI.

    Returns VerifyResult with status:
        - "safe": Mailbox exists, safe to send
        - "risky": Mailbox might exist (catch-all, etc)
        - "invalid": Mailbox does not exist
        - "unknown": Verification failed (timeout, error)
    """
    result = VerifyResult(email=email)

    for attempt in range(VERIFY_MAX_RETRIES + 1):
        try:
            proc = subprocess.run(
                [
                    "reacher", email,
                    "--from-email", REACHER_FROM_EMAIL,
                    "--hello-name", REACHER_HELLO_NAME,
                ],
                capture_output=True,
                text=True,
                timeout=VERIFY_TIMEOUT,
            )

            if proc.returncode == 0 and proc.stdout.strip():
                data = json.loads(proc.stdout)
                result = _parse_reacher_response(email, data)
                logger.info(
                    f"  Verified {email}: {result.status} "
                    f"(reachable={result.is_reachable}, mx={result.mx_found})"
                )
                return result

            logger.warning(f"  Reacher CLI failed for {email}: {proc.stderr[:100]}")

        except FileNotFoundError:
            logger.error("  Reacher CLI not found. Install reacher binary to /usr/local/bin/reacher")
            result.error = "reacher_not_installed"
            return result

        except subprocess.TimeoutExpired:
            logger.warning(f"  Reacher timeout for {email} (attempt {attempt + 1})")

        except Exception as e:
            logger.warning(f"  Reacher error for {email}: {e}")

        if attempt < VERIFY_MAX_RETRIES:
            time.sleep(2)

    result.status = "unknown"
    result.error = "max_retries_exceeded"
    return result


# ---------------------------------------------------------------------------
# Parse Reacher response
# ---------------------------------------------------------------------------

def _parse_reacher_response(email: str, data: dict) -> VerifyResult:
    """Parse Reacher JSON output into a VerifyResult."""
    result = VerifyResult(email=email)

    try:
        result.is_reachable = data.get("is_reachable", "unknown")

        if result.is_reachable == "safe":
            result.status = "safe"
        elif result.is_reachable == "risky":
            result.status = "risky"
        elif result.is_reachable == "invalid":
            result.status = "invalid"
        else:
            result.status = "unknown"

        misc = data.get("misc", {})
        result.is_disposable = misc.get("is_disposable", False)
        result.is_role_account = misc.get("is_role_account", False)

        mx = data.get("mx", {})
        result.mx_found = mx.get("accepts_mail", False)

        smtp = data.get("smtp", {})
        result.smtp_success = smtp.get("is_deliverable", False)

    except (KeyError, TypeError, AttributeError) as e:
        logger.warning(f"  Error parsing Reacher response for {email}: {e}")
        result.status = "unknown"
        result.error = f"parse_error: {e}"

    return result


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def _check_reacher_health() -> bool:
    """Check if Reacher CLI is installed and working."""
    try:
        proc = subprocess.run(["reacher", "--version"], capture_output=True, timeout=5)
        return proc.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


# ---------------------------------------------------------------------------
# Batch verification
# ---------------------------------------------------------------------------

def verify_emails_batch(emails: List[str]) -> List[VerifyResult]:
    """
    Verify a list of emails with rate limiting.
    Pauses every VERIFY_BATCH_SIZE emails.
    """
    results = []
    total = len(emails)

    if not emails:
        return results

    if not _check_reacher_health():
        logger.error("Reacher CLI is not available. Skipping all verifications.")
        return [VerifyResult(email=e, status="unknown", error="reacher_unavailable")
                for e in emails]

    for i, email in enumerate(emails):
        logger.info(f"[{i+1}/{total}] Verifying: {email}")

        result = verify_email(email)
        results.append(result)

        if i < total - 1:
            time.sleep(VERIFY_DELAY)

        if (i + 1) % VERIFY_BATCH_SIZE == 0 and i < total - 1:
            logger.info(f"  Batch pause ({VERIFY_BATCH_PAUSE}s)...")
            time.sleep(VERIFY_BATCH_PAUSE)

    safe = sum(1 for r in results if r.status == "safe")
    risky = sum(1 for r in results if r.status == "risky")
    invalid = sum(1 for r in results if r.status == "invalid")
    unknown = sum(1 for r in results if r.status == "unknown")

    logger.info(
        f"Verification complete: {safe} safe, {risky} risky, "
        f"{invalid} invalid, {unknown} unknown"
    )

    return results


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
    parser = argparse.ArgumentParser(description="Verify emails using Reacher CLI")
    parser.add_argument("emails", nargs="*", help="Emails to verify")
    parser.add_argument("--from-file", type=str,
                        help="Read emails from file (one per line)")
    args = parser.parse_args()

    emails = list(args.emails)
    if args.from_file:
        with open(args.from_file) as f:
            emails.extend(line.strip() for line in f if line.strip() and "@" in line)

    if not emails:
        print("No emails provided. Use: python verifier.py hello@store.co.za")
        print("Or: python verifier.py --from-file emails.txt")
        exit(1)

    results = verify_emails_batch(emails)

    for r in results:
        icon = {"safe": "✓", "risky": "⚠", "invalid": "✗", "unknown": "?"}.get(r.status, "?")
        flags = []
        if r.is_disposable:
            flags.append("disposable")
        if r.is_role_account:
            flags.append("role")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        print(f"  {icon} {r.email:40s} → {r.status}{flag_str}")
