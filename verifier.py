"""
Shopify Store Scraper — Email Verifier Module
===============================================
Verify extracted emails using Reacher (self-hosted).
SMTP-level verification — checks if mailbox exists without sending.

Prerequisites:
    Reacher running on VPS via Docker:
    docker run -p 8080:8080 reacherhq/backend:latest

Usage:
    from verifier import verify_email, verify_emails_batch
    result = verify_email("hello@store.co.za")
    results = verify_emails_batch(["a@store.co.za", "b@shop.co.za"])
"""

import logging
import time
import os
from dataclasses import dataclass
from typing import List, Optional, Dict

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Reacher API endpoint (self-hosted)
REACHER_URL: str = os.environ.get("REACHER_URL", "http://localhost:8080")
REACHER_ENDPOINT: str = f"{REACHER_URL}/v0/check_email"

# Rate limiting — be gentle even with self-hosted
VERIFY_DELAY: float = 1.0           # Seconds between verifications
VERIFY_TIMEOUT: int = 30            # Request timeout (SMTP can be slow)
VERIFY_MAX_RETRIES: int = 2         # Retries per email on failure
VERIFY_BATCH_PAUSE: float = 5.0     # Pause every N verifications
VERIFY_BATCH_SIZE: int = 20         # Verifications before pause


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class VerifyResult:
    """Email verification result."""
    email: str
    status: str = "unknown"       # "safe" | "risky" | "invalid" | "unknown"
    is_reachable: str = "unknown"  # "safe" | "risky" | "invalid" | "unknown"
    is_disposable: bool = False
    is_role_account: bool = False  # info@, support@, etc.
    mx_found: bool = False
    smtp_success: bool = False
    error: str = ""


# ---------------------------------------------------------------------------
# Single email verification
# ---------------------------------------------------------------------------

def verify_email(email: str) -> VerifyResult:
    """
    Verify a single email address using Reacher.
    
    Returns VerifyResult with status:
        - "safe": Mailbox exists, safe to send
        - "risky": Mailbox might exist (catch-all, etc)
        - "invalid": Mailbox does not exist
        - "unknown": Verification failed (timeout, error)
    """
    result = VerifyResult(email=email)

    for attempt in range(VERIFY_MAX_RETRIES + 1):
        try:
            resp = requests.post(
                REACHER_ENDPOINT,
                json={"to_email": email},
                timeout=VERIFY_TIMEOUT,
                headers={"Content-Type": "application/json"},
            )

            if resp.status_code == 200:
                data = resp.json()
                result = _parse_reacher_response(email, data)
                logger.info(
                    f"  Verified {email}: {result.status} "
                    f"(reachable={result.is_reachable}, mx={result.mx_found})"
                )
                return result

            logger.warning(f"  Reacher HTTP {resp.status_code} for {email}")

        except requests.ConnectionError:
            logger.error(
                f"  Cannot connect to Reacher at {REACHER_URL}. "
                f"Is it running? Start with: docker run -p 8080:8080 reacherhq/backend:latest"
            )
            result.error = "reacher_unavailable"
            return result

        except requests.Timeout:
            logger.warning(f"  Reacher timeout for {email} (attempt {attempt + 1})")

        except requests.RequestException as e:
            logger.warning(f"  Reacher error for {email}: {e}")

        # Retry delay
        if attempt < VERIFY_MAX_RETRIES:
            time.sleep(2)

    result.status = "unknown"
    result.error = "max_retries_exceeded"
    return result


def _parse_reacher_response(email: str, data: dict) -> VerifyResult:
    """
    Parse Reacher API response into a VerifyResult.
    
    Reacher response structure:
    {
        "input": "hello@store.co.za",
        "is_reachable": "safe" | "risky" | "invalid" | "unknown",
        "misc": {"is_disposable": bool, "is_role_account": bool},
        "mx": {"accepts_mail": bool, "records": [...]},
        "smtp": {"can_connect_smtp": bool, "is_deliverable": bool, ...}
    }
    """
    result = VerifyResult(email=email)

    try:
        # Top-level reachability
        result.is_reachable = data.get("is_reachable", "unknown")

        # Map to our status
        reachable = result.is_reachable
        if reachable == "safe":
            result.status = "safe"
        elif reachable == "risky":
            result.status = "risky"
        elif reachable == "invalid":
            result.status = "invalid"
        else:
            result.status = "unknown"

        # Misc fields
        misc = data.get("misc", {})
        result.is_disposable = misc.get("is_disposable", False)
        result.is_role_account = misc.get("is_role_account", False)

        # MX records
        mx = data.get("mx", {})
        result.mx_found = mx.get("accepts_mail", False)

        # SMTP check
        smtp = data.get("smtp", {})
        result.smtp_success = smtp.get("is_deliverable", False)

    except (KeyError, TypeError, AttributeError) as e:
        logger.warning(f"  Error parsing Reacher response for {email}: {e}")
        result.status = "unknown"
        result.error = f"parse_error: {e}"

    return result


# ---------------------------------------------------------------------------
# Batch verification
# ---------------------------------------------------------------------------

def verify_emails_batch(emails: List[str]) -> List[VerifyResult]:
    """
    Verify a list of emails with rate limiting.
    
    Pauses every VERIFY_BATCH_SIZE emails to avoid overwhelming
    the SMTP servers we're checking against.
    """
    results = []
    total = len(emails)

    if not emails:
        return results

    # Quick connectivity check
    if not _check_reacher_health():
        logger.error("Reacher is not available. Skipping all verifications.")
        return [VerifyResult(email=e, status="unknown", error="reacher_unavailable")
                for e in emails]

    for i, email in enumerate(emails):
        logger.info(f"[{i+1}/{total}] Verifying: {email}")

        result = verify_email(email)
        results.append(result)

        # Delay between verifications
        if i < total - 1:
            time.sleep(VERIFY_DELAY)

        # Batch pause
        if (i + 1) % VERIFY_BATCH_SIZE == 0 and i < total - 1:
            logger.info(f"  Batch pause ({VERIFY_BATCH_PAUSE}s)...")
            time.sleep(VERIFY_BATCH_PAUSE)

    # Summary
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
# Health check
# ---------------------------------------------------------------------------

def _check_reacher_health() -> bool:
    """Check if Reacher is running and accessible."""
    try:
        resp = requests.get(f"{REACHER_URL}/", timeout=5)
        return resp.status_code == 200
    except requests.RequestException:
        return False


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
    parser = argparse.ArgumentParser(description="Verify emails using Reacher")
    parser.add_argument("emails", nargs="*", help="Emails to verify")
    parser.add_argument("--from-file", type=str,
                        help="Read emails from file (one per line)")
    parser.add_argument("--reacher-url", type=str, default=REACHER_URL,
                        help=f"Reacher API URL (default: {REACHER_URL})")
    args = parser.parse_args()

    if args.reacher_url != REACHER_URL:
        REACHER_ENDPOINT = f"{args.reacher_url}/v0/check_email"

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
