"""
Vendor Master — self-learning per-entity vendor profile.

Builds and maintains vendor records automatically from invoice upload activity.
Vendors accumulate:
  - GL account classification (via vendor_classification_memory, separate)
  - Banking details (transit / institution / account for EFT payments)
  - Payment terms (net-N inferred from invoice_date → due_date observations)
  - Remittance email (for PDF advice on EFT generation)
  - Confidence score (0–1, mirrors vendor_classification_memory's model)

Multi-tenancy: every function scoped by entity_id UUID.
HH AP (account 2030) is explicitly excluded — this module covers outside vendors only.
"""
from __future__ import annotations

import difflib
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from uuid import UUID

from sqlalchemy import text

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Fuzzy-match threshold for vendor name similarity.
# Below this ratio, a new vendor row is created even if a partial match exists.
FUZZY_MATCH_THRESHOLD = 0.82

# Minimum invoice observations before payment_terms_confidence exceeds 0.5.
MIN_TERMS_OBS_FOR_CONFIDENCE = 3

# Default EFT transaction type code for business chequing (AFT credit).
# TD's modern code for business-account credits. Confirm with TD before go-live.
DEFAULT_EFT_TRANSACTION_TYPE = "470"


# ---------------------------------------------------------------------------
# Internal: normalization (reuses vendor_classification_memory's approach)
# ---------------------------------------------------------------------------

def _normalize_vendor_name(name: str | None) -> str:
    """Light normalization for invoice vendor names (not bank-description noise
    removal). Uppercases, strips common legal suffixes, collapses whitespace.
    Returns empty string for None/empty input."""
    if not name or not name.strip():
        return ""
    # Import the bank-description normalizer but apply it to the clean name.
    # It handles punctuation, noise tokens (INC, LTD, etc.), and tokenization.
    try:
        from .services_vendor_classification import _normalize_vendor_key
        result = _normalize_vendor_key(name)
    except Exception:
        # Fallback: simple uppercase + collapse
        import re
        result = re.sub(r'[^A-Z0-9 \-]', ' ', name.upper())
        result = '_'.join(t for t in result.split() if t)
    return result


# ---------------------------------------------------------------------------
# Core: ensure_vendor (auto-create or match)
# ---------------------------------------------------------------------------

def ensure_vendor(
    session,
    *,
    entity_id: UUID,
    vendor_name: str,
    invoice_date=None,
    due_date=None,
    invoice_id: str | None = None,
) -> dict[str, Any]:
    """Return the vendor master row for vendor_name, creating it if new.

    Matching priority:
    1. Exact match on normalized key (vendor_normalized column).
    2. Fuzzy match via difflib on all existing vendor rows (ratio >= threshold).
    3. Create new row.

    Updates invoice_count, last_seen_at on every call.
    If invoice_date and due_date are both provided, observes payment terms.

    Returns the vendor row as a dict.
    """
    entity_id_str = str(entity_id)
    normalized = _normalize_vendor_name(vendor_name)
    if not normalized:
        _log.warning("ensure_vendor: empty normalized key for %r", vendor_name)
        normalized = vendor_name.upper()[:80] if vendor_name else "UNKNOWN"

    # --- Pass 1: exact match on vendor_normalized ---
    existing = session.execute(
        text("""
            SELECT id, entity_id, vendor_name, vendor_normalized,
                   default_account_code, remittance_email, bank_transit,
                   bank_institution, bank_account, eft_transaction_type,
                   payment_terms_days, payment_terms_confidence,
                   profile_confidence, invoice_count, first_seen_at,
                   last_seen_at, banking_confirmed_at, banking_confirmed_by,
                   created_at, updated_at
            FROM vendors
            WHERE entity_id = :eid AND vendor_normalized = :norm
        """),
        {"eid": entity_id_str, "norm": normalized},
    ).mappings().first()

    if not existing:
        # --- Pass 2: fuzzy match against all entity vendors ---
        all_vendors = session.execute(
            text("""
                SELECT id, vendor_name, vendor_normalized
                FROM vendors
                WHERE entity_id = :eid
            """),
            {"eid": entity_id_str},
        ).mappings().all()

        best_match = None
        best_ratio = 0.0
        for v in all_vendors:
            vn = v["vendor_normalized"] or ""
            ratio = difflib.SequenceMatcher(None, normalized, vn).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = v

        if best_match and best_ratio >= FUZZY_MATCH_THRESHOLD:
            _log.info(
                "ensure_vendor: fuzzy match %r -> %r (%.2f) for entity %s",
                vendor_name, best_match["vendor_name"], best_ratio, entity_id,
            )
            # Reload full row
            existing = session.execute(
                text("""
                    SELECT id, entity_id, vendor_name, vendor_normalized,
                           default_account_code, remittance_email, bank_transit,
                           bank_institution, bank_account, eft_transaction_type,
                           payment_terms_days, payment_terms_confidence,
                           profile_confidence, invoice_count, first_seen_at,
                           last_seen_at, banking_confirmed_at, banking_confirmed_by,
                           created_at, updated_at
                    FROM vendors WHERE id = :vid
                """),
                {"vid": str(best_match["id"])},
            ).mappings().first()

    now_utc = datetime.now(timezone.utc)

    if existing:
        # Update counters
        session.execute(
            text("""
                UPDATE vendors
                   SET invoice_count = invoice_count + 1,
                       last_seen_at  = :now,
                       updated_at    = :now
                 WHERE id = :vid
            """),
            {"vid": str(existing["id"]), "now": now_utc},
        )
        row_id = str(existing["id"])
    else:
        # Create new vendor
        row = session.execute(
            text("""
                INSERT INTO vendors (entity_id, vendor_name, vendor_normalized,
                                     invoice_count, first_seen_at, last_seen_at,
                                     created_at, updated_at)
                VALUES (:eid, :name, :norm, 1, :now, :now, :now, :now)
                RETURNING id
            """),
            {
                "eid": entity_id_str,
                "name": vendor_name,
                "norm": normalized,
                "now": now_utc,
            },
        ).mappings().first()
        assert row is not None
        row_id = str(row["id"])
        _log.info("ensure_vendor: created new vendor %r (id=%s)", vendor_name, row_id)

    # Observe payment terms if both dates provided
    if invoice_date and due_date and invoice_date <= due_date:
        observe_payment_terms(
            session,
            vendor_id=UUID(row_id),
            entity_id=entity_id,
            invoice_date=invoice_date,
            due_date=due_date,
            invoice_id=invoice_id,
        )

    # Reload + return fresh row
    updated = session.execute(
        text("""
            SELECT id, entity_id, vendor_name, vendor_normalized,
                   default_account_code, remittance_email, bank_transit,
                   bank_institution, bank_account, eft_transaction_type,
                   payment_terms_days, payment_terms_confidence,
                   profile_confidence, invoice_count, first_seen_at,
                   last_seen_at, banking_confirmed_at, banking_confirmed_by,
                   created_at, updated_at
            FROM vendors WHERE id = :vid
        """),
        {"vid": row_id},
    ).mappings().first()
    return dict(updated)


# ---------------------------------------------------------------------------
# Payment terms observation + recompute
# ---------------------------------------------------------------------------

def observe_payment_terms(
    session,
    *,
    vendor_id: UUID,
    entity_id: UUID,
    invoice_date,
    due_date,
    invoice_id: str | None = None,
) -> None:
    """Record one invoice_date → due_date observation and recompute the
    vendor's modal payment_terms_days and payment_terms_confidence."""
    session.execute(
        text("""
            INSERT INTO vendor_payment_terms_observations
                (vendor_id, entity_id, invoice_date, due_date, observed_at)
            VALUES (:vid, :eid, :idate, :ddate, NOW())
        """),
        {
            "vid": str(vendor_id),
            "eid": str(entity_id),
            "idate": invoice_date,
            "ddate": due_date,
        },
    )

    # Recompute modal terms from all observations for this vendor
    obs = session.execute(
        text("""
            SELECT terms_days, COUNT(*) AS n
            FROM vendor_payment_terms_observations
            WHERE vendor_id = :vid
            GROUP BY terms_days
            ORDER BY n DESC, terms_days
            LIMIT 1
        """),
        {"vid": str(vendor_id)},
    ).mappings().first()

    if not obs:
        return

    modal_terms = obs["terms_days"]
    total_obs = session.execute(
        text("SELECT COUNT(*) FROM vendor_payment_terms_observations WHERE vendor_id = :vid"),
        {"vid": str(vendor_id)},
    ).scalar() or 0

    # Consistency: proportion of observations that match the modal value
    modal_count = obs["n"]
    consistency = Decimal(str(modal_count)) / Decimal(str(total_obs)) if total_obs > 0 else Decimal("0")
    # Volume boost: approaches 1 after MIN_TERMS_OBS_FOR_CONFIDENCE observations
    volume_factor = min(Decimal(str(total_obs)) / Decimal(str(MIN_TERMS_OBS_FOR_CONFIDENCE)), Decimal("1"))
    confidence = (consistency * volume_factor).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)

    session.execute(
        text("""
            UPDATE vendors
               SET payment_terms_days       = :terms,
                   payment_terms_confidence = :conf,
                   updated_at               = NOW()
             WHERE id = :vid
        """),
        {"terms": modal_terms, "conf": confidence, "vid": str(vendor_id)},
    )


# ---------------------------------------------------------------------------
# Banking details capture
# ---------------------------------------------------------------------------

def set_vendor_banking(
    session,
    *,
    vendor_id: UUID,
    transit: str,
    institution: str,
    account: str,
    eft_transaction_type: str | None = None,
    actor_email: str | None = None,
) -> dict[str, Any]:
    """Store EFT banking details on a vendor record.
    Stamps banking_confirmed_at and banking_confirmed_by.
    Returns the updated vendor row.
    """
    session.execute(
        text("""
            UPDATE vendors
               SET bank_transit          = :transit,
                   bank_institution      = :institution,
                   bank_account          = :account,
                   eft_transaction_type  = COALESCE(:eft_type, eft_transaction_type),
                   banking_confirmed_at  = NOW(),
                   banking_confirmed_by  = :actor,
                   updated_at            = NOW()
             WHERE id = :vid
        """),
        {
            "transit": transit.strip(),
            "institution": institution.strip(),
            "account": account.strip(),
            "eft_type": eft_transaction_type,
            "actor": actor_email,
            "vid": str(vendor_id),
        },
    )
    row = session.execute(
        text("SELECT * FROM vendors WHERE id = :vid"),
        {"vid": str(vendor_id)},
    ).mappings().first()
    return dict(row)


# ---------------------------------------------------------------------------
# Remittance email update
# ---------------------------------------------------------------------------

def set_vendor_email(
    session,
    *,
    vendor_id: UUID,
    email: str | None,
) -> dict[str, Any]:
    """Update the remittance advice email for a vendor."""
    session.execute(
        text("UPDATE vendors SET remittance_email = :email, updated_at = NOW() WHERE id = :vid"),
        {"email": email, "vid": str(vendor_id)},
    )
    row = session.execute(
        text("SELECT * FROM vendors WHERE id = :vid"),
        {"vid": str(vendor_id)},
    ).mappings().first()
    return dict(row)


# ---------------------------------------------------------------------------
# Profile confidence (blended score for display)
# ---------------------------------------------------------------------------

def compute_profile_confidence(vendor_row: dict[str, Any]) -> Decimal:
    """Blend banking-present, email-present, and payment-terms confidence
    into a single 0–1 score surfaced in the AP dashboard.

    Weights:
      banking  0.50  (most critical for EFT)
      terms    0.30  (payment-terms confidence)
      email    0.20  (remittance advice)
    """
    banking = Decimal("1.0") if all([
        vendor_row.get("bank_transit"),
        vendor_row.get("bank_institution"),
        vendor_row.get("bank_account"),
    ]) else Decimal("0.0")
    email = Decimal("1.0") if vendor_row.get("remittance_email") else Decimal("0.0")
    terms = Decimal(str(vendor_row.get("payment_terms_confidence") or "0"))
    score = (banking * Decimal("0.50") + terms * Decimal("0.30") + email * Decimal("0.20"))
    return score.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

def get_vendor(session, *, entity_id: UUID, vendor_id: UUID) -> dict[str, Any] | None:
    row = session.execute(
        text("SELECT * FROM vendors WHERE id = :vid AND entity_id = :eid"),
        {"vid": str(vendor_id), "eid": str(entity_id)},
    ).mappings().first()
    return dict(row) if row else None


def list_vendors(session, *, entity_id: UUID) -> list[dict[str, Any]]:
    rows = session.execute(
        text("""
            SELECT v.*,
                   CASE WHEN v.bank_transit IS NOT NULL
                          AND v.bank_institution IS NOT NULL
                          AND v.bank_account IS NOT NULL
                        THEN TRUE ELSE FALSE END AS banking_complete
            FROM vendors v
            WHERE v.entity_id = :eid
            ORDER BY v.vendor_name
        """),
        {"eid": str(entity_id)},
    ).mappings().all()
    result = []
    for r in rows:
        d = dict(r)
        d["profile_confidence_computed"] = float(compute_profile_confidence(d))
        result.append(d)
    return result
