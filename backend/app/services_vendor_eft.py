"""
Vendor AP EFT payment file builder (CPA Standard 005).

Reuses the payroll EFT builder (services_payroll_eft.py) to generate
AFT credit files for outside-vendor payments. Vendor payments use
business-account credit transaction-type codes rather than payroll
personal-deposit codes.

Multi-tenancy: all functions scoped by entity_id UUID.
HH AP (account 2030) is explicitly excluded.
Dry-run only — never auto-submits to the bank.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text

_log = logging.getLogger(__name__)

# Fallback EFT transaction type for vendor (business) account credits.
# TD's code for business-account AFT credits. Confirm with TD before go-live.
# Parameterized as vendors.eft_transaction_type; this is the entity-level default.
VENDOR_EFT_TRANSACTION_TYPE_DEFAULT = "470"

def _resolve_eft_settings(session, entity_id: UUID) -> dict[str, str]:
    """Read TD origination values from entity_settings. Raises HTTPException(400)
    if any required field is missing — never silently falls back to another
    entity's originator ID. Mirrors routes/payroll.py:_resolve_eft_settings."""
    from fastapi import HTTPException
    row = session.execute(
        text("""
            SELECT td_originator_id, td_short_name, td_long_name,
                   td_return_institution, td_return_transit, td_return_account
            FROM entity_settings
            WHERE entity_id = :eid
        """),
        {"eid": str(entity_id)},
    ).mappings().first()
    row = dict(row) if row else {}
    _REQUIRED = [
        ("originator_id", "td_originator_id"),
        ("short_name",    "td_short_name"),
        ("long_name",     "td_long_name"),
        ("return_institution", "td_return_institution"),
        ("return_transit",     "td_return_transit"),
        ("return_account",     "td_return_account"),
    ]
    result: dict[str, str] = {}
    missing = []
    for key, col in _REQUIRED:
        val = row.get(col)
        if not val:
            missing.append(col)
        else:
            result[key] = val
    if missing:
        raise HTTPException(
            status_code=400,
            detail=(
                f"EFT originator not configured for entity {entity_id}; "
                f"missing entity_settings fields: {', '.join(missing)}. "
                "Seed entity_settings before generating EFT files."
            ),
        )
    return result


def _next_file_creation_number(session, entity_id: UUID) -> int:
    """Monotonically increasing per-entity file creation number."""
    result = session.execute(
        text("SELECT COALESCE(MAX(file_creation_number), 0) + 1 FROM vendor_eft_files WHERE entity_id = :eid"),
        {"eid": str(entity_id)},
    ).scalar()
    return int(result)


# ---------------------------------------------------------------------------
# Preview (dry-run totals, banking gap detection)
# ---------------------------------------------------------------------------

def preview_vendor_payment_file(
    session,
    *,
    entity_id: UUID,
    invoice_ids: list[str],
) -> dict[str, Any]:
    """Return a dry-run preview: totals, vendor count, and list of vendors
    missing banking details. The frontend uses this to prompt for banking
    entry before calling build_vendor_payment_file.

    Returns:
      vendors_in_batch: list of {vendor_id, vendor_name, amount, banking_complete}
      missing_banking: list of {vendor_id, vendor_name} — must enter banking
      total_amount: Decimal
      vendor_count: int
      invoice_count: int
    """
    if not invoice_ids:
        raise ValueError("invoice_ids must not be empty")

    # Load invoices (guard: entity-scoped, non-paid, non-void, non-HH-AP)
    rows = session.execute(
        text("""
            SELECT i.id, i.entity_id, i.vendor_name, i.vendor_id,
                   i.invoice_number, i.open_amount, i.status, i.payment_status
            FROM direct_vendor_ap_invoices i
            WHERE i.entity_id = :eid
              AND i.id = ANY(:iids::uuid[])
              AND i.active = TRUE
              AND i.status NOT IN ('paid', 'void')
        """),
        {"eid": str(entity_id), "iids": invoice_ids},
    ).mappings().all()

    if not rows:
        raise ValueError("No eligible invoices found for the given IDs")

    # Group by vendor
    vendor_totals: dict[str, dict] = {}
    for inv in rows:
        vid = str(inv["vendor_id"]) if inv["vendor_id"] else None
        vname = inv["vendor_name"]
        key = vid or vname
        if key not in vendor_totals:
            vendor_totals[key] = {
                "vendor_id": vid,
                "vendor_name": vname,
                "amount": Decimal("0"),
                "invoice_ids": [],
                "banking_complete": False,
            }
        vendor_totals[key]["amount"] += Decimal(str(inv["open_amount"] or 0))
        vendor_totals[key]["invoice_ids"].append(str(inv["id"]))

    # Check banking completeness
    missing_banking = []
    for key, vdata in vendor_totals.items():
        vid = vdata["vendor_id"]
        if vid:
            vrow = session.execute(
                text("SELECT bank_transit, bank_institution, bank_account FROM vendors WHERE id = :vid"),
                {"vid": vid},
            ).mappings().first()
            if vrow and vrow["bank_transit"] and vrow["bank_institution"] and vrow["bank_account"]:
                vdata["banking_complete"] = True
            else:
                missing_banking.append({"vendor_id": vid, "vendor_name": vdata["vendor_name"]})
        else:
            # No vendor master linked — banking definitely missing
            missing_banking.append({"vendor_id": None, "vendor_name": vdata["vendor_name"]})

    total_amount = sum(v["amount"] for v in vendor_totals.values())

    return {
        "vendors_in_batch": list(vendor_totals.values()),
        "missing_banking": missing_banking,
        "banking_complete": len(missing_banking) == 0,
        "total_amount": str(total_amount),
        "vendor_count": len(vendor_totals),
        "invoice_count": len(rows),
    }


# ---------------------------------------------------------------------------
# Build + archive
# ---------------------------------------------------------------------------

def build_vendor_payment_file(
    session,
    *,
    entity_id: UUID,
    invoice_ids: list[str],
    payment_date: date,
    actor_email: str | None = None,
) -> dict[str, Any]:
    """Build a CPA-005 EFT credit file for the selected invoices.

    Groups one C-record (AFT credit) per vendor per batch (sums invoices).
    Flips each invoice to payment_pending. Uploads to R2 (fail-tolerant).
    Inserts vendor_eft_files row. Optionally sends remittance advice emails.

    Returns:
      file_id, file_name, download_url (presigned), record_count,
      total_amount, vendor_count, dry_run=True
    """
    from . import services_payroll_eft as _eft
    from .services_storage import storage_service as _r2

    if not invoice_ids:
        raise ValueError("invoice_ids must not be empty")

    entity_id_str = str(entity_id)

    # Load invoices
    rows = session.execute(
        text("""
            SELECT i.id, i.entity_id, i.vendor_name, i.vendor_id,
                   i.invoice_number, i.open_amount, i.status, i.payment_status
            FROM direct_vendor_ap_invoices i
            WHERE i.entity_id = :eid
              AND i.id = ANY(:iids::uuid[])
              AND i.active = TRUE
              AND i.status NOT IN ('paid', 'void')
        """),
        {"eid": entity_id_str, "iids": invoice_ids},
    ).mappings().all()

    if not rows:
        raise ValueError("No eligible invoices found")

    # Group by vendor
    vendor_batches: dict[str, dict] = {}
    for inv in rows:
        vid = str(inv["vendor_id"]) if inv["vendor_id"] else None
        vname = inv["vendor_name"]
        key = vid or vname
        if key not in vendor_batches:
            vendor_batches[key] = {
                "vendor_id": vid,
                "vendor_name": vname,
                "amount": Decimal("0"),
                "invoice_ids": [],
            }
        vendor_batches[key]["amount"] += Decimal(str(inv["open_amount"] or 0))
        vendor_batches[key]["invoice_ids"].append(str(inv["id"]))

    # Validate banking for all vendors
    eft_employees = []
    for key, vdata in vendor_batches.items():
        vid = vdata["vendor_id"]
        if not vid:
            raise ValueError(
                f"Vendor '{vdata['vendor_name']}' has no vendor master record. "
                "Enter banking details first via the vendor profile."
            )
        vrow = session.execute(
            text("""
                SELECT vendor_name, bank_transit, bank_institution, bank_account,
                       eft_transaction_type, remittance_email
                FROM vendors WHERE id = :vid
            """),
            {"vid": vid},
        ).mappings().first()
        if not vrow:
            raise ValueError(f"Vendor record {vid} not found")
        missing = [
            f for f in ("bank_transit", "bank_institution", "bank_account")
            if not vrow.get(f)
        ]
        if missing:
            raise ValueError(
                f"Vendor '{vrow['vendor_name']}' is missing banking fields: {', '.join(missing)}. "
                "Enter banking details via the vendor profile."
            )
        txn_type = vrow["eft_transaction_type"] or VENDOR_EFT_TRANSACTION_TYPE_DEFAULT
        eft_employees.append(_eft.EFTEmployee(
            name=vdata["vendor_name"][:30],
            transit=vrow["bank_transit"],
            institution=vrow["bank_institution"],
            account=vrow["bank_account"],
            amount=vdata["amount"],
            transaction_type=txn_type,
        ))
        vdata["remittance_email"] = vrow.get("remittance_email")

    # Resolve EFT origination settings
    eft_settings = _resolve_eft_settings(session, entity_id)
    fcn = _next_file_creation_number(session, entity_id)

    header = _eft.EFTHeader(
        originator_id=eft_settings["originator_id"],
        file_creation_number=fcn,
        creation_date=payment_date,
        originator_short_name=eft_settings["short_name"],
        originator_long_name=eft_settings["long_name"],
        return_institution=eft_settings["return_institution"],
        return_transit=eft_settings["return_transit"],
        return_account=eft_settings["return_account"],
    )

    cross_reference = f"VNDPMT{payment_date.strftime('%Y%m%d')}{fcn:04d}"
    result = _eft.build_eft_file(
        header=header,
        employees=eft_employees,
        payment_date=payment_date,
        cross_reference=cross_reference,
    )

    file_bytes = result.text.encode("ascii")
    entity_code_row = session.execute(
        text("SELECT entity_code FROM entities WHERE id = :eid"),
        {"eid": entity_id_str},
    ).mappings().first()
    entity_code = entity_code_row["entity_code"] if entity_code_row else "unknown"
    filename = f"vendor_eft_{entity_code}_{payment_date.isoformat()}_fcn{fcn:04d}.txt"

    # Upload to R2 (fail-tolerant)
    r2_key = None
    try:
        r2_key = _r2.upload_file(
            file_bytes=file_bytes,
            original_filename=filename,
            entity_code=entity_code,
            document_type="vendor-eft",
            content_type="text/plain",
        )
    except Exception as exc:
        _log.error("vendor_eft: R2 upload failed: %r", exc)

    # Insert vendor_eft_files row
    file_row = session.execute(
        text("""
            INSERT INTO vendor_eft_files
                (entity_id, file_name, file_path, record_count, total_amount,
                 file_creation_number, payment_date, invoice_ids, vendor_count,
                 summary_json, actor_email, status, generated_at)
            VALUES
                (:eid, :fname, :fpath, :rcount, :total, :fcn, :pdate,
                 CAST(:iids AS jsonb), :vcount, CAST(:sj AS jsonb),
                 :actor, 'generated', NOW())
            RETURNING id
        """),
        {
            "eid": entity_id_str,
            "fname": filename,
            "fpath": r2_key,
            "rcount": result.record_count,
            "total": str(result.total_amount),
            "fcn": fcn,
            "pdate": payment_date,
            "iids": json.dumps(invoice_ids),
            "vcount": result.credit_count,
            "sj": json.dumps({
                "originator_id": eft_settings["originator_id"],
                "cross_reference": cross_reference,
                "actor": actor_email,
            }),
            "actor": actor_email,
        },
    ).mappings().first()
    file_id = str(file_row["id"])

    # Flip invoices to payment_pending + log events
    now_utc = datetime.now(timezone.utc)
    for inv_id_str in invoice_ids:
        session.execute(
            text("""
                UPDATE direct_vendor_ap_invoices
                   SET status              = 'payment_pending',
                       payment_file_id     = :fid,
                       payment_pending_at  = :now,
                       updated_at          = :now
                 WHERE id = :iid AND entity_id = :eid
                   AND status NOT IN ('paid', 'void')
            """),
            {
                "fid": file_id,
                "now": now_utc,
                "iid": inv_id_str,
                "eid": entity_id_str,
            },
        )
        session.execute(
            text("""
                INSERT INTO direct_vendor_ap_invoice_events
                    (entity_id, invoice_id, action, actor_email, from_status,
                     to_status, note, payload_json)
                VALUES (:eid, :iid, 'payment_file_generated', :actor,
                        'open', 'payment_pending',
                        :note, CAST(:payload AS jsonb))
            """),
            {
                "eid": entity_id_str,
                "iid": inv_id_str,
                "actor": actor_email,
                "note": f"CPA-005 file {filename}",
                "payload": json.dumps({"vendor_eft_file_id": file_id, "fcn": fcn}),
            },
        )

    # Send remittance advice emails (best-effort, per vendor)
    _send_remittance_advice(
        session,
        entity_id=entity_id,
        vendor_batches=vendor_batches,
        payment_date=payment_date,
        actor_email=actor_email,
    )

    # Presigned download URL
    download_url = None
    if r2_key:
        try:
            download_url = _r2.get_presigned_url(r2_key, expires_in=3600)
        except Exception as exc:
            _log.error("vendor_eft: presign failed: %r", exc)

    return {
        "file_id": file_id,
        "file_name": filename,
        "file_path": r2_key,
        "download_url": download_url,
        "record_count": result.record_count,
        "total_amount": float(result.total_amount),
        "vendor_count": result.credit_count,
        "invoice_count": len(invoice_ids),
        "file_creation_number": fcn,
        "dry_run": True,
        "submission_note": "EFT file generated for manual review. NOT submitted to the bank.",
    }


# ---------------------------------------------------------------------------
# Remittance advice email
# ---------------------------------------------------------------------------

def _send_remittance_advice(
    session,
    *,
    entity_id: UUID,
    vendor_batches: dict,
    payment_date: date,
    actor_email: str | None,
) -> None:
    """Send remittance advice email per vendor. Best-effort; never raises."""
    try:
        from .services_email import email_configured, send_email
        if not email_configured():
            return

        # Check entity remittance_advice_enabled
        entity = session.execute(
            text("SELECT notification_preferences FROM entities WHERE id = :eid"),
            {"eid": str(entity_id)},
        ).mappings().first()
        prefs = {}
        if entity and entity["notification_preferences"]:
            p = entity["notification_preferences"]
            if isinstance(p, str):
                import json
                p = json.loads(p)
            prefs = p.get("ap_alerts") or {}
        if not prefs.get("remittance_advice_enabled", True):
            return

        for key, vdata in vendor_batches.items():
            email_addr = vdata.get("remittance_email")
            if not email_addr:
                continue
            inv_list = ", ".join(vdata.get("invoice_ids", [])[:5])
            amount = float(vdata["amount"])
            html = (
                f"<p>Dear {vdata['vendor_name']},</p>"
                f"<p>This is a remittance advice for payment of <strong>${amount:,.2f} CAD</strong> "
                f"expected on <strong>{payment_date.isoformat()}</strong>.</p>"
                f"<p>Invoice(s): {inv_list}</p>"
                f"<p>If you have any questions, please contact us.</p>"
            )
            result = send_email(
                to=[email_addr],
                subject=f"Remittance Advice — Payment {payment_date.isoformat()} — ${amount:,.2f}",
                html=html,
            )
            _log.info("remittance advice: vendor=%r email=%r result=%r", vdata["vendor_name"], email_addr, result)
    except Exception as exc:
        _log.error("remittance advice send failed: %r", exc)
