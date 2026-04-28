import json
from datetime import date, datetime, time, timezone, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .quickbooks import (
    BANK_ACCOUNT_TYPES,
    QuickBooksClient,
    ensure_valid_access_token,
    upsert_connection,
)

BANK_REVIEW_STATUSES = {"new", "needs_review", "matched", "ignored"}
BANK_MATCH_STATUSES = {"active", "released"}


def _parse_uuid(value: str, field_name: str) -> UUID:
    try:
        return UUID(str(value))
    except Exception as exc:
        raise ValueError(f"Invalid {field_name}: {value}") from exc


def get_entity_by_code(session, entity_code: str):
    return session.execute(
        text(
            """
            SELECT id, entity_code, entity_name, quickbooks_company_id
            FROM entities
            WHERE entity_code = :entity_code
            """
        ),
        {"entity_code": entity_code},
    ).mappings().first()


def get_active_connection(session, entity_id: str):
    return session.execute(
        text(
            """
            SELECT id, entity_id, realm_id, access_token, refresh_token, access_token_expires_at,
                   refresh_token_expires_at, connected_at
            FROM quickbooks_connections
            WHERE entity_id = :entity_id AND is_active = TRUE
            ORDER BY connected_at DESC
            LIMIT 1
            """
        ),
        {"entity_id": entity_id},
    ).mappings().first()


def get_or_create_accounting_period(session, entity_id: str, txn_date: date | None):
    if txn_date is None:
        return None

    row = session.execute(
        text(
            """
            SELECT id
            FROM accounting_periods
            WHERE entity_id = :entity_id
              AND :txn_date BETWEEN period_start AND period_end
            ORDER BY period_start DESC
            LIMIT 1
            """
        ),
        {"entity_id": entity_id, "txn_date": txn_date},
    ).mappings().first()

    return row["id"] if row else None


async def import_chart_of_accounts(session, entity_code: str) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    connection = get_active_connection(session, entity["id"])
    if not connection:
        raise ValueError("QuickBooks connection not found for entity")

    connection = await ensure_valid_access_token(session, connection)

    qb = QuickBooksClient()
    accounts = await qb.query_all(
        realm_id=connection["realm_id"],
        access_token=connection["access_token"],
        base_query="SELECT * FROM Account",
        object_name="Account",
    )

    imported = 0
    bank_accounts = 0

    for acc in accounts:
        code = acc.get("AcctNum") or acc.get("Id")
        name = acc.get("Name") or "Unnamed"
        classification = acc.get("Classification") or "Unclassified"
        account_type = acc.get("AccountType") or classification
        statement_type = (
            "balance_sheet"
            if classification in {"Asset", "Liability", "Equity"}
            else "income_statement"
        )

        session.execute(
            text(
                """
                INSERT INTO accounts (
                    entity_id, account_code, account_name, account_class, statement_type, quickbooks_account_id
                )
                VALUES (
                    :entity_id, :account_code, :account_name, :account_class, :statement_type, :quickbooks_account_id
                )
                ON CONFLICT (entity_id, account_code)
                DO UPDATE SET
                    account_name = EXCLUDED.account_name,
                    account_class = EXCLUDED.account_class,
                    statement_type = EXCLUDED.statement_type,
                    quickbooks_account_id = EXCLUDED.quickbooks_account_id,
                    is_active = TRUE
                """
            ),
            {
                "entity_id": entity["id"],
                "account_code": str(code),
                "account_name": name,
                "account_class": account_type,
                "statement_type": statement_type,
                "quickbooks_account_id": str(acc.get("Id")),
            },
        )

        imported += 1
        if account_type in BANK_ACCOUNT_TYPES:
            bank_accounts += 1

    return {
        "imported_count": imported,
        "realm_id": connection["realm_id"],
        "bank_account_count": bank_accounts,
    }


async def import_transactions_cdc(session, entity_code: str, date_from, date_to) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    connection = get_active_connection(session, entity["id"])
    if not connection:
        raise ValueError("QuickBooks connection not found for entity")

    connection = await ensure_valid_access_token(session, connection)

    qb = QuickBooksClient()
    changed_since = datetime.combine(date_from, time.min, tzinfo=timezone.utc).isoformat()

    payload = await qb.cdc(
        realm_id=connection["realm_id"],
        access_token=connection["access_token"],
        changed_since_iso=changed_since,
        entities=[
            "JournalEntry",
            "Bill",
            "BillPayment",
            "Deposit",
            "Purchase",
            "SalesReceipt",
            "Invoice",
            "Payment",
        ],
    )

    imported = 0
    entity_nodes = payload.get("CDCResponse", [{}])[0].get("QueryResponse", [])

    for bucket in entity_nodes:
        for txn_type, records in bucket.items():
            if not isinstance(records, list):
                continue

            for record in records:
                txn_date = (
                    record.get("TxnDate")
                    or record.get("MetaData", {}).get("LastUpdatedTime", "")[:10]
                    or None
                )
                amount = record.get("TotalAmt") or record.get("HomeTotalAmt") or 0

                counterparty = None
                if isinstance(record.get("VendorRef"), dict):
                    counterparty = record["VendorRef"].get("name")
                elif isinstance(record.get("CustomerRef"), dict):
                    counterparty = record["CustomerRef"].get("name")

                memo = record.get("PrivateNote") or record.get("DocNumber") or txn_type

                session.execute(
                    text(
                        """
                        INSERT INTO quickbooks_transactions (
                            entity_id, quickbooks_txn_id, txn_type, txn_date, memo,
                            counterparty_name, amount, source_account_name
                        )
                        VALUES (
                            :entity_id, :quickbooks_txn_id, :txn_type, :txn_date, :memo,
                            :counterparty_name, :amount, :source_account_name
                        )
                        """
                    ),
                    {
                        "entity_id": entity["id"],
                        "quickbooks_txn_id": str(record.get("Id")),
                        "txn_type": txn_type,
                        "txn_date": txn_date,
                        "memo": memo,
                        "counterparty_name": counterparty,
                        "amount": amount,
                        "source_account_name": record.get("TxnStatus") or "imported_from_cdc",
                    },
                )

                imported += 1

    return {
        "imported_count": imported,
        "realm_id": connection["realm_id"],
        "changed_since": changed_since,
        "note": "CDC is a starter import for staging and review. It is not yet a full GL detail importer.",
    }


async def connect_company(session, entity_code: str, realm_id: str, code: str) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    qb = QuickBooksClient()
    token_payload = await qb.exchange_code(code)
    upsert_connection(session, entity["id"], realm_id, token_payload)

    company_info = await qb.get_company_info(realm_id, token_payload["access_token"])

    return {
        "realm_id": realm_id,
        "company_info": company_info.get("CompanyInfo", {}),
    }


def _safe_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _line_account_ref(line: dict[str, Any]) -> tuple[str | None, str | None]:
    detail_type = line.get("DetailType")
    if not detail_type:
        return (None, None)

    raw_detail = line.get(detail_type)
    detail = raw_detail if isinstance(raw_detail, dict) else {}

    account_ref = detail.get("AccountRef") if isinstance(detail.get("AccountRef"), dict) else None
    if not account_ref:
        return (None, None)

    value = str(account_ref.get("value")) if account_ref.get("value") is not None else None
    return (value, account_ref.get("name"))


def _txn_header_bank_ref(record: dict[str, Any]) -> tuple[str | None, str | None]:
    for key in ("AccountRef", "DepositToAccountRef", "CreditCardAccountRef", "ARAccountRef"):
        ref = record.get(key)
        if isinstance(ref, dict) and ref.get("value") is not None:
            return (str(ref.get("value")), ref.get("name"))
    return (None, None)


def _extract_counterparty_name(record: dict[str, Any]) -> str | None:
    for ref_key in ("VendorRef", "CustomerRef", "EntityRef"):
        ref = record.get(ref_key)
        if isinstance(ref, dict):
            name = ref.get("name")
            if name:
                return name
    return None


def _build_descriptions(record: dict[str, Any], txn_type: str) -> tuple[str, str, str | None, str | None]:
    doc_number = record.get("DocNumber")
    private_note = record.get("PrivateNote")
    counterparty_name = _extract_counterparty_name(record)
    payment_type = record.get("PaymentType")

    reference_number = doc_number

    raw_description = private_note or doc_number or counterparty_name or txn_type

    parts: list[str] = []
    if txn_type == "Purchase":
        parts.append("Purchase")
        if payment_type:
            parts.append(f"{payment_type}")
    else:
        parts.append(txn_type)

    if counterparty_name:
        parts.append(counterparty_name)

    if private_note and private_note != raw_description:
        parts.append(private_note)
    elif private_note:
        parts.append(private_note)

    if doc_number and doc_number not in parts:
        parts.append(f"Doc {doc_number}")

    normalized_description = " - ".join([part for part in parts if part]) or raw_description
    return raw_description, normalized_description, counterparty_name, reference_number


def _extract_bank_hit(
    record: dict[str, Any],
    txn_type: str,
    bank_account_ids: set[str],
) -> list[dict[str, Any]]:
    txn_id = str(record.get("Id") or "")
    if not txn_id:
        return []

    txn_date = (
        record.get("TxnDate")
        or record.get("MetaData", {}).get("LastUpdatedTime", "")[:10]
        or None
    )
    posted_date = txn_date

    currency_code = (
        ((record.get("CurrencyRef") or {}).get("value"))
        if isinstance(record.get("CurrencyRef"), dict)
        else None
    ) or "CAD"

    total_amt = _safe_decimal(record.get("TotalAmt") or record.get("HomeTotalAmt") or 0)
    raw_description, normalized_description, counterparty_name, reference_number = _build_descriptions(record, txn_type)

    hits: list[dict[str, Any]] = []

    header_account_id, header_account_name = _txn_header_bank_ref(record)
    if header_account_id and header_account_id in bank_account_ids:
        signed_amount = total_amt

        if txn_type in {"Deposit", "SalesReceipt", "Payment"}:
            direction = "inflow"
        elif txn_type in {"Purchase", "Check", "BillPayment", "CreditCardPayment"}:
            direction = "outflow"
            signed_amount = -abs(total_amt)
        elif txn_type == "Transfer":
            direction = "transfer"
        else:
            direction = "unknown"

        hits.append(
            {
                "source_transaction_id": f"{txn_type}:{txn_id}:{header_account_id}:header",
                "source_transaction_type": txn_type,
                "transaction_date": txn_date,
                "posted_date": posted_date,
                "description": raw_description,
                "normalized_description": normalized_description,
                "counterparty_name": counterparty_name,
                "reference_number": reference_number,
                "amount": signed_amount,
                "direction": direction,
                "source_account_id": header_account_id,
                "source_account_name": header_account_name,
                "currency_code": currency_code,
                "raw_json": record,
            }
        )
        return hits

    if txn_type != "JournalEntry":
        return hits

    lines = record.get("Line", [])
    for idx, line in enumerate(lines, start=1):
        account_id, account_name = _line_account_ref(line)
        if not account_id or account_id not in bank_account_ids:
            continue

        amount = _safe_decimal(line.get("Amount"))
        detail = (
            line.get("JournalEntryLineDetail")
            if isinstance(line.get("JournalEntryLineDetail"), dict)
            else {}
        )
        posting_type = detail.get("PostingType")

        signed_amount = amount if posting_type == "Debit" else -amount
        direction = "inflow" if signed_amount > 0 else "outflow"
        line_description = line.get("Description") or raw_description
        line_normalized = line_description if line_description != raw_description else normalized_description

        hits.append(
            {
                "source_transaction_id": f"{txn_type}:{txn_id}:{account_id}:line:{idx}",
                "source_transaction_type": txn_type,
                "transaction_date": txn_date,
                "posted_date": posted_date,
                "description": line_description,
                "normalized_description": line_normalized,
                "counterparty_name": counterparty_name,
                "reference_number": reference_number,
                "amount": signed_amount,
                "direction": direction,
                "source_account_id": account_id,
                "source_account_name": account_name,
                "currency_code": currency_code,
                "raw_json": {
                    "transaction": record,
                    "line": line,
                    "line_index": idx,
                },
            }
        )

    return hits


async def sync_qbo_bank_transactions(
    session,
    entity_code: str,
    date_from: date,
    date_to: date,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    connection = get_active_connection(session, entity["id"])
    if not connection:
        raise ValueError("QuickBooks connection not found for entity")

    connection = await ensure_valid_access_token(session, connection)

    bank_accounts = session.execute(
        text(
            """
            SELECT quickbooks_account_id, account_code, account_name, account_class
            FROM accounts
            WHERE entity_id = :entity_id
              AND quickbooks_account_id IS NOT NULL
              AND account_class IN ('Bank', 'CashOnHand', 'CreditCard', 'OtherCurrentAsset')
            """
        ),
        {"entity_id": entity["id"]},
    ).mappings().all()

    if not bank_accounts:
        raise ValueError("No QuickBooks bank-type accounts found. Run chart-of-accounts sync first.")

    bank_account_ids = {
        str(row["quickbooks_account_id"])
        for row in bank_accounts
        if row["quickbooks_account_id"]
    }
    account_name_map = {
        str(row["quickbooks_account_id"]): row["account_name"]
        for row in bank_accounts
        if row["quickbooks_account_id"]
    }
    account_code_map = {
        str(row["quickbooks_account_id"]): row["account_code"]
        for row in bank_accounts
        if row["quickbooks_account_id"]
    }

    qb = QuickBooksClient()
    query_from = date_from.isoformat()
    query_to = date_to.isoformat()

    objects = [
        "Purchase",
        "Deposit",
        "Transfer",
    ]

    inserted_count = 0
    updated_count = 0
    reviewed_candidates = 0
    per_type_counts: dict[str, int] = {}
    seen_ids: set[str] = set()
    object_errors: list[dict[str, Any]] = []

    for object_name in objects:
        base_query = (
            f"SELECT * FROM {object_name} "
            f"WHERE TxnDate >= '{query_from}' "
            f"AND TxnDate <= '{query_to}'"
        )

        try:
            rows = await qb.query_all(
                realm_id=connection["realm_id"],
                access_token=connection["access_token"],
                base_query=base_query,
                object_name=object_name,
            )
        except Exception as exc:
            object_errors.append(
                {
                    "object_name": object_name,
                    "query": base_query,
                    "error": str(exc),
                }
            )
            continue

        for row in rows:
            if object_name == "Purchase":
                account_ref = row.get("AccountRef") if isinstance(row.get("AccountRef"), dict) else {}
                account_id = str(account_ref.get("value")) if account_ref.get("value") is not None else None
                payment_type = row.get("PaymentType")

                if account_id not in bank_account_ids:
                    continue

                if payment_type not in {"Check"}:
                    continue

            hits = _extract_bank_hit(row, object_name, bank_account_ids)

            for hit in hits:
                source_transaction_id = hit["source_transaction_id"]
                if source_transaction_id in seen_ids:
                    continue

                seen_ids.add(source_transaction_id)

                txn_date = (
                    date.fromisoformat(hit["transaction_date"])
                    if hit["transaction_date"]
                    else None
                )
                posted_date = (
                    date.fromisoformat(hit["posted_date"])
                    if hit["posted_date"]
                    else None
                )
                accounting_period_id = get_or_create_accounting_period(session, entity["id"], txn_date)
                account_id = hit["source_account_id"]

                existing = session.execute(
                    text(
                        """
                        SELECT id
                        FROM bank_transactions
                        WHERE entity_id = :entity_id
                          AND source_system = 'quickbooks'
                          AND source_transaction_id = :source_transaction_id
                        LIMIT 1
                        """
                    ),
                    {
                        "entity_id": entity["id"],
                        "source_transaction_id": source_transaction_id,
                    },
                ).mappings().first()

                params = {
                    "entity_id": entity["id"],
                    "accounting_period_id": accounting_period_id,
                    "source_connection_id": connection["id"],
                    "source_account_id": account_id,
                    "source_account_name": (
                        account_name_map.get(account_id)
                        or hit["source_account_name"]
                        or "Unknown bank account"
                    ),
                    "source_account_code": account_code_map.get(account_id),
                    "source_transaction_id": source_transaction_id,
                    "source_transaction_type": hit["source_transaction_type"],
                    "transaction_date": txn_date,
                    "posted_date": posted_date,
                    "description": (hit.get("description") or "")[:500],
                    "normalized_description": (hit.get("normalized_description") or "")[:500],
                    "counterparty_name": hit.get("counterparty_name"),
                    "reference_number": hit.get("reference_number"),
                    "amount": hit["amount"],
                    "currency_code": hit.get("currency_code"),
                    "direction": hit["direction"],
                    "raw_json": json.dumps(hit.get("raw_json") or {}, default=str),
                }

                if existing:
                    session.execute(
                        text(
                            """
                            UPDATE bank_transactions
                            SET accounting_period_id = :accounting_period_id,
                                source_connection_id = :source_connection_id,
                                source_account_id = :source_account_id,
                                source_account_name = :source_account_name,
                                source_account_code = :source_account_code,
                                source_transaction_type = :source_transaction_type,
                                transaction_date = :transaction_date,
                                posted_date = :posted_date,
                                description = :description,
                                normalized_description = :normalized_description,
                                counterparty_name = :counterparty_name,
                                reference_number = :reference_number,
                                amount = :amount,
                                currency_code = :currency_code,
                                direction = :direction,
                                raw_json = CAST(:raw_json AS jsonb),
                                last_seen_at = NOW()
                            WHERE id = :id
                            """
                        ),
                        {**params, "id": existing["id"]},
                    )
                    updated_count += 1
                else:
                    session.execute(
                        text(
                            """
                            INSERT INTO bank_transactions (
                                entity_id, accounting_period_id, source_system, source_connection_id,
                                source_account_id, source_account_name, source_account_code,
                                source_transaction_id, source_transaction_type,
                                transaction_date, posted_date, description, normalized_description,
                                counterparty_name, reference_number,
                                amount, currency_code, direction, raw_json
                            )
                            VALUES (
                                :entity_id, :accounting_period_id, 'quickbooks', :source_connection_id,
                                :source_account_id, :source_account_name, :source_account_code,
                                :source_transaction_id, :source_transaction_type,
                                :transaction_date, :posted_date, :description, :normalized_description,
                                :counterparty_name, :reference_number,
                                :amount, :currency_code, :direction, CAST(:raw_json AS jsonb)
                            )
                            """
                        ),
                        params,
                    )
                    inserted_count += 1

                reviewed_candidates += 1
                txn_type = hit["source_transaction_type"]
                per_type_counts[txn_type] = per_type_counts.get(txn_type, 0) + 1

    summary_rows = session.execute(
        text(
            """
            SELECT source_account_name, review_status, COUNT(*) AS row_count, COALESCE(SUM(amount), 0) AS total_amount
            FROM bank_transactions
            WHERE entity_id = :entity_id
              AND transaction_date BETWEEN :date_from AND :date_to
              AND source_system = 'quickbooks'
            GROUP BY source_account_name, review_status
            ORDER BY source_account_name, review_status
            """
        ),
        {
            "entity_id": entity["id"],
            "date_from": date_from,
            "date_to": date_to,
        },
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "realm_id": connection["realm_id"],
        "date_from": query_from,
        "date_to": query_to,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "reviewed_candidates": reviewed_candidates,
        "per_transaction_type_counts": per_type_counts,
        "bank_account_count": len(bank_accounts),
        "summary_by_account_status": [dict(row) for row in summary_rows],
        "object_errors": object_errors,
        "note": "This sync imports QuickBooks-posted bank activity into the control layer. It does not yet pull the native bank-feed tab from QuickBooks.",
    }


def _get_transaction_with_match_summary(session, transaction_id: UUID):
    return session.execute(
        text(
            """
            WITH match_summary AS (
                SELECT bank_transaction_id,
                       COUNT(*) FILTER (WHERE active = TRUE) AS active_match_count,
                       COALESCE(SUM(matched_amount) FILTER (WHERE active = TRUE), 0) AS matched_amount
                FROM bank_transaction_matches
                GROUP BY bank_transaction_id
            )
            SELECT bt.id,
                   bt.entity_id,
                   bt.source_system,
                   bt.source_account_name,
                   bt.source_account_code,
                   bt.source_transaction_id,
                   bt.source_transaction_type,
                   bt.transaction_date,
                   bt.posted_date,
                   bt.description,
                   bt.normalized_description,
                   bt.counterparty_name,
                   bt.reference_number,
                   bt.amount,
                   bt.currency_code,
                   bt.direction,
                   bt.review_status,
                   bt.review_note,
                   bt.reviewed_by,
                   bt.last_reviewed_at,
                   bt.imported_at,
                   bt.last_seen_at,
                   bt.raw_json,
                   COALESCE(ms.active_match_count, 0) AS active_match_count,
                   COALESCE(ms.matched_amount, 0) AS matched_amount,
                   GREATEST(ABS(bt.amount) - COALESCE(ms.matched_amount, 0), 0) AS unmatched_amount,
                   (COALESCE(ms.active_match_count, 0) > 0) AS has_active_match
            FROM bank_transactions bt
            LEFT JOIN match_summary ms ON ms.bank_transaction_id = bt.id
            WHERE bt.id = :transaction_id
            """
        ),
        {"transaction_id": transaction_id},
    ).mappings().first()


def _derive_review_status_for_transaction(session, transaction_id: UUID) -> str:
    row = _get_transaction_with_match_summary(session, transaction_id)
    if not row:
        raise ValueError("Bank transaction not found")

    active_match_count = int(row["active_match_count"] or 0)
    matched_amount = Decimal(str(row["matched_amount"] or 0))
    txn_amount = abs(Decimal(str(row["amount"] or 0)))
    tolerance = Decimal("0.01")

    if active_match_count <= 0:
        return "new"
    if matched_amount + tolerance >= txn_amount:
        return "matched"
    return "needs_review"


def list_bank_transactions(
    session,
    entity_code: str,
    date_from: date,
    date_to: date,
    review_status: str | None = None,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    sql = """
        WITH match_summary AS (
            SELECT bank_transaction_id,
                   COUNT(*) FILTER (WHERE active = TRUE) AS active_match_count,
                   COALESCE(SUM(matched_amount) FILTER (WHERE active = TRUE), 0) AS matched_amount
            FROM bank_transaction_matches
            GROUP BY bank_transaction_id
        )
        SELECT bt.id,
               bt.source_system,
               bt.source_account_name,
               bt.source_account_code,
               bt.source_transaction_id,
               bt.source_transaction_type,
               bt.transaction_date,
               bt.posted_date,
               bt.description,
               bt.normalized_description,
               bt.counterparty_name,
               bt.reference_number,
               bt.amount,
               bt.currency_code,
               bt.direction,
               bt.review_status,
               bt.review_note,
               bt.reviewed_by,
               bt.last_reviewed_at,
               bt.imported_at,
               bt.last_seen_at,
               COALESCE(ms.active_match_count, 0) AS active_match_count,
               COALESCE(ms.matched_amount, 0) AS matched_amount,
               GREATEST(ABS(bt.amount) - COALESCE(ms.matched_amount, 0), 0) AS unmatched_amount,
               (COALESCE(ms.active_match_count, 0) > 0) AS has_active_match
        FROM bank_transactions bt
        LEFT JOIN match_summary ms ON ms.bank_transaction_id = bt.id
        WHERE bt.entity_id = :entity_id
          AND bt.transaction_date BETWEEN :date_from AND :date_to
    """

    params: dict[str, Any] = {
        "entity_id": entity["id"],
        "date_from": date_from,
        "date_to": date_to,
    }

    if review_status:
        sql += " AND bt.review_status = :review_status"
        params["review_status"] = review_status

    sql += " ORDER BY bt.transaction_date DESC, bt.imported_at DESC LIMIT 500"

    rows = session.execute(text(sql), params).mappings().all()

    status_counts = session.execute(
        text(
            """
            SELECT review_status, COUNT(*) AS row_count
            FROM bank_transactions
            WHERE entity_id = :entity_id
              AND transaction_date BETWEEN :date_from AND :date_to
            GROUP BY review_status
            ORDER BY review_status
            """
        ),
        {
            "entity_id": entity["id"],
            "date_from": date_from,
            "date_to": date_to,
        },
    ).mappings().all()

    type_counts = session.execute(
        text(
            """
            SELECT source_transaction_type, COUNT(*) AS row_count, COALESCE(SUM(amount), 0) AS total_amount
            FROM bank_transactions
            WHERE entity_id = :entity_id
              AND transaction_date BETWEEN :date_from AND :date_to
            GROUP BY source_transaction_type
            ORDER BY source_transaction_type
            """
        ),
        {
            "entity_id": entity["id"],
            "date_from": date_from,
            "date_to": date_to,
        },
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "review_status": review_status,
        "count": len(rows),
        "summary": {
            "counts_by_review_status": [dict(row) for row in status_counts],
            "counts_by_transaction_type": [dict(row) for row in type_counts],
        },
        "transactions": [dict(row) for row in rows],
    }


def get_bank_transaction_detail(session, entity_code: str, transaction_id: str) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    transaction_uuid = _parse_uuid(transaction_id, "transaction_id")
    transaction = _get_transaction_with_match_summary(session, transaction_uuid)
    if not transaction:
        raise ValueError("Bank transaction not found")
    if transaction["entity_id"] != entity["id"]:
        raise ValueError("Bank transaction does not belong to that entity")

    matches = session.execute(
        text(
            """
            SELECT id,
                   match_type,
                   target_table_name,
                   target_record_id,
                   target_label,
                   matched_amount,
                   active,
                   note,
                   payload_json,
                   created_by,
                   created_at,
                   released_by,
                   released_at
            FROM bank_transaction_matches
            WHERE bank_transaction_id = :transaction_id
            ORDER BY created_at DESC
            """
        ),
        {"transaction_id": transaction_uuid},
    ).mappings().all()

    review_history = session.execute(
        text(
            """
            SELECT id,
                   action,
                   from_review_status,
                   to_review_status,
                   actor_email,
                   note,
                   payload_json,
                   created_at
            FROM bank_transaction_review_events
            WHERE bank_transaction_id = :transaction_id
            ORDER BY created_at DESC
            """
        ),
        {"transaction_id": transaction_uuid},
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "transaction": dict(transaction),
        "matches": [dict(row) for row in matches],
        "review_history": [dict(row) for row in review_history],
    }


def set_bank_transaction_review_status(
    session,
    entity_code: str,
    transaction_id: str,
    review_status: str,
    actor_email: str,
    note: str | None = None,
) -> dict[str, Any]:
    if review_status not in BANK_REVIEW_STATUSES:
        raise ValueError(f"Invalid review_status: {review_status}")

    detail = get_bank_transaction_detail(session, entity_code, transaction_id)
    transaction = detail["transaction"]
    transaction_uuid = _parse_uuid(transaction_id, "transaction_id")

    previous_status = transaction["review_status"]

    session.execute(
        text(
            """
            UPDATE bank_transactions
            SET review_status = :review_status,
                review_note = :review_note,
                reviewed_by = :reviewed_by,
                last_reviewed_at = NOW()
            WHERE id = :transaction_id
            """
        ),
        {
            "transaction_id": transaction_uuid,
            "review_status": review_status,
            "review_note": note,
            "reviewed_by": actor_email,
        },
    )

    session.execute(
        text(
            """
            INSERT INTO bank_transaction_review_events (
                entity_id,
                bank_transaction_id,
                action,
                from_review_status,
                to_review_status,
                actor_email,
                note,
                payload_json
            )
            VALUES (
                :entity_id,
                :bank_transaction_id,
                :action,
                :from_review_status,
                :to_review_status,
                :actor_email,
                :note,
                CAST(:payload_json AS jsonb)
            )
            """
        ),
        {
            "entity_id": transaction["entity_id"],
            "bank_transaction_id": transaction_uuid,
            "action": "set_review_status",
            "from_review_status": previous_status,
            "to_review_status": review_status,
            "actor_email": actor_email,
            "note": note,
            "payload_json": json.dumps({}, default=str),
        },
    )

    return get_bank_transaction_detail(session, entity_code, transaction_id)


def create_bank_transaction_match(
    session,
    entity_code: str,
    transaction_id: str,
    match_type: str,
    target_table: str | None,
    target_record_id: str | None,
    target_label: str,
    amount_matched: Decimal | None,
    actor_email: str,
    note: str | None = None,
    payload_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not match_type:
        raise ValueError("match_type is required")
    if not target_label:
        raise ValueError("target_label is required")

    detail = get_bank_transaction_detail(session, entity_code, transaction_id)
    transaction = detail["transaction"]
    transaction_uuid = _parse_uuid(transaction_id, "transaction_id")
    transaction_amount = abs(Decimal(str(transaction["amount"] or 0)))

    amount_to_match = amount_matched if amount_matched is not None else transaction_amount
    amount_to_match = abs(Decimal(str(amount_to_match)))
    if amount_to_match <= 0:
        raise ValueError("amount_matched must be greater than zero")

    duplicate = session.execute(
        text(
            """
            SELECT id
            FROM bank_transaction_matches
            WHERE bank_transaction_id = :bank_transaction_id
              AND active = TRUE
              AND COALESCE(match_type, '') = COALESCE(:match_type, '')
              AND COALESCE(target_table_name, '') = COALESCE(:target_table, '')
              AND COALESCE(target_record_id, '') = COALESCE(:target_record_id, '')
              AND COALESCE(target_label, '') = COALESCE(:target_label, '')
            LIMIT 1
            """
        ),
        {
            "bank_transaction_id": transaction_uuid,
            "match_type": match_type,
            "target_table": target_table,
            "target_record_id": target_record_id,
            "target_label": target_label,
        },
    ).mappings().first()
    if duplicate:
        raise ValueError("An active match with the same target already exists for this bank transaction")

    session.execute(
        text(
            """
            INSERT INTO bank_transaction_matches (
                entity_id,
                bank_transaction_id,
                match_type,
                target_table_name,
                target_record_id,
                target_label,
                matched_amount,
                active,
                note,
                payload_json,
                created_by
            )
            VALUES (
                :entity_id,
                :bank_transaction_id,
                :match_type,
                :target_table,
                :target_record_id,
                :target_label,
                :amount_matched,
                TRUE,
                :note,
                CAST(:payload_json AS jsonb),
                :created_by
            )
            """
        ),
        {
            "entity_id": transaction["entity_id"],
            "bank_transaction_id": transaction_uuid,
            "match_type": match_type,
            "target_table": target_table,
            "target_record_id": target_record_id,
            "target_label": target_label,
            "amount_matched": amount_to_match,
            "note": note,
            "payload_json": json.dumps(payload_json or {}, default=str),
            "created_by": actor_email,
        },
    )

    previous_status = transaction["review_status"]
    new_status = _derive_review_status_for_transaction(session, transaction_uuid)

    session.execute(
        text(
            """
            UPDATE bank_transactions
            SET review_status = :review_status,
                reviewed_by = :reviewed_by,
                last_reviewed_at = NOW()
            WHERE id = :transaction_id
            """
        ),
        {
            "transaction_id": transaction_uuid,
            "review_status": new_status,
            "reviewed_by": actor_email,
        },
    )

    session.execute(
        text(
            """
            INSERT INTO bank_transaction_review_events (
                entity_id,
                bank_transaction_id,
                action,
                from_review_status,
                to_review_status,
                actor_email,
                note,
                payload_json
            )
            VALUES (
                :entity_id,
                :bank_transaction_id,
                :action,
                :from_review_status,
                :to_review_status,
                :actor_email,
                :note,
                CAST(:payload_json AS jsonb)
            )
            """
        ),
        {
            "entity_id": transaction["entity_id"],
            "bank_transaction_id": transaction_uuid,
            "action": "match_created",
            "from_review_status": previous_status,
            "to_review_status": new_status,
            "actor_email": actor_email,
            "note": note,
            "payload_json": json.dumps(
                {
                    "match_type": match_type,
                    "target_table": target_table,
                    "target_record_id": target_record_id,
                    "target_label": target_label,
                    "matched_amount": str(amount_to_match),
                },
                default=str,
            ),
        },
    )

    return get_bank_transaction_detail(session, entity_code, transaction_id)


def release_bank_transaction_match(
    session,
    entity_code: str,
    transaction_id: str,
    match_id: str,
    actor_email: str,
    note: str | None = None,
) -> dict[str, Any]:
    detail = get_bank_transaction_detail(session, entity_code, transaction_id)
    transaction = detail["transaction"]
    transaction_uuid = _parse_uuid(transaction_id, "transaction_id")
    match_uuid = _parse_uuid(match_id, "match_id")

    match_row = session.execute(
        text(
            """
            SELECT id, bank_transaction_id, active, match_type, target_table_name, target_record_id, target_label, matched_amount
            FROM bank_transaction_matches
            WHERE id = :match_id
            """
        ),
        {"match_id": match_uuid},
    ).mappings().first()

    if not match_row:
        raise ValueError("Bank transaction match not found")
    if match_row["bank_transaction_id"] != transaction_uuid:
        raise ValueError("That match does not belong to the supplied bank transaction")
    if not match_row["active"]:
        raise ValueError("That match is already released")

    session.execute(
        text(
            """
            UPDATE bank_transaction_matches
            SET active = FALSE,
                note = COALESCE(:note, note),
                released_by = :released_by,
                released_at = NOW()
            WHERE id = :match_id
            """
        ),
        {
            "match_id": match_uuid,
            "note": note,
            "released_by": actor_email,
        },
    )

    previous_status = transaction["review_status"]
    new_status = _derive_review_status_for_transaction(session, transaction_uuid)

    session.execute(
        text(
            """
            UPDATE bank_transactions
            SET review_status = :review_status,
                reviewed_by = :reviewed_by,
                last_reviewed_at = NOW()
            WHERE id = :transaction_id
            """
        ),
        {
            "transaction_id": transaction_uuid,
            "review_status": new_status,
            "reviewed_by": actor_email,
        },
    )

    session.execute(
        text(
            """
            INSERT INTO bank_transaction_review_events (
                entity_id,
                bank_transaction_id,
                action,
                from_review_status,
                to_review_status,
                actor_email,
                note,
                payload_json
            )
            VALUES (
                :entity_id,
                :bank_transaction_id,
                :action,
                :from_review_status,
                :to_review_status,
                :actor_email,
                :note,
                CAST(:payload_json AS jsonb)
            )
            """
        ),
        {
            "entity_id": transaction["entity_id"],
            "bank_transaction_id": transaction_uuid,
            "action": "match_released",
            "from_review_status": previous_status,
            "to_review_status": new_status,
            "actor_email": actor_email,
            "note": note,
            "payload_json": json.dumps(
                {
                    "released_match_id": str(match_uuid),
                    "match_type": match_row["match_type"],
                    "target_table": match_row["target_table_name"],
                    "target_record_id": match_row["target_record_id"],
                    "target_label": match_row["target_label"],
                    "matched_amount": str(match_row["matched_amount"]),
                },
                default=str,
            ),
        },
    )

    return get_bank_transaction_detail(session, entity_code, transaction_id)


REMITTANCE_BANK_MATCH_TYPE = "hh_ap_remittance"
REMITTANCE_BANK_TARGET_TABLE = "hh_ap_remittances"
REMITTANCE_BANK_MATCH_STATUSES = {"matched", "unmatched"}


def _build_hh_ap_remittance_target_label(remittance: dict[str, Any]) -> str:
    reference = remittance.get("remittance_reference") or str(remittance.get("id"))[:8]
    withdrawal_date = remittance.get("withdrawal_date") or remittance.get("remittance_date")
    label_parts = ["HH remittance", str(reference)]
    if withdrawal_date:
        label_parts.append(str(withdrawal_date))
    return " - ".join(label_parts)


def _get_hh_ap_remittance_row(session, remittance_uuid: UUID):
    return session.execute(
        text(
            """
            WITH line_summary AS (
                SELECT
                    remittance_id,
                    COUNT(*) AS line_count,
                    COALESCE(SUM(line_amount), 0) AS line_total_amount,
                    COUNT(*) FILTER (WHERE match_status = 'matched') AS matched_invoice_line_count,
                    COUNT(*) FILTER (WHERE match_status <> 'matched') AS unmatched_invoice_line_count
                FROM hh_ap_remittance_lines
                WHERE remittance_id = :remittance_id
                GROUP BY remittance_id
            ),
            active_bank_match AS (
                SELECT
                    m.id AS match_id,
                    m.target_record_id,
                    m.bank_transaction_id,
                    m.matched_amount,
                    m.created_at AS matched_at,
                    m.created_by AS matched_by,
                    m.note AS match_note,
                    bt.transaction_date AS bank_transaction_date,
                    bt.posted_date AS bank_posted_date,
                    bt.amount AS bank_transaction_amount,
                    bt.source_account_name AS bank_source_account_name,
                    bt.source_account_code AS bank_source_account_code,
                    bt.description AS bank_description,
                    bt.normalized_description AS bank_normalized_description,
                    bt.counterparty_name AS bank_counterparty_name,
                    bt.reference_number AS bank_reference_number,
                    bt.review_status AS bank_review_status
                FROM bank_transaction_matches m
                JOIN bank_transactions bt ON bt.id = m.bank_transaction_id
                WHERE m.active = TRUE
                  AND m.target_table_name = :target_table
                  AND m.target_record_id = :remittance_id_text
                LIMIT 1
            )
            SELECT
                r.id,
                r.entity_id,
                r.document_id,
                r.remittance_reference,
                r.remittance_date,
                r.withdrawal_date,
                r.total_amount,
                r.raw_json,
                COALESCE(ls.line_count, 0) AS line_count,
                COALESCE(ls.line_total_amount, 0) AS line_total_amount,
                COALESCE(ls.matched_invoice_line_count, 0) AS matched_invoice_line_count,
                COALESCE(ls.unmatched_invoice_line_count, 0) AS unmatched_invoice_line_count,
                CASE WHEN abm.match_id IS NULL THEN 'unmatched' ELSE 'matched' END AS bank_match_status,
                abm.match_id,
                abm.bank_transaction_id,
                abm.matched_amount AS bank_amount_matched,
                abm.matched_at AS bank_matched_at,
                abm.matched_by AS bank_matched_by,
                abm.match_note AS bank_match_note,
                abm.bank_transaction_date,
                abm.bank_posted_date,
                abm.bank_transaction_amount,
                abm.bank_source_account_name,
                abm.bank_source_account_code,
                abm.bank_description,
                abm.bank_normalized_description,
                abm.bank_counterparty_name,
                abm.bank_reference_number,
                abm.bank_review_status
            FROM hh_ap_remittances r
            LEFT JOIN line_summary ls ON ls.remittance_id = r.id
            LEFT JOIN active_bank_match abm ON TRUE
            WHERE r.id = :remittance_id
            """
        ),
        {
            "remittance_id": remittance_uuid,
            "remittance_id_text": str(remittance_uuid),
            "target_table": REMITTANCE_BANK_TARGET_TABLE,
        },
    ).mappings().first()


def _get_hh_ap_remittance_lines(session, remittance_uuid: UUID) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                id,
                invoice_number,
                line_description,
                due_date,
                line_amount,
                matched_invoice_id,
                match_status,
                raw_json
            FROM hh_ap_remittance_lines
            WHERE remittance_id = :remittance_id
            ORDER BY due_date NULLS LAST, invoice_number NULLS LAST, id
            """
        ),
        {"remittance_id": remittance_uuid},
    ).mappings().all()
    return [dict(row) for row in rows]


def _get_hh_ap_remittance_match_history(session, remittance_uuid: UUID) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                id,
                remittance_id,
                bank_transaction_id,
                bank_transaction_match_id,
                action,
                actor_email,
                note,
                payload_json,
                created_at
            FROM hh_ap_remittance_bank_match_events
            WHERE remittance_id = :remittance_id
            ORDER BY created_at DESC
            """
        ),
        {"remittance_id": remittance_uuid},
    ).mappings().all()
    return [dict(row) for row in rows]


def _insert_hh_ap_remittance_bank_event(
    session,
    entity_id: UUID,
    remittance_id: UUID,
    bank_transaction_id: UUID | None,
    bank_transaction_match_id: UUID | None,
    action: str,
    actor_email: str,
    note: str | None = None,
    payload_json: dict[str, Any] | None = None,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO hh_ap_remittance_bank_match_events (
                entity_id,
                remittance_id,
                bank_transaction_id,
                bank_transaction_match_id,
                action,
                actor_email,
                note,
                payload_json
            )
            VALUES (
                :entity_id,
                :remittance_id,
                :bank_transaction_id,
                :bank_transaction_match_id,
                :action,
                :actor_email,
                :note,
                CAST(:payload_json AS jsonb)
            )
            """
        ),
        {
            "entity_id": entity_id,
            "remittance_id": remittance_id,
            "bank_transaction_id": bank_transaction_id,
            "bank_transaction_match_id": bank_transaction_match_id,
            "action": action,
            "actor_email": actor_email,
            "note": note,
            "payload_json": json.dumps(payload_json or {}, default=str),
        },
    )


def _suggest_bank_transactions_for_remittance(
    session,
    entity_id: UUID,
    remittance: dict[str, Any],
    date_window_days: int = 7,
    amount_tolerance: Decimal = Decimal("0.05"),
    limit: int = 5,
) -> list[dict[str, Any]]:
    target_amount = abs(Decimal(str(remittance.get("total_amount") or remittance.get("line_total_amount") or 0)))
    target_date = remittance.get("withdrawal_date") or remittance.get("remittance_date")
    if target_amount <= 0 or not target_date:
        return []

    if isinstance(target_date, str):
        target_date = date.fromisoformat(target_date)

    date_start = target_date - timedelta(days=max(0, int(date_window_days)))
    date_end = target_date + timedelta(days=max(0, int(date_window_days)))

    rows = session.execute(
        text(
            """
            WITH active_matches AS (
                SELECT bank_transaction_id, COUNT(*) AS active_match_count
                FROM bank_transaction_matches
                WHERE active = TRUE
                GROUP BY bank_transaction_id
            )
            SELECT
                bt.id,
                bt.transaction_date,
                bt.posted_date,
                bt.amount,
                bt.source_account_name,
                bt.source_account_code,
                bt.description,
                bt.normalized_description,
                bt.counterparty_name,
                bt.reference_number,
                bt.review_status,
                COALESCE(am.active_match_count, 0) AS active_match_count
            FROM bank_transactions bt
            LEFT JOIN active_matches am ON am.bank_transaction_id = bt.id
            WHERE bt.entity_id = :entity_id
              AND bt.direction = 'outflow'
              AND COALESCE(bt.review_status, 'new') <> 'ignored'
              AND bt.transaction_date BETWEEN :date_start AND :date_end
            ORDER BY bt.transaction_date, bt.id
            LIMIT 100
            """
        ),
        {
            "entity_id": entity_id,
            "date_start": date_start,
            "date_end": date_end,
        },
    ).mappings().all()

    suggestions: list[dict[str, Any]] = []
    for row in rows:
        bank_amount = abs(Decimal(str(row["amount"] or 0)))
        amount_difference = abs(bank_amount - target_amount)
        txn_date = row["transaction_date"]
        if isinstance(txn_date, str):
            txn_date = date.fromisoformat(txn_date)
        date_difference_days = abs((txn_date - target_date).days) if txn_date else None
        has_active_match = int(row["active_match_count"] or 0) > 0
        is_amount_within_tolerance = amount_difference <= amount_tolerance

        if is_amount_within_tolerance and (date_difference_days is not None and date_difference_days <= 1) and not has_active_match:
            suggestion_strength = "exact"
        elif is_amount_within_tolerance and (date_difference_days is not None and date_difference_days <= 3) and not has_active_match:
            suggestion_strength = "strong"
        elif is_amount_within_tolerance and not has_active_match:
            suggestion_strength = "good"
        elif amount_difference <= Decimal("5.00") and (date_difference_days is not None and date_difference_days <= 3) and not has_active_match:
            suggestion_strength = "possible"
        else:
            suggestion_strength = "weak"

        score = (
            (1000 if has_active_match else 0)
            + (0 if is_amount_within_tolerance else 100)
            + int(amount_difference * 100)
            + (date_difference_days or 999)
        )

        suggestions.append(
            {
                "bank_transaction_id": str(row["id"]),
                "transaction_date": row["transaction_date"],
                "posted_date": row["posted_date"],
                "amount": row["amount"],
                "source_account_name": row["source_account_name"],
                "source_account_code": row["source_account_code"],
                "description": row["description"],
                "normalized_description": row["normalized_description"],
                "counterparty_name": row["counterparty_name"],
                "reference_number": row["reference_number"],
                "review_status": row["review_status"],
                "active_match_count": int(row["active_match_count"] or 0),
                "has_active_match": has_active_match,
                "bank_amount_abs": str(bank_amount),
                "target_amount": str(target_amount),
                "amount_difference": str(amount_difference),
                "date_difference_days": date_difference_days,
                "is_amount_within_tolerance": is_amount_within_tolerance,
                "suggestion_strength": suggestion_strength,
                "score": score,
            }
        )

    suggestions.sort(
        key=lambda item: (
            item["score"],
            item["date_difference_days"] if item["date_difference_days"] is not None else 999,
            item["bank_transaction_id"],
        )
    )
    return suggestions[:limit]


def list_hh_ap_remittances_for_bank_matching(
    session,
    entity_code: str,
    date_from: date,
    date_to: date,
    bank_match_status: str | None = None,
    suggestion_date_window_days: int = 7,
    amount_tolerance: Decimal = Decimal("0.05"),
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    if bank_match_status and bank_match_status not in REMITTANCE_BANK_MATCH_STATUSES:
        raise ValueError(f"Invalid bank_match_status: {bank_match_status}")

    rows = session.execute(
        text(
            """
            WITH line_summary AS (
                SELECT
                    remittance_id,
                    COUNT(*) AS line_count,
                    COALESCE(SUM(line_amount), 0) AS line_total_amount,
                    COUNT(*) FILTER (WHERE match_status = 'matched') AS matched_invoice_line_count,
                    COUNT(*) FILTER (WHERE match_status <> 'matched') AS unmatched_invoice_line_count
                FROM hh_ap_remittance_lines
                GROUP BY remittance_id
            ),
            active_bank_match AS (
                SELECT
                    m.id AS match_id,
                    m.target_record_id,
                    m.bank_transaction_id,
                    m.matched_amount,
                    m.created_at AS matched_at,
                    m.created_by AS matched_by,
                    m.note AS match_note,
                    bt.transaction_date AS bank_transaction_date,
                    bt.amount AS bank_transaction_amount,
                    bt.source_account_name AS bank_source_account_name,
                    bt.description AS bank_description,
                    bt.normalized_description AS bank_normalized_description,
                    bt.reference_number AS bank_reference_number,
                    bt.review_status AS bank_review_status
                FROM bank_transaction_matches m
                JOIN bank_transactions bt ON bt.id = m.bank_transaction_id
                WHERE m.active = TRUE
                  AND m.target_table_name = :target_table
            )
            SELECT
                r.id,
                r.entity_id,
                r.document_id,
                r.remittance_reference,
                r.remittance_date,
                r.withdrawal_date,
                r.total_amount,
                COALESCE(ls.line_count, 0) AS line_count,
                COALESCE(ls.line_total_amount, 0) AS line_total_amount,
                COALESCE(ls.matched_invoice_line_count, 0) AS matched_invoice_line_count,
                COALESCE(ls.unmatched_invoice_line_count, 0) AS unmatched_invoice_line_count,
                CASE WHEN abm.match_id IS NULL THEN 'unmatched' ELSE 'matched' END AS bank_match_status,
                abm.match_id,
                abm.bank_transaction_id,
                abm.matched_amount AS bank_amount_matched,
                abm.matched_at AS bank_matched_at,
                abm.matched_by AS bank_matched_by,
                abm.match_note AS bank_match_note,
                abm.bank_transaction_date,
                abm.bank_transaction_amount,
                abm.bank_source_account_name,
                abm.bank_description,
                abm.bank_normalized_description,
                abm.bank_reference_number,
                abm.bank_review_status
            FROM hh_ap_remittances r
            LEFT JOIN line_summary ls ON ls.remittance_id = r.id
            LEFT JOIN active_bank_match abm ON abm.target_record_id = r.id::text
            WHERE r.entity_id = :entity_id
              AND COALESCE(r.withdrawal_date, r.remittance_date) BETWEEN :date_from AND :date_to
            ORDER BY COALESCE(r.withdrawal_date, r.remittance_date), r.remittance_reference, r.id
            """
        ),
        {
            "entity_id": entity["id"],
            "date_from": date_from,
            "date_to": date_to,
            "target_table": REMITTANCE_BANK_TARGET_TABLE,
        },
    ).mappings().all()

    remittances: list[dict[str, Any]] = []
    exact_match_suggestion_count = 0
    auto_match_ready_count = 0

    for row in rows:
        item = dict(row)
        if bank_match_status and item["bank_match_status"] != bank_match_status:
            continue

        suggestions: list[dict[str, Any]] = []
        if item["bank_match_status"] == "unmatched":
            suggestions = _suggest_bank_transactions_for_remittance(
                session=session,
                entity_id=entity["id"],
                remittance=item,
                date_window_days=suggestion_date_window_days,
                amount_tolerance=amount_tolerance,
                limit=5,
            )

        open_exact_suggestions = [
            suggestion
            for suggestion in suggestions
            if suggestion["is_amount_within_tolerance"] and not suggestion["has_active_match"]
        ]
        if open_exact_suggestions:
            exact_match_suggestion_count += 1
        if len(open_exact_suggestions) == 1:
            auto_match_ready_count += 1

        item["top_bank_suggestion"] = suggestions[0] if suggestions else None
        item["suggestion_count"] = len(suggestions)
        item["open_exact_suggestion_count"] = len(open_exact_suggestions)
        item["can_auto_match"] = len(open_exact_suggestions) == 1
        remittances.append(item)

    total_amount = sum(Decimal(str(item.get("total_amount") or 0)) for item in remittances)
    matched_amount = sum(
        Decimal(str(item.get("total_amount") or 0))
        for item in remittances
        if item.get("bank_match_status") == "matched"
    )
    unmatched_amount = total_amount - matched_amount

    return {
        "entity_code": entity_code,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "bank_match_status": bank_match_status,
        "count": len(remittances),
        "summary": {
            "total_remittance_count": len(remittances),
            "matched_remittance_count": sum(1 for item in remittances if item.get("bank_match_status") == "matched"),
            "unmatched_remittance_count": sum(1 for item in remittances if item.get("bank_match_status") == "unmatched"),
            "total_remittance_amount": str(total_amount),
            "matched_remittance_amount": str(matched_amount),
            "unmatched_remittance_amount": str(unmatched_amount),
            "exact_match_suggestion_count": exact_match_suggestion_count,
            "auto_match_ready_count": auto_match_ready_count,
        },
        "remittances": remittances,
    }


def get_hh_ap_remittance_bank_match_detail(
    session,
    entity_code: str,
    remittance_id: str,
    suggestion_date_window_days: int = 7,
    amount_tolerance: Decimal = Decimal("0.05"),
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    remittance_uuid = _parse_uuid(remittance_id, "remittance_id")
    remittance = _get_hh_ap_remittance_row(session, remittance_uuid)
    if not remittance:
        raise ValueError("HH AP remittance not found")
    if remittance["entity_id"] != entity["id"]:
        raise ValueError("HH AP remittance does not belong to that entity")

    lines = _get_hh_ap_remittance_lines(session, remittance_uuid)
    match_history = _get_hh_ap_remittance_match_history(session, remittance_uuid)
    suggestions: list[dict[str, Any]] = []
    if remittance["bank_match_status"] == "unmatched":
        suggestions = _suggest_bank_transactions_for_remittance(
            session=session,
            entity_id=entity["id"],
            remittance=dict(remittance),
            date_window_days=suggestion_date_window_days,
            amount_tolerance=amount_tolerance,
            limit=10,
        )

    return {
        "entity_code": entity_code,
        "remittance": dict(remittance),
        "lines": lines,
        "suggestions": suggestions,
        "match_history": match_history,
    }


def create_hh_ap_remittance_bank_match(
    session,
    entity_code: str,
    remittance_id: str,
    bank_transaction_id: str,
    actor_email: str,
    amount_matched: Decimal | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    detail = get_hh_ap_remittance_bank_match_detail(session, entity_code, remittance_id)
    remittance = detail["remittance"]
    remittance_uuid = _parse_uuid(remittance_id, "remittance_id")

    if remittance["bank_match_status"] == "matched":
        raise ValueError("This remittance already has an active bank match")

    bank_detail = get_bank_transaction_detail(session, entity_code, bank_transaction_id)
    bank_transaction = bank_detail["transaction"]
    bank_transaction_uuid = _parse_uuid(bank_transaction_id, "bank_transaction_id")

    if bank_transaction.get("direction") != "outflow":
        raise ValueError("HH remittances can only be matched to outflow bank transactions")
    if bank_transaction.get("has_active_match"):
        raise ValueError("That bank transaction already has an active match")

    remittance_amount = abs(Decimal(str(remittance.get("total_amount") or 0)))
    bank_amount = abs(Decimal(str(bank_transaction.get("amount") or 0)))
    amount_to_match = abs(Decimal(str(amount_matched))) if amount_matched is not None else remittance_amount

    match_result = create_bank_transaction_match(
        session=session,
        entity_code=entity_code,
        transaction_id=bank_transaction_id,
        match_type=REMITTANCE_BANK_MATCH_TYPE,
        target_table=REMITTANCE_BANK_TARGET_TABLE,
        target_record_id=remittance_id,
        target_label=_build_hh_ap_remittance_target_label(remittance),
        amount_matched=amount_to_match,
        actor_email=actor_email,
        note=note,
        payload_json={
            "remittance_reference": remittance.get("remittance_reference"),
            "withdrawal_date": str(remittance.get("withdrawal_date") or ""),
            "remittance_total_amount": str(remittance_amount),
            "bank_transaction_amount": str(bank_amount),
            "amount_difference": str(abs(bank_amount - remittance_amount)),
        },
    )

    active_match = session.execute(
        text(
            """
            SELECT id
            FROM bank_transaction_matches
            WHERE bank_transaction_id = :bank_transaction_id
              AND active = TRUE
              AND target_table_name = :target_table
              AND target_record_id = :target_record_id
            ORDER BY created_at DESC
            LIMIT 1
            """
        ),
        {
            "bank_transaction_id": bank_transaction_uuid,
            "target_table": REMITTANCE_BANK_TARGET_TABLE,
            "target_record_id": remittance_id,
        },
    ).mappings().first()

    _insert_hh_ap_remittance_bank_event(
        session=session,
        entity_id=remittance["entity_id"],
        remittance_id=remittance_uuid,
        bank_transaction_id=bank_transaction_uuid,
        bank_transaction_match_id=active_match["id"] if active_match else None,
        action="match_created",
        actor_email=actor_email,
        note=note,
        payload_json={
            "match_summary": match_result,
        },
    )

    return get_hh_ap_remittance_bank_match_detail(session, entity_code, remittance_id)


def release_hh_ap_remittance_bank_match(
    session,
    entity_code: str,
    remittance_id: str,
    match_id: str,
    actor_email: str,
    note: str | None = None,
) -> dict[str, Any]:
    detail = get_hh_ap_remittance_bank_match_detail(session, entity_code, remittance_id)
    remittance = detail["remittance"]
    remittance_uuid = _parse_uuid(remittance_id, "remittance_id")
    match_uuid = _parse_uuid(match_id, "match_id")

    bank_transaction_id = remittance.get("bank_transaction_id")
    if not bank_transaction_id:
        raise ValueError("This remittance does not have an active bank match")

    release_bank_transaction_match(
        session=session,
        entity_code=entity_code,
        transaction_id=str(bank_transaction_id),
        match_id=match_id,
        actor_email=actor_email,
        note=note,
    )

    _insert_hh_ap_remittance_bank_event(
        session=session,
        entity_id=remittance["entity_id"],
        remittance_id=remittance_uuid,
        bank_transaction_id=_parse_uuid(str(bank_transaction_id), "bank_transaction_id"),
        bank_transaction_match_id=match_uuid,
        action="match_released",
        actor_email=actor_email,
        note=note,
        payload_json={},
    )

    return get_hh_ap_remittance_bank_match_detail(session, entity_code, remittance_id)


def auto_match_hh_ap_remittances_to_bank(
    session,
    entity_code: str,
    date_from: date,
    date_to: date,
    actor_email: str,
    date_window_days: int = 5,
    amount_tolerance: Decimal = Decimal("0.05"),
    max_to_apply: int = 100,
    note: str | None = None,
) -> dict[str, Any]:
    review = list_hh_ap_remittances_for_bank_matching(
        session=session,
        entity_code=entity_code,
        date_from=date_from,
        date_to=date_to,
        bank_match_status="unmatched",
        suggestion_date_window_days=date_window_days,
        amount_tolerance=amount_tolerance,
    )

    matched: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for remittance in review["remittances"]:
        if len(matched) >= max_to_apply:
            skipped.append(
                {
                    "remittance_id": str(remittance["id"]),
                    "reason": "max_to_apply_reached",
                }
            )
            continue

        suggestions = _suggest_bank_transactions_for_remittance(
            session=session,
            entity_id=_parse_uuid(str(remittance["entity_id"]), "entity_id"),
            remittance=remittance,
            date_window_days=date_window_days,
            amount_tolerance=amount_tolerance,
            limit=10,
        )
        exact_open = [
            suggestion
            for suggestion in suggestions
            if suggestion["is_amount_within_tolerance"] and not suggestion["has_active_match"]
        ]

        if len(exact_open) != 1:
            skipped.append(
                {
                    "remittance_id": str(remittance["id"]),
                    "remittance_reference": remittance.get("remittance_reference"),
                    "reason": "no_unique_exact_candidate",
                    "exact_candidate_count": len(exact_open),
                }
            )
            continue

        selected = exact_open[0]
        match_note = note or "Auto-matched HH remittance to bank transaction"
        detail_result = create_hh_ap_remittance_bank_match(
            session=session,
            entity_code=entity_code,
            remittance_id=str(remittance["id"]),
            bank_transaction_id=selected["bank_transaction_id"],
            actor_email=actor_email,
            amount_matched=abs(Decimal(str(remittance.get("total_amount") or 0))),
            note=match_note,
        )

        active_match = detail_result["remittance"].get("match_id")
        _insert_hh_ap_remittance_bank_event(
            session=session,
            entity_id=_parse_uuid(str(remittance["entity_id"]), "entity_id"),
            remittance_id=_parse_uuid(str(remittance["id"]), "remittance_id"),
            bank_transaction_id=_parse_uuid(selected["bank_transaction_id"], "bank_transaction_id"),
            bank_transaction_match_id=_parse_uuid(str(active_match), "match_id") if active_match else None,
            action="auto_matched",
            actor_email=actor_email,
            note=match_note,
            payload_json={
                "selected_suggestion": selected,
            },
        )

        matched.append(
            {
                "remittance_id": str(remittance["id"]),
                "remittance_reference": remittance.get("remittance_reference"),
                "bank_transaction_id": selected["bank_transaction_id"],
                "bank_transaction_amount": selected["amount"],
                "transaction_date": selected["transaction_date"],
            }
        )

    return {
        "entity_code": entity_code,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "matched_count": len(matched),
        "skipped_count": len(skipped),
        "matched": matched,
        "skipped": skipped,
        "date_window_days": date_window_days,
        "amount_tolerance": str(amount_tolerance),
        "max_to_apply": max_to_apply,
    }


DIRECT_VENDOR_INVOICE_STATUSES = {"open", "needs_review", "approved", "scheduled", "paid", "void"}
DIRECT_VENDOR_PAYMENT_STATUSES = {"unpaid", "partially_paid", "paid"}
DIRECT_VENDOR_PRIORITIES = {"low", "normal", "high", "urgent"}
DIRECT_VENDOR_MATCH_TYPE = "direct_vendor_ap_invoice"
DIRECT_VENDOR_TARGET_TABLE = "direct_vendor_ap_invoices"

CARD_SETTLEMENT_RECON_STATUSES = {"new", "needs_review", "reconciled", "ignored"}
CARD_SETTLEMENT_MATCH_TYPE = "card_settlement_batch"
CARD_SETTLEMENT_TARGET_TABLE = "card_settlement_batches"


def _today_utc_date() -> date:
    return datetime.now(timezone.utc).date()


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _normalize_match_state(active_match_count: int | None) -> str:
    return "matched" if int(active_match_count or 0) > 0 else "unmatched"


def _compute_due_state(due_date: date | None, payment_status: str, as_of_date: date | None = None) -> str:
    if payment_status == "paid":
        return "paid"
    if due_date is None:
        return "no_due_date"
    as_of = as_of_date or _today_utc_date()
    if due_date < as_of:
        return "overdue"
    if due_date == as_of:
        return "due_today"
    if due_date <= as_of + timedelta(days=7):
        return "due_soon"
    return "upcoming"


def _vendor_name_match_score(vendor_name: str | None, candidate_text: str | None) -> int:
    vendor = (_safe_text(vendor_name) or "").lower()
    candidate = (_safe_text(candidate_text) or "").lower()
    if not vendor or not candidate:
        return 0
    if vendor == candidate:
        return 4
    if vendor in candidate:
        return 3
    vendor_words = [word for word in vendor.replace("&", " ").replace("/", " ").split() if len(word) >= 4]
    overlaps = sum(1 for word in vendor_words if word in candidate)
    if overlaps >= 2:
        return 2
    if overlaps == 1:
        return 1
    return 0


def _has_table(session, table_name: str) -> bool:
    row = session.execute(
        text("SELECT to_regclass(:table_name) AS table_name"),
        {"table_name": f"public.{table_name}"},
    ).mappings().first()
    return bool(row and row["table_name"])


def _build_direct_vendor_target_label(invoice_row: dict[str, Any]) -> str:
    parts = ["Direct vendor invoice"]
    vendor_name = _safe_text(invoice_row.get("vendor_name"))
    invoice_number = _safe_text(invoice_row.get("invoice_number"))
    if vendor_name:
        parts.append(vendor_name)
    if invoice_number:
        parts.append(invoice_number)
    return " - ".join(parts)


def _insert_direct_vendor_invoice_event(
    session,
    entity_id: UUID,
    invoice_id: UUID,
    action: str,
    actor_email: str,
    from_status: str | None = None,
    to_status: str | None = None,
    note: str | None = None,
    payload_json: dict[str, Any] | None = None,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO direct_vendor_ap_invoice_events (
                entity_id,
                invoice_id,
                action,
                actor_email,
                from_status,
                to_status,
                note,
                payload_json
            )
            VALUES (
                :entity_id,
                :invoice_id,
                :action,
                :actor_email,
                :from_status,
                :to_status,
                :note,
                CAST(:payload_json AS jsonb)
            )
            """
        ),
        {
            "entity_id": entity_id,
            "invoice_id": invoice_id,
            "action": action,
            "actor_email": actor_email,
            "from_status": from_status,
            "to_status": to_status,
            "note": note,
            "payload_json": json.dumps(payload_json or {}, default=str),
        },
    )


def _get_direct_vendor_invoice_matches(session, invoice_uuid: UUID) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                m.id,
                m.bank_transaction_id,
                m.match_type,
                m.target_table_name AS target_table,
                m.target_record_id,
                m.target_label,
                m.matched_amount,
                m.active,
                m.note,
                m.payload_json,
                m.created_by,
                m.created_at,
                m.released_by,
                m.released_at,
                bt.transaction_date AS bank_transaction_date,
                bt.posted_date AS bank_posted_date,
                bt.amount AS bank_transaction_amount,
                bt.source_account_name AS bank_source_account_name,
                bt.source_account_code AS bank_source_account_code,
                bt.description AS bank_description,
                bt.normalized_description AS bank_normalized_description,
                bt.counterparty_name AS bank_counterparty_name,
                bt.reference_number AS bank_reference_number,
                bt.review_status AS bank_review_status
            FROM bank_transaction_matches m
            LEFT JOIN bank_transactions bt ON bt.id = m.bank_transaction_id
            WHERE m.target_table_name = :target_table
              AND m.target_record_id = :target_record_id
            ORDER BY m.created_at DESC
            """
        ),
        {
            "target_table": DIRECT_VENDOR_TARGET_TABLE,
            "target_record_id": str(invoice_uuid),
        },
    ).mappings().all()
    return [dict(row) for row in rows]


def _get_direct_vendor_invoice_history(session, invoice_uuid: UUID) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                id,
                invoice_id,
                action,
                actor_email,
                from_status,
                to_status,
                note,
                payload_json,
                created_at
            FROM direct_vendor_ap_invoice_events
            WHERE invoice_id = :invoice_id
            ORDER BY created_at DESC
            """
        ),
        {"invoice_id": invoice_uuid},
    ).mappings().all()
    return [dict(row) for row in rows]


def _hydrate_direct_vendor_invoice_row(row: dict[str, Any]) -> dict[str, Any]:
    hydrated = dict(row)
    total_amount_abs = abs(Decimal(str(row.get("total_amount") or 0)))
    matched_amount = abs(Decimal(str(row.get("active_matched_amount") or 0)))
    open_amount = total_amount_abs - matched_amount
    if open_amount < Decimal("0.00"):
        open_amount = Decimal("0.00")

    if matched_amount == Decimal("0.00"):
        derived_payment_status = "unpaid"
    elif open_amount <= Decimal("0.05"):
        derived_payment_status = "paid"
    else:
        derived_payment_status = "partially_paid"

    due_date_value = row.get("due_date")
    if isinstance(due_date_value, str):
        due_date_value = date.fromisoformat(due_date_value)

    hydrated["active_matched_amount"] = matched_amount
    hydrated["open_amount"] = open_amount
    hydrated["derived_payment_status"] = derived_payment_status
    hydrated["match_state"] = _normalize_match_state(row.get("active_match_count"))
    hydrated["due_state"] = _compute_due_state(due_date_value, derived_payment_status)
    return hydrated


def _get_direct_vendor_invoice_row(session, invoice_uuid: UUID):
    row = session.execute(
        text(
            """
            WITH active_matches AS (
                SELECT
                    m.target_record_id,
                    COUNT(*) AS active_match_count,
                    COALESCE(SUM(m.matched_amount), 0) AS active_matched_amount,
                    MAX(bt.transaction_date) AS last_payment_date
                FROM bank_transaction_matches m
                LEFT JOIN bank_transactions bt ON bt.id = m.bank_transaction_id
                WHERE m.active = TRUE
                  AND m.target_table_name = :target_table
                GROUP BY m.target_record_id
            )
            SELECT
                i.*,
                COALESCE(am.active_match_count, 0) AS active_match_count,
                COALESCE(am.active_matched_amount, 0) AS active_matched_amount,
                am.last_payment_date
            FROM direct_vendor_ap_invoices i
            LEFT JOIN active_matches am ON am.target_record_id = i.id::text
            WHERE i.id = :invoice_id
              AND i.active = TRUE
            """
        ),
        {
            "invoice_id": invoice_uuid,
            "target_table": DIRECT_VENDOR_TARGET_TABLE,
        },
    ).mappings().first()
    return _hydrate_direct_vendor_invoice_row(row) if row else None


def _recalculate_direct_vendor_invoice_payment_fields(session, invoice_uuid: UUID) -> None:
    invoice_row = _get_direct_vendor_invoice_row(session, invoice_uuid)
    if not invoice_row:
        return

    derived_payment_status = invoice_row["derived_payment_status"]
    current_status = invoice_row.get("status")
    next_status = current_status
    if current_status != "void":
        if derived_payment_status == "paid":
            next_status = "paid"
        elif current_status == "paid" and derived_payment_status != "paid":
            next_status = "needs_review"

    session.execute(
        text(
            """
            UPDATE direct_vendor_ap_invoices
            SET paid_amount = :paid_amount,
                open_amount = :open_amount,
                payment_status = :payment_status,
                last_payment_date = :last_payment_date,
                status = :status,
                updated_at = NOW()
            WHERE id = :invoice_id
            """
        ),
        {
            "invoice_id": invoice_uuid,
            "paid_amount": invoice_row["active_matched_amount"],
            "open_amount": invoice_row["open_amount"],
            "payment_status": derived_payment_status,
            "last_payment_date": invoice_row.get("last_payment_date"),
            "status": next_status,
        },
    )


def _suggest_bank_transactions_for_direct_vendor_invoice(
    session,
    entity_id: UUID,
    invoice_row: dict[str, Any],
    date_window_days: int = 14,
    amount_tolerance: Decimal = Decimal("0.05"),
    limit: int = 10,
) -> list[dict[str, Any]]:
    target_amount = abs(Decimal(str(invoice_row.get("total_amount") or 0)))
    if target_amount <= Decimal("0.00"):
        return []

    anchor_date = invoice_row.get("due_date") or invoice_row.get("invoice_date")
    if isinstance(anchor_date, str):
        anchor_date = date.fromisoformat(anchor_date)
    if not anchor_date:
        return []

    date_start = anchor_date - timedelta(days=max(0, int(date_window_days)))
    date_end = anchor_date + timedelta(days=max(0, int(date_window_days)))
    rows = session.execute(
        text(
            """
            WITH active_matches AS (
                SELECT bank_transaction_id, COUNT(*) AS active_match_count
                FROM bank_transaction_matches
                WHERE active = TRUE
                GROUP BY bank_transaction_id
            )
            SELECT
                bt.id,
                bt.transaction_date,
                bt.posted_date,
                bt.amount,
                bt.source_account_name,
                bt.source_account_code,
                bt.description,
                bt.normalized_description,
                bt.counterparty_name,
                bt.reference_number,
                bt.review_status,
                COALESCE(am.active_match_count, 0) AS active_match_count
            FROM bank_transactions bt
            LEFT JOIN active_matches am ON am.bank_transaction_id = bt.id
            WHERE bt.entity_id = :entity_id
              AND bt.direction = 'outflow'
              AND COALESCE(bt.review_status, 'new') <> 'ignored'
              AND bt.transaction_date BETWEEN :date_start AND :date_end
            ORDER BY bt.transaction_date, bt.id
            """
        ),
        {
            "entity_id": entity_id,
            "date_start": date_start,
            "date_end": date_end,
        },
    ).mappings().all()

    vendor_name = invoice_row.get("vendor_name")
    suggestions: list[dict[str, Any]] = []
    for row in rows:
        bank_amount_abs = abs(Decimal(str(row["amount"] or 0)))
        amount_diff = abs(bank_amount_abs - target_amount)
        vendor_score = max(
            _vendor_name_match_score(vendor_name, row.get("counterparty_name")),
            _vendor_name_match_score(vendor_name, row.get("normalized_description")),
            _vendor_name_match_score(vendor_name, row.get("description")),
        )
        threshold = max(amount_tolerance, target_amount * Decimal("0.15"))
        if amount_diff > threshold and vendor_score == 0:
            continue

        transaction_date = row.get("transaction_date")
        if isinstance(transaction_date, str):
            transaction_date = date.fromisoformat(transaction_date)
        date_diff_days = abs((transaction_date - anchor_date).days) if transaction_date else None

        suggestion = dict(row)
        suggestion["amount_diff"] = amount_diff
        suggestion["date_diff_days"] = date_diff_days
        suggestion["vendor_name_match_score"] = vendor_score
        suggestion["suggestion_score"] = (
            (100 - min(99, int(amount_diff * 100)))
            + (vendor_score * 25)
            + (0 if date_diff_days is None else max(0, 14 - min(date_diff_days, 14)))
        )
        suggestions.append(suggestion)

    suggestions.sort(
        key=lambda row: (
            -row["suggestion_score"],
            row["amount_diff"],
            row["date_diff_days"] if row["date_diff_days"] is not None else 999,
            row["id"],
        )
    )
    return suggestions[:limit]


def list_direct_vendor_ap_invoices(
    session,
    entity_code: str,
    date_from: date,
    date_to: date,
    status: str | None = None,
    payment_status: str | None = None,
    due_state: str | None = None,
    match_state: str | None = None,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    rows = session.execute(
        text(
            """
            WITH active_matches AS (
                SELECT
                    m.target_record_id,
                    COUNT(*) AS active_match_count,
                    COALESCE(SUM(m.matched_amount), 0) AS active_matched_amount,
                    MAX(bt.transaction_date) AS last_payment_date
                FROM bank_transaction_matches m
                LEFT JOIN bank_transactions bt ON bt.id = m.bank_transaction_id
                WHERE m.active = TRUE
                  AND m.target_table_name = :target_table
                GROUP BY m.target_record_id
            )
            SELECT
                i.*,
                COALESCE(am.active_match_count, 0) AS active_match_count,
                COALESCE(am.active_matched_amount, 0) AS active_matched_amount,
                am.last_payment_date
            FROM direct_vendor_ap_invoices i
            LEFT JOIN active_matches am ON am.target_record_id = i.id::text
            WHERE i.entity_id = :entity_id
              AND i.active = TRUE
              AND i.invoice_date BETWEEN :date_from AND :date_to
            ORDER BY COALESCE(i.due_date, i.invoice_date), i.vendor_name, i.invoice_number
            """
        ),
        {
            "entity_id": entity["id"],
            "date_from": date_from,
            "date_to": date_to,
            "target_table": DIRECT_VENDOR_TARGET_TABLE,
        },
    ).mappings().all()

    hydrated_rows = [_hydrate_direct_vendor_invoice_row(dict(row)) for row in rows]
    filtered_rows: list[dict[str, Any]] = []
    for row in hydrated_rows:
        if status and row.get("status") != status:
            continue
        if payment_status and row.get("derived_payment_status") != payment_status:
            continue
        if due_state and row.get("due_state") != due_state:
            continue
        if match_state and row.get("match_state") != match_state:
            continue
        filtered_rows.append(row)

    summary = {
        "total_count": len(filtered_rows),
        "total_amount": sum(Decimal(str(row.get("total_amount") or 0)) for row in filtered_rows),
        "unpaid_count": sum(1 for row in filtered_rows if row["derived_payment_status"] == "unpaid"),
        "partially_paid_count": sum(1 for row in filtered_rows if row["derived_payment_status"] == "partially_paid"),
        "paid_count": sum(1 for row in filtered_rows if row["derived_payment_status"] == "paid"),
        "overdue_count": sum(1 for row in filtered_rows if row["due_state"] == "overdue"),
        "due_today_count": sum(1 for row in filtered_rows if row["due_state"] == "due_today"),
        "due_soon_count": sum(1 for row in filtered_rows if row["due_state"] == "due_soon"),
        "matched_count": sum(1 for row in filtered_rows if row["match_state"] == "matched"),
        "unmatched_count": sum(1 for row in filtered_rows if row["match_state"] == "unmatched"),
        "open_amount_total": sum(Decimal(str(row.get("open_amount") or 0)) for row in filtered_rows),
    }

    by_status: dict[str, dict[str, Any]] = {}
    by_due_state: dict[str, dict[str, Any]] = {}
    for row in filtered_rows:
        status_key = row.get("status") or "unknown"
        by_status.setdefault(status_key, {"status": status_key, "row_count": 0, "total_amount": Decimal("0.00")})
        by_status[status_key]["row_count"] += 1
        by_status[status_key]["total_amount"] += Decimal(str(row.get("total_amount") or 0))

        due_key = row.get("due_state") or "unknown"
        by_due_state.setdefault(due_key, {"due_state": due_key, "row_count": 0, "open_amount": Decimal("0.00")})
        by_due_state[due_key]["row_count"] += 1
        by_due_state[due_key]["open_amount"] += Decimal(str(row.get("open_amount") or 0))

    summary["by_status"] = list(by_status.values())
    summary["by_due_state"] = list(by_due_state.values())

    return {
        "entity_code": entity_code,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "status": status,
        "payment_status": payment_status,
        "due_state": due_state,
        "match_state": match_state,
        "count": len(filtered_rows),
        "summary": summary,
        "invoices": filtered_rows,
    }


def get_direct_vendor_ap_invoice_detail(
    session,
    entity_code: str,
    invoice_id: str,
    suggestion_date_window_days: int = 14,
    amount_tolerance: Decimal = Decimal("0.05"),
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    invoice_uuid = _parse_uuid(invoice_id, "invoice_id")
    invoice_row = _get_direct_vendor_invoice_row(session, invoice_uuid)
    if not invoice_row:
        raise ValueError("Direct vendor invoice not found")
    if invoice_row["entity_id"] != entity["id"]:
        raise ValueError("Direct vendor invoice does not belong to that entity")

    matches = _get_direct_vendor_invoice_matches(session, invoice_uuid)
    history = _get_direct_vendor_invoice_history(session, invoice_uuid)
    suggestions = _suggest_bank_transactions_for_direct_vendor_invoice(
        session=session,
        entity_id=entity["id"],
        invoice_row=invoice_row,
        date_window_days=suggestion_date_window_days,
        amount_tolerance=amount_tolerance,
        limit=10,
    )

    return {
        "entity_code": entity_code,
        "invoice": invoice_row,
        "matches": matches,
        "history": history,
        "suggestions": suggestions,
    }


def upsert_direct_vendor_ap_invoice(
    session,
    entity_code: str,
    actor_email: str,
    vendor_name: str,
    invoice_number: str,
    invoice_date: date,
    total_amount: Decimal,
    due_date: date | None = None,
    received_date: date | None = None,
    vendor_code: str | None = None,
    subtotal_amount: Decimal | None = None,
    tax_amount: Decimal | None = None,
    currency_code: str | None = None,
    priority: str = "normal",
    status: str = "open",
    payment_status: str = "unpaid",
    source_document_name: str | None = None,
    note: str | None = None,
    payload_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    if priority not in DIRECT_VENDOR_PRIORITIES:
        raise ValueError(f"Invalid priority: {priority}")
    if status not in DIRECT_VENDOR_INVOICE_STATUSES:
        raise ValueError(f"Invalid status: {status}")
    if payment_status not in DIRECT_VENDOR_PAYMENT_STATUSES:
        raise ValueError(f"Invalid payment_status: {payment_status}")

    vendor_name_clean = _safe_text(vendor_name)
    invoice_number_clean = _safe_text(invoice_number)
    if not vendor_name_clean:
        raise ValueError("vendor_name is required")
    if not invoice_number_clean:
        raise ValueError("invoice_number is required")

    accounting_period_id = get_or_create_accounting_period(session, entity["id"], invoice_date)
    total_amount = Decimal(str(total_amount))
    subtotal_amount = Decimal(str(subtotal_amount or 0))
    tax_amount = Decimal(str(tax_amount or 0))
    open_amount = abs(total_amount)

    existing = session.execute(
        text(
            """
            SELECT id, status
            FROM direct_vendor_ap_invoices
            WHERE entity_id = :entity_id
              AND active = TRUE
              AND vendor_name = :vendor_name
              AND invoice_number = :invoice_number
            LIMIT 1
            """
        ),
        {
            "entity_id": entity["id"],
            "vendor_name": vendor_name_clean,
            "invoice_number": invoice_number_clean,
        },
    ).mappings().first()

    if existing:
        invoice_uuid = existing["id"]
        previous_status = existing["status"]
        session.execute(
            text(
                """
                UPDATE direct_vendor_ap_invoices
                SET accounting_period_id = :accounting_period_id,
                    vendor_code = :vendor_code,
                    invoice_date = :invoice_date,
                    due_date = :due_date,
                    received_date = :received_date,
                    currency_code = :currency_code,
                    subtotal_amount = :subtotal_amount,
                    tax_amount = :tax_amount,
                    total_amount = :total_amount,
                    priority = :priority,
                    status = :status,
                    payment_status = :payment_status,
                    source_document_name = :source_document_name,
                    note = :note,
                    raw_json = CAST(:raw_json AS jsonb),
                    updated_at = NOW()
                WHERE id = :invoice_id
                """
            ),
            {
                "invoice_id": invoice_uuid,
                "accounting_period_id": accounting_period_id,
                "vendor_code": _safe_text(vendor_code),
                "invoice_date": invoice_date,
                "due_date": due_date,
                "received_date": received_date,
                "currency_code": _safe_text(currency_code) or "CAD",
                "subtotal_amount": subtotal_amount,
                "tax_amount": tax_amount,
                "total_amount": total_amount,
                "priority": priority,
                "status": status,
                "payment_status": payment_status,
                "source_document_name": _safe_text(source_document_name),
                "note": note,
                "raw_json": json.dumps(payload_json or {}, default=str),
            },
        )
        action = "update"
    else:
        invoice_uuid = session.execute(
            text(
                """
                INSERT INTO direct_vendor_ap_invoices (
                    entity_id,
                    accounting_period_id,
                    vendor_name,
                    vendor_code,
                    invoice_number,
                    invoice_date,
                    due_date,
                    received_date,
                    currency_code,
                    subtotal_amount,
                    tax_amount,
                    total_amount,
                    paid_amount,
                    open_amount,
                    status,
                    payment_status,
                    priority,
                    source_document_name,
                    note,
                    raw_json,
                    created_by
                )
                VALUES (
                    :entity_id,
                    :accounting_period_id,
                    :vendor_name,
                    :vendor_code,
                    :invoice_number,
                    :invoice_date,
                    :due_date,
                    :received_date,
                    :currency_code,
                    :subtotal_amount,
                    :tax_amount,
                    :total_amount,
                    0,
                    :open_amount,
                    :status,
                    :payment_status,
                    :priority,
                    :source_document_name,
                    :note,
                    CAST(:raw_json AS jsonb),
                    :created_by
                )
                RETURNING id
                """
            ),
            {
                "entity_id": entity["id"],
                "accounting_period_id": accounting_period_id,
                "vendor_name": vendor_name_clean,
                "vendor_code": _safe_text(vendor_code),
                "invoice_number": invoice_number_clean,
                "invoice_date": invoice_date,
                "due_date": due_date,
                "received_date": received_date,
                "currency_code": _safe_text(currency_code) or "CAD",
                "subtotal_amount": subtotal_amount,
                "tax_amount": tax_amount,
                "total_amount": total_amount,
                "open_amount": open_amount,
                "status": status,
                "payment_status": payment_status,
                "priority": priority,
                "source_document_name": _safe_text(source_document_name),
                "note": note,
                "raw_json": json.dumps(payload_json or {}, default=str),
                "created_by": actor_email,
            },
        ).scalar_one()
        previous_status = None
        action = "create"

    _recalculate_direct_vendor_invoice_payment_fields(session, invoice_uuid)
    invoice_row = _get_direct_vendor_invoice_row(session, invoice_uuid)
    _insert_direct_vendor_invoice_event(
        session=session,
        entity_id=entity["id"],
        invoice_id=invoice_uuid,
        action=action,
        actor_email=actor_email,
        from_status=previous_status,
        to_status=invoice_row.get("status") if invoice_row else status,
        note=note,
        payload_json={
            "invoice_number": invoice_number_clean,
            "vendor_name": vendor_name_clean,
            "total_amount": str(total_amount),
        },
    )

    return get_direct_vendor_ap_invoice_detail(session, entity_code, str(invoice_uuid))


def set_direct_vendor_ap_invoice_status(
    session,
    entity_code: str,
    invoice_id: str,
    status: str,
    actor_email: str,
    note: str | None = None,
) -> dict[str, Any]:
    if status not in DIRECT_VENDOR_INVOICE_STATUSES:
        raise ValueError(f"Invalid status: {status}")

    detail = get_direct_vendor_ap_invoice_detail(session, entity_code, invoice_id)
    invoice_row = detail["invoice"]
    invoice_uuid = _parse_uuid(invoice_id, "invoice_id")
    previous_status = invoice_row.get("status")

    approved_by = actor_email if status == "approved" else invoice_row.get("approved_by")
    approved_at = datetime.now(timezone.utc) if status == "approved" else invoice_row.get("approved_at")

    session.execute(
        text(
            """
            UPDATE direct_vendor_ap_invoices
            SET status = :status,
                note = COALESCE(:note, note),
                approved_by = :approved_by,
                approved_at = :approved_at,
                updated_at = NOW()
            WHERE id = :invoice_id
            """
        ),
        {
            "invoice_id": invoice_uuid,
            "status": status,
            "note": note,
            "approved_by": approved_by,
            "approved_at": approved_at,
        },
    )

    _insert_direct_vendor_invoice_event(
        session=session,
        entity_id=invoice_row["entity_id"],
        invoice_id=invoice_uuid,
        action="set_status",
        actor_email=actor_email,
        from_status=previous_status,
        to_status=status,
        note=note,
        payload_json={},
    )

    return get_direct_vendor_ap_invoice_detail(session, entity_code, invoice_id)


def create_direct_vendor_ap_invoice_bank_match(
    session,
    entity_code: str,
    invoice_id: str,
    bank_transaction_id: str,
    actor_email: str,
    amount_matched: Decimal | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    detail = get_direct_vendor_ap_invoice_detail(session, entity_code, invoice_id)
    invoice_row = detail["invoice"]
    invoice_uuid = _parse_uuid(invoice_id, "invoice_id")

    remaining_amount = Decimal(str(invoice_row.get("open_amount") or 0))
    amount_to_match = amount_matched if amount_matched is not None else remaining_amount
    amount_to_match = abs(Decimal(str(amount_to_match)))
    if amount_to_match <= Decimal("0.00"):
        raise ValueError("No open amount remains to match")

    create_bank_transaction_match(
        session=session,
        entity_code=entity_code,
        transaction_id=bank_transaction_id,
        match_type=DIRECT_VENDOR_MATCH_TYPE,
        target_table=DIRECT_VENDOR_TARGET_TABLE,
        target_record_id=str(invoice_uuid),
        target_label=_build_direct_vendor_target_label(invoice_row),
        amount_matched=amount_to_match,
        actor_email=actor_email,
        note=note,
        payload_json={
            "vendor_name": invoice_row.get("vendor_name"),
            "invoice_number": invoice_row.get("invoice_number"),
        },
    )

    _recalculate_direct_vendor_invoice_payment_fields(session, invoice_uuid)
    refreshed = _get_direct_vendor_invoice_row(session, invoice_uuid)

    _insert_direct_vendor_invoice_event(
        session=session,
        entity_id=invoice_row["entity_id"],
        invoice_id=invoice_uuid,
        action="match",
        actor_email=actor_email,
        from_status=invoice_row.get("status"),
        to_status=refreshed.get("status") if refreshed else invoice_row.get("status"),
        note=note,
        payload_json={
            "bank_transaction_id": bank_transaction_id,
            "matched_amount": str(amount_to_match),
        },
    )

    return get_direct_vendor_ap_invoice_detail(session, entity_code, invoice_id)


def release_direct_vendor_ap_invoice_bank_match(
    session,
    entity_code: str,
    invoice_id: str,
    match_id: str,
    actor_email: str,
    note: str | None = None,
) -> dict[str, Any]:
    detail = get_direct_vendor_ap_invoice_detail(session, entity_code, invoice_id)
    invoice_row = detail["invoice"]
    invoice_uuid = _parse_uuid(invoice_id, "invoice_id")
    match_uuid = _parse_uuid(match_id, "match_id")

    match_row = session.execute(
        text(
            """
            SELECT id, bank_transaction_id, active, matched_amount
            FROM bank_transaction_matches
            WHERE id = :match_id
              AND target_table_name = :target_table
              AND target_record_id = :target_record_id
            LIMIT 1
            """
        ),
        {
            "match_id": match_uuid,
            "target_table": DIRECT_VENDOR_TARGET_TABLE,
            "target_record_id": str(invoice_uuid),
        },
    ).mappings().first()
    if not match_row:
        raise ValueError("Direct vendor invoice match not found")

    release_bank_transaction_match(
        session=session,
        entity_code=entity_code,
        transaction_id=str(match_row["bank_transaction_id"]),
        match_id=match_id,
        actor_email=actor_email,
        note=note,
    )

    _recalculate_direct_vendor_invoice_payment_fields(session, invoice_uuid)
    refreshed = _get_direct_vendor_invoice_row(session, invoice_uuid)

    _insert_direct_vendor_invoice_event(
        session=session,
        entity_id=invoice_row["entity_id"],
        invoice_id=invoice_uuid,
        action="unmatch",
        actor_email=actor_email,
        from_status=invoice_row.get("status"),
        to_status=refreshed.get("status") if refreshed else invoice_row.get("status"),
        note=note,
        payload_json={
            "match_id": match_id,
            "amount_released": str(match_row.get("matched_amount") or 0),
        },
    )

    return get_direct_vendor_ap_invoice_detail(session, entity_code, invoice_id)


def _cash_balancing_card_totals(session, entity_id: UUID, business_date: date | None) -> dict[str, Any] | None:
    if business_date is None:
        return None
    if not _has_table(session, "cash_balancing_rows"):
        return None

    row = session.execute(
        text(
            """
            SELECT
                COALESCE(SUM(debit_amount), 0) AS debit_amount,
                COALESCE(SUM(credit_amount), 0) AS credit_amount,
                COALESCE(SUM(COALESCE(debit_amount, 0) + COALESCE(credit_amount, 0)), 0) AS total_card_amount
            FROM cash_balancing_rows
            WHERE entity_id = :entity_id
              AND business_date = :business_date
            """
        ),
        {
            "entity_id": entity_id,
            "business_date": business_date,
        },
    ).mappings().first()
    return dict(row) if row else None


def _build_card_settlement_target_label(batch_row: dict[str, Any]) -> str:
    parts = ["Card settlement", _safe_text(batch_row.get("processor_name")) or "processor"]
    reference = _safe_text(batch_row.get("settlement_reference"))
    if reference:
        parts.append(reference)
    business_date = batch_row.get("business_date")
    if business_date:
        parts.append(str(business_date))
    return " - ".join(parts)


def _insert_card_settlement_event(
    session,
    entity_id: UUID,
    batch_id: UUID,
    action: str,
    actor_email: str,
    from_status: str | None = None,
    to_status: str | None = None,
    note: str | None = None,
    payload_json: dict[str, Any] | None = None,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO card_settlement_events (
                entity_id,
                batch_id,
                action,
                actor_email,
                from_status,
                to_status,
                note,
                payload_json
            )
            VALUES (
                :entity_id,
                :batch_id,
                :action,
                :actor_email,
                :from_status,
                :to_status,
                :note,
                CAST(:payload_json AS jsonb)
            )
            """
        ),
        {
            "entity_id": entity_id,
            "batch_id": batch_id,
            "action": action,
            "actor_email": actor_email,
            "from_status": from_status,
            "to_status": to_status,
            "note": note,
            "payload_json": json.dumps(payload_json or {}, default=str),
        },
    )


def _get_card_settlement_matches(session, batch_uuid: UUID) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                m.id,
                m.bank_transaction_id,
                m.match_type,
                m.target_table_name AS target_table,
                m.target_record_id,
                m.target_label,
                m.matched_amount,
                m.active,
                m.note,
                m.payload_json,
                m.created_by,
                m.created_at,
                m.released_by,
                m.released_at,
                bt.transaction_date AS bank_transaction_date,
                bt.posted_date AS bank_posted_date,
                bt.amount AS bank_transaction_amount,
                bt.source_account_name AS bank_source_account_name,
                bt.source_account_code AS bank_source_account_code,
                bt.description AS bank_description,
                bt.normalized_description AS bank_normalized_description,
                bt.counterparty_name AS bank_counterparty_name,
                bt.reference_number AS bank_reference_number,
                bt.review_status AS bank_review_status
            FROM bank_transaction_matches m
            LEFT JOIN bank_transactions bt ON bt.id = m.bank_transaction_id
            WHERE m.target_table_name = :target_table
              AND m.target_record_id = :target_record_id
            ORDER BY m.created_at DESC
            """
        ),
        {
            "target_table": CARD_SETTLEMENT_TARGET_TABLE,
            "target_record_id": str(batch_uuid),
        },
    ).mappings().all()
    return [dict(row) for row in rows]


def _get_card_settlement_history(session, batch_uuid: UUID) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                id,
                batch_id,
                action,
                actor_email,
                from_status,
                to_status,
                note,
                payload_json,
                created_at
            FROM card_settlement_events
            WHERE batch_id = :batch_id
            ORDER BY created_at DESC
            """
        ),
        {"batch_id": batch_uuid},
    ).mappings().all()
    return [dict(row) for row in rows]


def _hydrate_card_settlement_batch_row(session, row: dict[str, Any]) -> dict[str, Any]:
    hydrated = dict(row)
    business_date = row.get("business_date")
    if isinstance(business_date, str):
        business_date = date.fromisoformat(business_date)

    cash_totals = _cash_balancing_card_totals(session, row["entity_id"], business_date)
    expected_cash_amount = Decimal(str((cash_totals or {}).get("total_card_amount") or row.get("expected_cash_balancing_amount") or 0))
    gross_amount = Decimal(str(row.get("gross_sales_amount") or 0))
    net_amount = abs(Decimal(str(row.get("net_deposit_amount") or 0)))
    matched_bank_amount = abs(Decimal(str(row.get("active_matched_amount") or 0)))
    bank_difference = net_amount - matched_bank_amount
    if bank_difference < Decimal("0.00"):
        bank_difference = Decimal("0.00")
    cash_variance = gross_amount - expected_cash_amount if expected_cash_amount != Decimal("0.00") else None
    bank_match_state = "matched" if bank_difference <= Decimal("0.05") and matched_bank_amount > 0 else ("partially_matched" if matched_bank_amount > 0 else "unmatched")
    hydrated["expected_cash_balancing_amount"] = expected_cash_amount if cash_totals is not None else row.get("expected_cash_balancing_amount")
    hydrated["cash_balancing_totals"] = cash_totals
    hydrated["cash_variance_amount"] = cash_variance
    hydrated["active_matched_amount"] = matched_bank_amount
    hydrated["bank_unmatched_amount"] = bank_difference
    hydrated["bank_match_state"] = bank_match_state
    return hydrated


def _get_card_settlement_batch_row(session, batch_uuid: UUID):
    row = session.execute(
        text(
            """
            WITH active_matches AS (
                SELECT
                    m.target_record_id,
                    COUNT(*) AS active_match_count,
                    COALESCE(SUM(m.matched_amount), 0) AS active_matched_amount,
                    MAX(bt.transaction_date) AS last_bank_match_date
                FROM bank_transaction_matches m
                LEFT JOIN bank_transactions bt ON bt.id = m.bank_transaction_id
                WHERE m.active = TRUE
                  AND m.target_table_name = :target_table
                GROUP BY m.target_record_id
            )
            SELECT
                b.*,
                COALESCE(am.active_match_count, 0) AS active_match_count,
                COALESCE(am.active_matched_amount, 0) AS active_matched_amount,
                am.last_bank_match_date
            FROM card_settlement_batches b
            LEFT JOIN active_matches am ON am.target_record_id = b.id::text
            WHERE b.id = :batch_id
              AND b.active = TRUE
            """
        ),
        {
            "batch_id": batch_uuid,
            "target_table": CARD_SETTLEMENT_TARGET_TABLE,
        },
    ).mappings().first()
    return _hydrate_card_settlement_batch_row(session, row) if row else None


def _recalculate_card_settlement_metrics(session, batch_uuid: UUID) -> None:
    row = _get_card_settlement_batch_row(session, batch_uuid)
    if not row:
        return

    current_status = row.get("reconciliation_status")
    next_status = current_status
    cash_variance = row.get("cash_variance_amount")
    bank_match_state = row.get("bank_match_state")

    if current_status != "ignored":
        if bank_match_state == "matched" and (cash_variance is None or abs(Decimal(str(cash_variance))) <= Decimal("0.05")):
            next_status = "reconciled"
        elif current_status == "reconciled":
            next_status = "needs_review"

    session.execute(
        text(
            """
            UPDATE card_settlement_batches
            SET expected_cash_balancing_amount = :expected_cash_balancing_amount,
                matched_bank_amount = :matched_bank_amount,
                reconciliation_status = :reconciliation_status,
                updated_at = NOW()
            WHERE id = :batch_id
            """
        ),
        {
            "batch_id": batch_uuid,
            "expected_cash_balancing_amount": row.get("expected_cash_balancing_amount"),
            "matched_bank_amount": row.get("active_matched_amount"),
            "reconciliation_status": next_status,
        },
    )


def _suggest_bank_transactions_for_card_settlement(
    session,
    entity_id: UUID,
    batch_row: dict[str, Any],
    date_window_days: int = 7,
    amount_tolerance: Decimal = Decimal("0.05"),
    limit: int = 10,
) -> list[dict[str, Any]]:
    target_amount = abs(Decimal(str(batch_row.get("net_deposit_amount") or 0)))
    if target_amount <= Decimal("0.00"):
        return []

    anchor_date = batch_row.get("deposit_date") or batch_row.get("business_date")
    if isinstance(anchor_date, str):
        anchor_date = date.fromisoformat(anchor_date)
    if not anchor_date:
        return []

    date_start = anchor_date - timedelta(days=max(0, int(date_window_days)))
    date_end = anchor_date + timedelta(days=max(0, int(date_window_days)))

    rows = session.execute(
        text(
            """
            WITH active_matches AS (
                SELECT bank_transaction_id, COUNT(*) AS active_match_count
                FROM bank_transaction_matches
                WHERE active = TRUE
                GROUP BY bank_transaction_id
            )
            SELECT
                bt.id,
                bt.transaction_date,
                bt.posted_date,
                bt.amount,
                bt.source_account_name,
                bt.source_account_code,
                bt.description,
                bt.normalized_description,
                bt.counterparty_name,
                bt.reference_number,
                bt.review_status,
                COALESCE(am.active_match_count, 0) AS active_match_count
            FROM bank_transactions bt
            LEFT JOIN active_matches am ON am.bank_transaction_id = bt.id
            WHERE bt.entity_id = :entity_id
              AND COALESCE(bt.review_status, 'new') <> 'ignored'
              AND bt.transaction_date BETWEEN :date_start AND :date_end
            ORDER BY bt.transaction_date, bt.id
            """
        ),
        {
            "entity_id": entity_id,
            "date_start": date_start,
            "date_end": date_end,
        },
    ).mappings().all()

    processor_name = (_safe_text(batch_row.get("processor_name")) or "").lower()
    suggestions: list[dict[str, Any]] = []
    for row in rows:
        bank_amount_abs = abs(Decimal(str(row.get("amount") or 0)))
        amount_diff = abs(bank_amount_abs - target_amount)
        direction = "inflow" if Decimal(str(row.get("amount") or 0)) >= 0 else "outflow"
        if direction != "inflow":
            continue

        desc_blob = " ".join(
            [
                _safe_text(row.get("counterparty_name")) or "",
                _safe_text(row.get("normalized_description")) or "",
                _safe_text(row.get("description")) or "",
            ]
        ).lower()
        processor_match = 1 if processor_name and processor_name in desc_blob else 0
        threshold = max(amount_tolerance, target_amount * Decimal("0.05"))
        if amount_diff > threshold and processor_match == 0:
            continue

        transaction_date = row.get("transaction_date")
        if isinstance(transaction_date, str):
            transaction_date = date.fromisoformat(transaction_date)
        date_diff_days = abs((transaction_date - anchor_date).days) if transaction_date else None

        suggestion = dict(row)
        suggestion["amount_diff"] = amount_diff
        suggestion["date_diff_days"] = date_diff_days
        suggestion["processor_match"] = bool(processor_match)
        suggestion["suggestion_score"] = (
            (100 - min(99, int(amount_diff * 100)))
            + (processor_match * 20)
            + (0 if date_diff_days is None else max(0, 10 - min(date_diff_days, 10)))
        )
        suggestions.append(suggestion)

    suggestions.sort(
        key=lambda row: (
            -row["suggestion_score"],
            row["amount_diff"],
            row["date_diff_days"] if row["date_diff_days"] is not None else 999,
            row["id"],
        )
    )
    return suggestions[:limit]


def list_card_settlement_batches(
    session,
    entity_code: str,
    date_from: date,
    date_to: date,
    reconciliation_status: str | None = None,
    bank_match_state: str | None = None,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    rows = session.execute(
        text(
            """
            WITH active_matches AS (
                SELECT
                    m.target_record_id,
                    COUNT(*) AS active_match_count,
                    COALESCE(SUM(m.matched_amount), 0) AS active_matched_amount,
                    MAX(bt.transaction_date) AS last_bank_match_date
                FROM bank_transaction_matches m
                LEFT JOIN bank_transactions bt ON bt.id = m.bank_transaction_id
                WHERE m.active = TRUE
                  AND m.target_table_name = :target_table
                GROUP BY m.target_record_id
            )
            SELECT
                b.*,
                COALESCE(am.active_match_count, 0) AS active_match_count,
                COALESCE(am.active_matched_amount, 0) AS active_matched_amount,
                am.last_bank_match_date
            FROM card_settlement_batches b
            LEFT JOIN active_matches am ON am.target_record_id = b.id::text
            WHERE b.entity_id = :entity_id
              AND b.active = TRUE
              AND b.business_date BETWEEN :date_from AND :date_to
            ORDER BY b.business_date, b.processor_name, b.id
            """
        ),
        {
            "entity_id": entity["id"],
            "date_from": date_from,
            "date_to": date_to,
            "target_table": CARD_SETTLEMENT_TARGET_TABLE,
        },
    ).mappings().all()

    hydrated_rows = [_hydrate_card_settlement_batch_row(session, dict(row)) for row in rows]
    filtered_rows: list[dict[str, Any]] = []
    for row in hydrated_rows:
        if reconciliation_status and row.get("reconciliation_status") != reconciliation_status:
            continue
        if bank_match_state and row.get("bank_match_state") != bank_match_state:
            continue
        filtered_rows.append(row)

    summary = {
        "total_count": len(filtered_rows),
        "gross_sales_amount_total": sum(Decimal(str(row.get("gross_sales_amount") or 0)) for row in filtered_rows),
        "net_deposit_amount_total": sum(Decimal(str(row.get("net_deposit_amount") or 0)) for row in filtered_rows),
        "matched_bank_amount_total": sum(Decimal(str(row.get("active_matched_amount") or 0)) for row in filtered_rows),
        "reconciled_count": sum(1 for row in filtered_rows if row.get("reconciliation_status") == "reconciled"),
        "needs_review_count": sum(1 for row in filtered_rows if row.get("reconciliation_status") == "needs_review"),
        "new_count": sum(1 for row in filtered_rows if row.get("reconciliation_status") == "new"),
        "ignored_count": sum(1 for row in filtered_rows if row.get("reconciliation_status") == "ignored"),
        "bank_matched_count": sum(1 for row in filtered_rows if row.get("bank_match_state") == "matched"),
        "bank_partially_matched_count": sum(1 for row in filtered_rows if row.get("bank_match_state") == "partially_matched"),
        "bank_unmatched_count": sum(1 for row in filtered_rows if row.get("bank_match_state") == "unmatched"),
    }
    return {
        "entity_code": entity_code,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "reconciliation_status": reconciliation_status,
        "bank_match_state": bank_match_state,
        "count": len(filtered_rows),
        "summary": summary,
        "batches": filtered_rows,
    }


def get_card_settlement_batch_detail(
    session,
    entity_code: str,
    batch_id: str,
    suggestion_date_window_days: int = 7,
    amount_tolerance: Decimal = Decimal("0.05"),
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    batch_uuid = _parse_uuid(batch_id, "batch_id")
    batch_row = _get_card_settlement_batch_row(session, batch_uuid)
    if not batch_row:
        raise ValueError("Card settlement batch not found")
    if batch_row["entity_id"] != entity["id"]:
        raise ValueError("Card settlement batch does not belong to that entity")

    matches = _get_card_settlement_matches(session, batch_uuid)
    history = _get_card_settlement_history(session, batch_uuid)
    suggestions = _suggest_bank_transactions_for_card_settlement(
        session=session,
        entity_id=entity["id"],
        batch_row=batch_row,
        date_window_days=suggestion_date_window_days,
        amount_tolerance=amount_tolerance,
        limit=10,
    )

    return {
        "entity_code": entity_code,
        "batch": batch_row,
        "matches": matches,
        "history": history,
        "suggestions": suggestions,
    }


def upsert_card_settlement_batch(
    session,
    entity_code: str,
    actor_email: str,
    processor_name: str,
    business_date: date,
    net_deposit_amount: Decimal,
    deposit_date: date | None = None,
    merchant_account: str | None = None,
    settlement_reference: str | None = None,
    currency_code: str | None = None,
    gross_sales_amount: Decimal | None = None,
    refunds_amount: Decimal | None = None,
    chargebacks_amount: Decimal | None = None,
    fees_amount: Decimal | None = None,
    tax_on_fees_amount: Decimal | None = None,
    reconciliation_status: str = "new",
    source_file_name: str | None = None,
    note: str | None = None,
    payload_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    if reconciliation_status not in CARD_SETTLEMENT_RECON_STATUSES:
        raise ValueError(f"Invalid reconciliation_status: {reconciliation_status}")

    processor_name_clean = _safe_text(processor_name)
    if not processor_name_clean:
        raise ValueError("processor_name is required")

    accounting_period_id = get_or_create_accounting_period(session, entity["id"], deposit_date or business_date)
    gross_sales_amount = Decimal(str(gross_sales_amount or 0))
    refunds_amount = Decimal(str(refunds_amount or 0))
    chargebacks_amount = Decimal(str(chargebacks_amount or 0))
    fees_amount = Decimal(str(fees_amount or 0))
    tax_on_fees_amount = Decimal(str(tax_on_fees_amount or 0))
    net_deposit_amount = Decimal(str(net_deposit_amount))
    expected_cash = (_cash_balancing_card_totals(session, entity["id"], business_date) or {}).get("total_card_amount")

    existing = session.execute(
        text(
            """
            SELECT id, reconciliation_status
            FROM card_settlement_batches
            WHERE entity_id = :entity_id
              AND active = TRUE
              AND processor_name = :processor_name
              AND business_date = :business_date
              AND settlement_reference IS NOT DISTINCT FROM :settlement_reference
            LIMIT 1
            """
        ),
        {
            "entity_id": entity["id"],
            "processor_name": processor_name_clean,
            "business_date": business_date,
            "settlement_reference": _safe_text(settlement_reference),
        },
    ).mappings().first()

    if existing:
        batch_uuid = existing["id"]
        previous_status = existing["reconciliation_status"]
        session.execute(
            text(
                """
                UPDATE card_settlement_batches
                SET accounting_period_id = :accounting_period_id,
                    deposit_date = :deposit_date,
                    merchant_account = :merchant_account,
                    currency_code = :currency_code,
                    gross_sales_amount = :gross_sales_amount,
                    refunds_amount = :refunds_amount,
                    chargebacks_amount = :chargebacks_amount,
                    fees_amount = :fees_amount,
                    tax_on_fees_amount = :tax_on_fees_amount,
                    net_deposit_amount = :net_deposit_amount,
                    expected_cash_balancing_amount = :expected_cash_balancing_amount,
                    reconciliation_status = :reconciliation_status,
                    source_file_name = :source_file_name,
                    note = :note,
                    raw_json = CAST(:raw_json AS jsonb),
                    updated_at = NOW()
                WHERE id = :batch_id
                """
            ),
            {
                "batch_id": batch_uuid,
                "accounting_period_id": accounting_period_id,
                "deposit_date": deposit_date,
                "merchant_account": _safe_text(merchant_account),
                "currency_code": _safe_text(currency_code) or "CAD",
                "gross_sales_amount": gross_sales_amount,
                "refunds_amount": refunds_amount,
                "chargebacks_amount": chargebacks_amount,
                "fees_amount": fees_amount,
                "tax_on_fees_amount": tax_on_fees_amount,
                "net_deposit_amount": net_deposit_amount,
                "expected_cash_balancing_amount": expected_cash,
                "reconciliation_status": reconciliation_status,
                "source_file_name": _safe_text(source_file_name),
                "note": note,
                "raw_json": json.dumps(payload_json or {}, default=str),
            },
        )
        action = "update"
    else:
        batch_uuid = session.execute(
            text(
                """
                INSERT INTO card_settlement_batches (
                    entity_id,
                    accounting_period_id,
                    processor_name,
                    merchant_account,
                    settlement_reference,
                    business_date,
                    deposit_date,
                    currency_code,
                    gross_sales_amount,
                    refunds_amount,
                    chargebacks_amount,
                    fees_amount,
                    tax_on_fees_amount,
                    net_deposit_amount,
                    expected_cash_balancing_amount,
                    matched_bank_amount,
                    reconciliation_status,
                    source_file_name,
                    note,
                    raw_json,
                    created_by
                )
                VALUES (
                    :entity_id,
                    :accounting_period_id,
                    :processor_name,
                    :merchant_account,
                    :settlement_reference,
                    :business_date,
                    :deposit_date,
                    :currency_code,
                    :gross_sales_amount,
                    :refunds_amount,
                    :chargebacks_amount,
                    :fees_amount,
                    :tax_on_fees_amount,
                    :net_deposit_amount,
                    :expected_cash_balancing_amount,
                    0,
                    :reconciliation_status,
                    :source_file_name,
                    :note,
                    CAST(:raw_json AS jsonb),
                    :created_by
                )
                RETURNING id
                """
            ),
            {
                "entity_id": entity["id"],
                "accounting_period_id": accounting_period_id,
                "processor_name": processor_name_clean,
                "merchant_account": _safe_text(merchant_account),
                "settlement_reference": _safe_text(settlement_reference),
                "business_date": business_date,
                "deposit_date": deposit_date,
                "currency_code": _safe_text(currency_code) or "CAD",
                "gross_sales_amount": gross_sales_amount,
                "refunds_amount": refunds_amount,
                "chargebacks_amount": chargebacks_amount,
                "fees_amount": fees_amount,
                "tax_on_fees_amount": tax_on_fees_amount,
                "net_deposit_amount": net_deposit_amount,
                "expected_cash_balancing_amount": expected_cash,
                "reconciliation_status": reconciliation_status,
                "source_file_name": _safe_text(source_file_name),
                "note": note,
                "raw_json": json.dumps(payload_json or {}, default=str),
                "created_by": actor_email,
            },
        ).scalar_one()
        previous_status = None
        action = "create"

    _recalculate_card_settlement_metrics(session, batch_uuid)
    batch_row = _get_card_settlement_batch_row(session, batch_uuid)
    _insert_card_settlement_event(
        session=session,
        entity_id=entity["id"],
        batch_id=batch_uuid,
        action=action,
        actor_email=actor_email,
        from_status=previous_status,
        to_status=batch_row.get("reconciliation_status") if batch_row else reconciliation_status,
        note=note,
        payload_json={
            "processor_name": processor_name_clean,
            "business_date": str(business_date),
            "net_deposit_amount": str(net_deposit_amount),
        },
    )

    return get_card_settlement_batch_detail(session, entity_code, str(batch_uuid))


def set_card_settlement_status(
    session,
    entity_code: str,
    batch_id: str,
    reconciliation_status: str,
    actor_email: str,
    note: str | None = None,
) -> dict[str, Any]:
    if reconciliation_status not in CARD_SETTLEMENT_RECON_STATUSES:
        raise ValueError(f"Invalid reconciliation_status: {reconciliation_status}")

    detail = get_card_settlement_batch_detail(session, entity_code, batch_id)
    batch_row = detail["batch"]
    batch_uuid = _parse_uuid(batch_id, "batch_id")
    previous_status = batch_row.get("reconciliation_status")

    session.execute(
        text(
            """
            UPDATE card_settlement_batches
            SET reconciliation_status = :reconciliation_status,
                reviewed_by = :reviewed_by,
                reviewed_at = NOW(),
                note = COALESCE(:note, note),
                updated_at = NOW()
            WHERE id = :batch_id
            """
        ),
        {
            "batch_id": batch_uuid,
            "reconciliation_status": reconciliation_status,
            "reviewed_by": actor_email,
            "note": note,
        },
    )

    _insert_card_settlement_event(
        session=session,
        entity_id=batch_row["entity_id"],
        batch_id=batch_uuid,
        action="set_status",
        actor_email=actor_email,
        from_status=previous_status,
        to_status=reconciliation_status,
        note=note,
        payload_json={},
    )

    return get_card_settlement_batch_detail(session, entity_code, batch_id)


def create_card_settlement_bank_match(
    session,
    entity_code: str,
    batch_id: str,
    bank_transaction_id: str,
    actor_email: str,
    amount_matched: Decimal | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    detail = get_card_settlement_batch_detail(session, entity_code, batch_id)
    batch_row = detail["batch"]
    batch_uuid = _parse_uuid(batch_id, "batch_id")

    remaining_amount = Decimal(str(batch_row.get("bank_unmatched_amount") or 0))
    amount_to_match = amount_matched if amount_matched is not None else remaining_amount
    amount_to_match = abs(Decimal(str(amount_to_match)))
    if amount_to_match <= Decimal("0.00"):
        raise ValueError("No bank amount remains to match")

    create_bank_transaction_match(
        session=session,
        entity_code=entity_code,
        transaction_id=bank_transaction_id,
        match_type=CARD_SETTLEMENT_MATCH_TYPE,
        target_table=CARD_SETTLEMENT_TARGET_TABLE,
        target_record_id=str(batch_uuid),
        target_label=_build_card_settlement_target_label(batch_row),
        amount_matched=amount_to_match,
        actor_email=actor_email,
        note=note,
        payload_json={
            "processor_name": batch_row.get("processor_name"),
            "business_date": str(batch_row.get("business_date")),
            "settlement_reference": batch_row.get("settlement_reference"),
        },
    )

    _recalculate_card_settlement_metrics(session, batch_uuid)
    refreshed = _get_card_settlement_batch_row(session, batch_uuid)

    _insert_card_settlement_event(
        session=session,
        entity_id=batch_row["entity_id"],
        batch_id=batch_uuid,
        action="match",
        actor_email=actor_email,
        from_status=batch_row.get("reconciliation_status"),
        to_status=refreshed.get("reconciliation_status") if refreshed else batch_row.get("reconciliation_status"),
        note=note,
        payload_json={
            "bank_transaction_id": bank_transaction_id,
            "matched_amount": str(amount_to_match),
        },
    )

    return get_card_settlement_batch_detail(session, entity_code, batch_id)


def release_card_settlement_bank_match(
    session,
    entity_code: str,
    batch_id: str,
    match_id: str,
    actor_email: str,
    note: str | None = None,
) -> dict[str, Any]:
    detail = get_card_settlement_batch_detail(session, entity_code, batch_id)
    batch_row = detail["batch"]
    batch_uuid = _parse_uuid(batch_id, "batch_id")
    match_uuid = _parse_uuid(match_id, "match_id")

    match_row = session.execute(
        text(
            """
            SELECT id, bank_transaction_id, active, matched_amount
            FROM bank_transaction_matches
            WHERE id = :match_id
              AND target_table_name = :target_table
              AND target_record_id = :target_record_id
            LIMIT 1
            """
        ),
        {
            "match_id": match_uuid,
            "target_table": CARD_SETTLEMENT_TARGET_TABLE,
            "target_record_id": str(batch_uuid),
        },
    ).mappings().first()
    if not match_row:
        raise ValueError("Card settlement match not found")

    release_bank_transaction_match(
        session=session,
        entity_code=entity_code,
        transaction_id=str(match_row["bank_transaction_id"]),
        match_id=match_id,
        actor_email=actor_email,
        note=note,
    )

    _recalculate_card_settlement_metrics(session, batch_uuid)
    refreshed = _get_card_settlement_batch_row(session, batch_uuid)

    _insert_card_settlement_event(
        session=session,
        entity_id=batch_row["entity_id"],
        batch_id=batch_uuid,
        action="unmatch",
        actor_email=actor_email,
        from_status=batch_row.get("reconciliation_status"),
        to_status=refreshed.get("reconciliation_status") if refreshed else batch_row.get("reconciliation_status"),
        note=note,
        payload_json={
            "match_id": match_id,
            "amount_released": str(match_row.get("matched_amount") or 0),
        },
    )

    return get_card_settlement_batch_detail(session, entity_code, batch_id)
