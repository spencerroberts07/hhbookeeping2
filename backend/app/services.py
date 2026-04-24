import json
from datetime import date, datetime, time, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from sqlalchemy import text

from .quickbooks import (
    BANK_ACCOUNT_TYPES,
    QuickBooksClient,
    ensure_valid_access_token,
    upsert_connection,
)

REVIEW_STATUS_NEW = "new"
REVIEW_STATUS_NEEDS_REVIEW = "needs_review"
REVIEW_STATUS_MATCHED = "matched"
REVIEW_STATUS_IGNORED = "ignored"

VALID_REVIEW_STATUSES = {
    REVIEW_STATUS_NEW,
    REVIEW_STATUS_NEEDS_REVIEW,
    REVIEW_STATUS_MATCHED,
    REVIEW_STATUS_IGNORED,
}

VALID_MATCH_TYPES = {
    "manual_explanation",
    "hh_remittance",
    "direct_vendor_payment",
    "cash_deposit",
    "card_settlement",
    "transfer_pair",
    "other",
}


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None



def normalize_actor_email(value: Any) -> str | None:
    cleaned = normalize_text(value)
    if not cleaned:
        return None
    return cleaned.lower()



def parse_json_value(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}



def money_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0.00")
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)



def money_float(value: Any) -> float:
    return float(money_decimal(value))



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



def _derive_counterparty_name(record: dict[str, Any]) -> str | None:
    for ref_key in ("VendorRef", "CustomerRef", "EntityRef"):
        ref = record.get(ref_key)
        if isinstance(ref, dict):
            name = normalize_text(ref.get("name"))
            if name:
                return name

    nested_txn = record.get("transaction") if isinstance(record.get("transaction"), dict) else None
    if nested_txn:
        return _derive_counterparty_name(nested_txn)

    return None



def _looks_unhelpful_description(value: Any) -> bool:
    cleaned = normalize_text(value)
    if not cleaned:
        return True
    if cleaned.isdigit():
        return True
    if len(cleaned) <= 2:
        return True
    return False



def _derive_display_description(row: dict[str, Any]) -> str:
    description = normalize_text(row.get("description"))
    raw_json = parse_json_value(row.get("raw_json"))
    counterparty = _derive_counterparty_name(raw_json)
    reference = normalize_text(row.get("reference_number"))
    source_type = normalize_text(row.get("source_transaction_type")) or "Transaction"

    if description and not _looks_unhelpful_description(description):
        return description
    if counterparty and reference:
        return f"{counterparty} / {reference}"
    if counterparty:
        return counterparty
    if reference:
        return f"{source_type} {reference}"
    if description:
        return description
    return source_type



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
    doc_number = record.get("DocNumber")
    private_note = record.get("PrivateNote")

    currency_code = (
        ((record.get("CurrencyRef") or {}).get("value"))
        if isinstance(record.get("CurrencyRef"), dict)
        else None
    ) or "CAD"

    total_amt = _safe_decimal(record.get("TotalAmt") or record.get("HomeTotalAmt") or 0)
    counterparty = _derive_counterparty_name(record)

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

        description = private_note or counterparty or doc_number or txn_type

        hits.append(
            {
                "source_transaction_id": f"{txn_type}:{txn_id}:{header_account_id}:header",
                "source_transaction_type": txn_type,
                "transaction_date": txn_date,
                "posted_date": posted_date,
                "description": description,
                "reference_number": doc_number,
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
        description = line.get("Description") or private_note or counterparty or doc_number or txn_type

        hits.append(
            {
                "source_transaction_id": f"{txn_type}:{txn_id}:{account_id}:line:{idx}",
                "source_transaction_type": txn_type,
                "transaction_date": txn_date,
                "posted_date": posted_date,
                "description": description,
                "reference_number": doc_number,
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
                                transaction_date, posted_date, description, reference_number,
                                amount, currency_code, direction, raw_json
                            )
                            VALUES (
                                :entity_id, :accounting_period_id, 'quickbooks', :source_connection_id,
                                :source_account_id, :source_account_name, :source_account_code,
                                :source_transaction_id, :source_transaction_type,
                                :transaction_date, :posted_date, :description, :reference_number,
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
        SELECT id, source_system, source_account_name, source_account_code,
               source_transaction_id, source_transaction_type,
               transaction_date, posted_date, description, reference_number,
               amount, currency_code, direction, review_status, imported_at, last_seen_at
        FROM bank_transactions
        WHERE entity_id = :entity_id
          AND transaction_date BETWEEN :date_from AND :date_to
    """

    params: dict[str, Any] = {
        "entity_id": entity["id"],
        "date_from": date_from,
        "date_to": date_to,
    }

    if review_status:
        sql += " AND review_status = :review_status"
        params["review_status"] = review_status

    sql += " ORDER BY transaction_date DESC, imported_at DESC LIMIT 500"

    rows = session.execute(text(sql), params).mappings().all()

    return {
        "entity_code": entity_code,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "review_status": review_status,
        "count": len(rows),
        "transactions": [dict(row) for row in rows],
    }



def _get_bank_transaction_row(session, transaction_id: str):
    return session.execute(
        text(
            """
            SELECT bt.*,
                   e.entity_code,
                   e.entity_name
            FROM bank_transactions bt
            JOIN entities e ON e.id = bt.entity_id
            WHERE bt.id = :transaction_id
            LIMIT 1
            """
        ),
        {"transaction_id": transaction_id},
    ).mappings().first()



def _serialize_match_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    if payload.get("matched_amount") is not None:
        payload["matched_amount"] = money_float(payload.get("matched_amount"))
    if payload.get("created_at"):
        payload["created_at"] = payload["created_at"].isoformat()
    if payload.get("released_at"):
        payload["released_at"] = payload["released_at"].isoformat()
    payload["raw_json"] = parse_json_value(payload.get("raw_json"))
    return payload



def _serialize_review_event_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    if payload.get("created_at"):
        payload["created_at"] = payload["created_at"].isoformat()
    payload["payload_json"] = parse_json_value(payload.get("payload_json"))
    return payload



def _serialize_bank_transaction_row(row: dict[str, Any], *, include_raw_json: bool) -> dict[str, Any]:
    payload = dict(row)
    raw_json = parse_json_value(payload.get("raw_json"))
    payload["display_description"] = _derive_display_description(payload)
    payload["counterparty_name_guess"] = _derive_counterparty_name(raw_json)
    payload["is_matched"] = bool(payload.get("active_match_id"))

    for key in ("amount", "active_matched_amount"):
        if payload.get(key) is not None:
            payload[key] = money_float(payload.get(key))

    for key in (
        "transaction_date",
        "posted_date",
        "reviewed_at",
        "imported_at",
        "last_seen_at",
    ):
        if payload.get(key):
            value = payload.get(key)
            payload[key] = value.isoformat() if hasattr(value, "isoformat") else str(value)

    if include_raw_json:
        payload["raw_json"] = raw_json
    else:
        payload.pop("raw_json", None)

    return payload



def _insert_bank_review_event(
    session,
    *,
    bank_transaction_id: str,
    entity_id: str,
    action: str,
    actor_email: str,
    from_review_status: str | None,
    to_review_status: str | None,
    note: str | None,
    payload_json: dict[str, Any] | None = None,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO bank_transaction_review_events (
                bank_transaction_id,
                entity_id,
                action,
                actor_email,
                from_review_status,
                to_review_status,
                note,
                payload_json
            ) VALUES (
                :bank_transaction_id,
                :entity_id,
                :action,
                :actor_email,
                :from_review_status,
                :to_review_status,
                :note,
                CAST(:payload_json AS jsonb)
            )
            """
        ),
        {
            "bank_transaction_id": bank_transaction_id,
            "entity_id": entity_id,
            "action": action,
            "actor_email": actor_email,
            "from_review_status": from_review_status,
            "to_review_status": to_review_status,
            "note": note,
            "payload_json": json.dumps(payload_json or {}, default=str),
        },
    )



def list_bank_review_summary(session, entity_code: str, date_from: date, date_to: date) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    totals = session.execute(
        text(
            """
            SELECT
                COUNT(*) AS total_count,
                COALESCE(SUM(bt.amount), 0) AS total_amount,
                COALESCE(SUM(CASE WHEN bt.review_status = 'new' THEN 1 ELSE 0 END), 0) AS new_count,
                COALESCE(SUM(CASE WHEN bt.review_status = 'needs_review' THEN 1 ELSE 0 END), 0) AS needs_review_count,
                COALESCE(SUM(CASE WHEN bt.review_status = 'matched' THEN 1 ELSE 0 END), 0) AS matched_status_count,
                COALESCE(SUM(CASE WHEN bt.review_status = 'ignored' THEN 1 ELSE 0 END), 0) AS ignored_count,
                COALESCE(SUM(CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END), 0) AS active_match_count,
                COALESCE(SUM(CASE WHEN m.id IS NULL THEN 1 ELSE 0 END), 0) AS unmatched_count
            FROM bank_transactions bt
            LEFT JOIN bank_transaction_matches m
              ON m.bank_transaction_id = bt.id
             AND m.active = TRUE
            WHERE bt.entity_id = :entity_id
              AND bt.transaction_date BETWEEN :date_from AND :date_to
            """
        ),
        {
            "entity_id": entity["id"],
            "date_from": date_from,
            "date_to": date_to,
        },
    ).mappings().first()

    by_status = session.execute(
        text(
            """
            SELECT review_status, COUNT(*) AS row_count, COALESCE(SUM(amount), 0) AS total_amount
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

    by_account = session.execute(
        text(
            """
            SELECT
                bt.source_account_name,
                COUNT(*) AS row_count,
                COALESCE(SUM(bt.amount), 0) AS total_amount,
                COALESCE(SUM(CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END), 0) AS matched_row_count
            FROM bank_transactions bt
            LEFT JOIN bank_transaction_matches m
              ON m.bank_transaction_id = bt.id
             AND m.active = TRUE
            WHERE bt.entity_id = :entity_id
              AND bt.transaction_date BETWEEN :date_from AND :date_to
            GROUP BY bt.source_account_name
            ORDER BY bt.source_account_name
            """
        ),
        {
            "entity_id": entity["id"],
            "date_from": date_from,
            "date_to": date_to,
        },
    ).mappings().all()

    by_match_type = session.execute(
        text(
            """
            SELECT m.match_type, COUNT(*) AS row_count, COALESCE(SUM(m.matched_amount), 0) AS matched_amount
            FROM bank_transaction_matches m
            JOIN bank_transactions bt ON bt.id = m.bank_transaction_id
            WHERE bt.entity_id = :entity_id
              AND bt.transaction_date BETWEEN :date_from AND :date_to
              AND m.active = TRUE
            GROUP BY m.match_type
            ORDER BY m.match_type
            """
        ),
        {
            "entity_id": entity["id"],
            "date_from": date_from,
            "date_to": date_to,
        },
    ).mappings().all()

    totals_payload = dict(totals or {})
    for key in (
        "total_amount",
    ):
        if totals_payload.get(key) is not None:
            totals_payload[key] = money_float(totals_payload.get(key))

    return {
        "entity_code": entity_code,
        "entity_name": entity.get("entity_name"),
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "totals": totals_payload,
        "summary_by_status": [
            {
                **dict(row),
                "total_amount": money_float(row.get("total_amount")),
            }
            for row in by_status
        ],
        "summary_by_account": [
            {
                **dict(row),
                "total_amount": money_float(row.get("total_amount")),
            }
            for row in by_account
        ],
        "summary_by_match_type": [
            {
                **dict(row),
                "matched_amount": money_float(row.get("matched_amount")),
            }
            for row in by_match_type
        ],
    }



def list_bank_review_transactions(
    session,
    entity_code: str,
    date_from: date,
    date_to: date,
    review_status: str | None = None,
    match_state: str | None = None,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    if review_status and review_status not in VALID_REVIEW_STATUSES:
        raise ValueError(f"Unsupported review_status: {review_status}")

    if match_state and match_state not in {"matched", "unmatched"}:
        raise ValueError("match_state must be 'matched' or 'unmatched'")

    sql = """
        SELECT
            bt.id,
            bt.entity_id,
            bt.source_system,
            bt.source_account_name,
            bt.source_account_code,
            bt.source_transaction_id,
            bt.source_transaction_type,
            bt.transaction_date,
            bt.posted_date,
            bt.description,
            bt.reference_number,
            bt.amount,
            bt.currency_code,
            bt.direction,
            bt.review_status,
            bt.review_note,
            bt.reviewed_by,
            bt.reviewed_at,
            bt.imported_at,
            bt.last_seen_at,
            bt.raw_json,
            m.id AS active_match_id,
            m.match_type AS active_match_type,
            m.target_table_name AS active_target_table_name,
            m.target_record_id AS active_target_record_id,
            m.matched_amount AS active_matched_amount,
            m.note AS active_match_note
        FROM bank_transactions bt
        LEFT JOIN bank_transaction_matches m
          ON m.bank_transaction_id = bt.id
         AND m.active = TRUE
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

    if match_state == "matched":
        sql += " AND m.id IS NOT NULL"
    elif match_state == "unmatched":
        sql += " AND m.id IS NULL"

    sql += " ORDER BY bt.transaction_date DESC, bt.imported_at DESC LIMIT 500"

    rows = session.execute(text(sql), params).mappings().all()
    serialized = [
        _serialize_bank_transaction_row(dict(row), include_raw_json=False)
        for row in rows
    ]

    return {
        "entity_code": entity_code,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "review_status": review_status,
        "match_state": match_state,
        "count": len(serialized),
        "transactions": serialized,
    }



def get_bank_transaction_detail(session, transaction_id: str) -> dict[str, Any]:
    row = _get_bank_transaction_row(session, transaction_id)
    if not row:
        raise ValueError("Bank transaction not found")

    matches = session.execute(
        text(
            """
            SELECT id, bank_transaction_id, entity_id, match_type, target_table_name, target_record_id,
                   matched_amount, note, active, created_by, created_at, released_by, released_at,
                   released_note, raw_json
            FROM bank_transaction_matches
            WHERE bank_transaction_id = :bank_transaction_id
            ORDER BY active DESC, created_at DESC
            """
        ),
        {"bank_transaction_id": transaction_id},
    ).mappings().all()

    history = session.execute(
        text(
            """
            SELECT id, bank_transaction_id, entity_id, action, actor_email,
                   from_review_status, to_review_status, note, payload_json, created_at
            FROM bank_transaction_review_events
            WHERE bank_transaction_id = :bank_transaction_id
            ORDER BY created_at DESC
            """
        ),
        {"bank_transaction_id": transaction_id},
    ).mappings().all()

    payload = _serialize_bank_transaction_row(dict(row), include_raw_json=True)

    return {
        "entity_code": row["entity_code"],
        "entity_name": row["entity_name"],
        "transaction": payload,
        "matches": [_serialize_match_row(dict(match)) for match in matches],
        "history": [_serialize_review_event_row(dict(event)) for event in history],
    }



def set_bank_transaction_review_status(
    session,
    transaction_id: str,
    actor_email: str,
    review_status: str,
    note: str | None = None,
) -> dict[str, Any]:
    normalized_status = normalize_text(review_status)
    if normalized_status not in VALID_REVIEW_STATUSES:
        raise ValueError(f"Unsupported review_status: {review_status}")

    actor = normalize_actor_email(actor_email)
    if not actor:
        raise ValueError("actor_email is required")

    row = _get_bank_transaction_row(session, transaction_id)
    if not row:
        raise ValueError("Bank transaction not found")

    active_match = session.execute(
        text(
            """
            SELECT id
            FROM bank_transaction_matches
            WHERE bank_transaction_id = :bank_transaction_id
              AND active = TRUE
            LIMIT 1
            """
        ),
        {"bank_transaction_id": transaction_id},
    ).mappings().first()

    if normalized_status == REVIEW_STATUS_MATCHED and not active_match:
        raise ValueError("Cannot set review_status to matched without an active bank transaction match")

    if normalized_status in {REVIEW_STATUS_NEW, REVIEW_STATUS_NEEDS_REVIEW, REVIEW_STATUS_IGNORED} and active_match:
        raise ValueError("This transaction has an active match. Unmatch it first before changing review_status away from matched")

    current_status = row.get("review_status") or REVIEW_STATUS_NEW
    cleaned_note = normalize_text(note)

    session.execute(
        text(
            """
            UPDATE bank_transactions
            SET review_status = :review_status,
                review_note = :review_note,
                reviewed_by = :reviewed_by,
                reviewed_at = NOW()
            WHERE id = :bank_transaction_id
            """
        ),
        {
            "bank_transaction_id": transaction_id,
            "review_status": normalized_status,
            "review_note": cleaned_note,
            "reviewed_by": actor,
        },
    )

    _insert_bank_review_event(
        session,
        bank_transaction_id=transaction_id,
        entity_id=str(row["entity_id"]),
        action="set_review_status",
        actor_email=actor,
        from_review_status=current_status,
        to_review_status=normalized_status,
        note=cleaned_note,
        payload_json={"active_match_exists": bool(active_match)},
    )

    return get_bank_transaction_detail(session, transaction_id)



def match_bank_transaction(
    session,
    transaction_id: str,
    actor_email: str,
    match_type: str,
    note: str | None = None,
    matched_amount: Any | None = None,
    target_table_name: str | None = None,
    target_record_id: str | None = None,
    raw_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_match_type = normalize_text(match_type)
    if normalized_match_type not in VALID_MATCH_TYPES:
        raise ValueError(f"Unsupported match_type: {match_type}")

    actor = normalize_actor_email(actor_email)
    if not actor:
        raise ValueError("actor_email is required")

    row = _get_bank_transaction_row(session, transaction_id)
    if not row:
        raise ValueError("Bank transaction not found")

    active_match = session.execute(
        text(
            """
            SELECT id
            FROM bank_transaction_matches
            WHERE bank_transaction_id = :bank_transaction_id
              AND active = TRUE
            LIMIT 1
            """
        ),
        {"bank_transaction_id": transaction_id},
    ).mappings().first()

    if active_match:
        raise ValueError("This bank transaction already has an active match. Unmatch it first before creating a new one")

    current_status = row.get("review_status") or REVIEW_STATUS_NEW
    cleaned_note = normalize_text(note)
    effective_amount = matched_amount if matched_amount is not None else row.get("amount")
    effective_amount = money_decimal(effective_amount)

    session.execute(
        text(
            """
            INSERT INTO bank_transaction_matches (
                bank_transaction_id,
                entity_id,
                match_type,
                target_table_name,
                target_record_id,
                matched_amount,
                note,
                active,
                created_by,
                raw_json
            ) VALUES (
                :bank_transaction_id,
                :entity_id,
                :match_type,
                :target_table_name,
                :target_record_id,
                :matched_amount,
                :note,
                TRUE,
                :created_by,
                CAST(:raw_json AS jsonb)
            )
            """
        ),
        {
            "bank_transaction_id": transaction_id,
            "entity_id": row["entity_id"],
            "match_type": normalized_match_type,
            "target_table_name": normalize_text(target_table_name),
            "target_record_id": normalize_text(target_record_id),
            "matched_amount": effective_amount,
            "note": cleaned_note,
            "created_by": actor,
            "raw_json": json.dumps(raw_json or {}, default=str),
        },
    )

    session.execute(
        text(
            """
            UPDATE bank_transactions
            SET review_status = :review_status,
                review_note = :review_note,
                reviewed_by = :reviewed_by,
                reviewed_at = NOW()
            WHERE id = :bank_transaction_id
            """
        ),
        {
            "bank_transaction_id": transaction_id,
            "review_status": REVIEW_STATUS_MATCHED,
            "review_note": cleaned_note,
            "reviewed_by": actor,
        },
    )

    _insert_bank_review_event(
        session,
        bank_transaction_id=transaction_id,
        entity_id=str(row["entity_id"]),
        action="match",
        actor_email=actor,
        from_review_status=current_status,
        to_review_status=REVIEW_STATUS_MATCHED,
        note=cleaned_note,
        payload_json={
            "match_type": normalized_match_type,
            "target_table_name": normalize_text(target_table_name),
            "target_record_id": normalize_text(target_record_id),
            "matched_amount": str(effective_amount),
        },
    )

    return get_bank_transaction_detail(session, transaction_id)



def unmatch_bank_transaction(
    session,
    transaction_id: str,
    actor_email: str,
    note: str | None = None,
) -> dict[str, Any]:
    actor = normalize_actor_email(actor_email)
    if not actor:
        raise ValueError("actor_email is required")

    row = _get_bank_transaction_row(session, transaction_id)
    if not row:
        raise ValueError("Bank transaction not found")

    active_match = session.execute(
        text(
            """
            SELECT id, match_type, target_table_name, target_record_id, matched_amount
            FROM bank_transaction_matches
            WHERE bank_transaction_id = :bank_transaction_id
              AND active = TRUE
            LIMIT 1
            """
        ),
        {"bank_transaction_id": transaction_id},
    ).mappings().first()

    if not active_match:
        raise ValueError("This bank transaction does not have an active match")

    current_status = row.get("review_status") or REVIEW_STATUS_NEW
    cleaned_note = normalize_text(note)

    session.execute(
        text(
            """
            UPDATE bank_transaction_matches
            SET active = FALSE,
                released_by = :released_by,
                released_at = NOW(),
                released_note = :released_note
            WHERE id = :match_id
            """
        ),
        {
            "match_id": active_match["id"],
            "released_by": actor,
            "released_note": cleaned_note,
        },
    )

    session.execute(
        text(
            """
            UPDATE bank_transactions
            SET review_status = :review_status,
                review_note = :review_note,
                reviewed_by = :reviewed_by,
                reviewed_at = NOW()
            WHERE id = :bank_transaction_id
            """
        ),
        {
            "bank_transaction_id": transaction_id,
            "review_status": REVIEW_STATUS_NEEDS_REVIEW,
            "review_note": cleaned_note,
            "reviewed_by": actor,
        },
    )

    _insert_bank_review_event(
        session,
        bank_transaction_id=transaction_id,
        entity_id=str(row["entity_id"]),
        action="unmatch",
        actor_email=actor,
        from_review_status=current_status,
        to_review_status=REVIEW_STATUS_NEEDS_REVIEW,
        note=cleaned_note,
        payload_json={
            "match_id": str(active_match["id"]),
            "match_type": active_match.get("match_type"),
            "target_table_name": active_match.get("target_table_name"),
            "target_record_id": active_match.get("target_record_id"),
            "matched_amount": str(active_match.get("matched_amount")),
        },
    )

    return get_bank_transaction_detail(session, transaction_id)
