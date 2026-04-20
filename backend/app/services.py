import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from .quickbooks import (
    BANK_ACCOUNT_TYPES,
    QuickBooksClient,
    ensure_valid_access_token,
    upsert_connection,
)


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

    counterparty = None
    for ref_key in ("VendorRef", "CustomerRef", "EntityRef"):
        ref = record.get(ref_key)
        if isinstance(ref, dict):
            counterparty = ref.get("name")
            if counterparty:
                break

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

        description = private_note or doc_number or counterparty or txn_type

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
        description = line.get("Description") or private_note or doc_number or txn_type

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
        "Check",
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
