from datetime import datetime, time, timezone
from typing import Any

from sqlalchemy import text

from .quickbooks import QuickBooksClient, upsert_connection


def get_entity_by_code(session, entity_code: str):
    return session.execute(
        text("SELECT id, entity_code, entity_name, quickbooks_company_id FROM entities WHERE entity_code = :entity_code"),
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


async def import_chart_of_accounts(session, entity_code: str) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    connection = get_active_connection(session, entity["id"])
    if not connection:
        raise ValueError("QuickBooks connection not found for entity")

    qb = QuickBooksClient()
    payload = await qb.query(
        realm_id=connection["realm_id"],
        access_token=connection["access_token"],
        query="select * from Account maxresults 1000",
    )
    accounts = payload.get("QueryResponse", {}).get("Account", [])

    imported = 0
    for acc in accounts:
        code = acc.get("AcctNum") or acc.get("Id")
        name = acc.get("Name") or "Unnamed"
        classification = acc.get("Classification") or "Unclassified"
        account_type = acc.get("AccountType") or classification
        statement_type = "balance_sheet" if classification in {"Asset", "Liability", "Equity"} else "income_statement"
        session.execute(
            text(
                """
                INSERT INTO accounts (entity_id, account_code, account_name, account_class, statement_type, quickbooks_account_id)
                VALUES (:entity_id, :account_code, :account_name, :account_class, :statement_type, :quickbooks_account_id)
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

    return {"imported_count": imported, "realm_id": connection["realm_id"]}


async def import_transactions_cdc(session, entity_code: str, date_from, date_to) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    connection = get_active_connection(session, entity["id"])
    if not connection:
        raise ValueError("QuickBooks connection not found for entity")

    qb = QuickBooksClient()
    changed_since = datetime.combine(date_from, time.min, tzinfo=timezone.utc).isoformat()
    payload = await qb.cdc(
        realm_id=connection["realm_id"],
        access_token=connection["access_token"],
        changed_since_iso=changed_since,
        entities=["JournalEntry", "Bill", "BillPayment", "Deposit", "Purchase", "SalesReceipt", "Invoice", "Payment"],
    )

    imported = 0
    entity_nodes = payload.get("CDCResponse", [{}])[0].get("QueryResponse", [])
    for bucket in entity_nodes:
        for txn_type, records in bucket.items():
            if not isinstance(records, list):
                continue
            for record in records:
                txn_date = record.get("TxnDate") or record.get("MetaData", {}).get("LastUpdatedTime", "")[:10] or None
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
                        ) VALUES (
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
