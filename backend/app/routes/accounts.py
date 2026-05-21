"""
Chart of accounts CRUD — app-native admin endpoints.

The legacy `accounts` table was populated by import_chart_of_accounts
(QBO sync) and the new save_chart_of_accounts (onboarding upload).
This route exposes the same rows through GET/POST and seeds a default
HH-dealer chart from existing journal_lines when an entity has no
rows yet, so /settings/accounts is never blank.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import db_session
from ..services import get_entity_by_code
from ..services_auth import enforce_entity_code, require_role

router = APIRouter(prefix="/api/accounts", tags=["accounts"])


_ACCOUNT_CLASSES = {
    "Asset", "Liability", "Equity", "Revenue", "Expense", "COGS", "Other",
}


def _type_from_code(code: str) -> str:
    p = (code or "").strip()[:1]
    return {
        "1": "Asset", "2": "Liability", "3": "Equity",
        "4": "Revenue", "5": "COGS",
        "6": "Expense", "7": "Expense", "8": "Expense", "9": "Expense",
    }.get(p, "Other")


def _normal_balance_from_type(type_str: str) -> str:
    return "debit" if type_str in {"Asset", "Expense", "COGS"} else "credit"


def _statement_type_from_class(account_class: str) -> str:
    return (
        "balance_sheet"
        if account_class in {"Asset", "Liability", "Equity"}
        else "income_statement"
    )


@router.get("")
def list_accounts(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Return all accounts for the entity. When none exist, seed from
    distinct account_codes already used in journal_lines so the dealer
    sees their working chart instead of an empty list.
    """
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")

        rows = session.execute(
            text(
                """
                SELECT account_code, account_name, account_class,
                       statement_type, is_active
                  FROM accounts
                 WHERE entity_id = :eid
                 ORDER BY account_code
                """
            ),
            {"eid": entity["id"]},
        ).mappings().all()

        if not rows:
            # Bootstrap from journal_lines so the page is useful even
            # before a real chart import runs.
            seen = session.execute(
                text(
                    """
                    SELECT DISTINCT jl.account_code
                      FROM journal_lines jl
                      JOIN journal_batches jb ON jb.id = jl.journal_batch_id
                     WHERE jb.entity_id = :eid
                     ORDER BY jl.account_code
                    """
                ),
                {"eid": entity["id"]},
            ).mappings().all()
            accounts = [
                {
                    "code": r["account_code"],
                    "name": r["account_code"],
                    "type": _type_from_code(r["account_code"]),
                    "normal_balance": _normal_balance_from_type(
                        _type_from_code(r["account_code"])
                    ),
                    "is_active": True,
                    "source": "journal_lines_seed",
                }
                for r in seen
            ]
            return {
                "entity_code": entity_code,
                "accounts": accounts,
                "count": len(accounts),
                "seeded_from": "journal_lines" if accounts else "empty",
            }

        accounts = [
            {
                "code": r["account_code"],
                "name": r["account_name"],
                "type": r["account_class"],
                "normal_balance": _normal_balance_from_type(r["account_class"]),
                "is_active": r["is_active"],
                "source": "accounts_table",
            }
            for r in rows
        ]
        return {
            "entity_code": entity_code,
            "accounts": accounts,
            "count": len(accounts),
            "seeded_from": "accounts_table",
        }


class UpsertAccountRequest(BaseModel):
    entity_code: str
    account_code: str = Field(min_length=1, max_length=32)
    account_name: str = Field(min_length=1, max_length=200)
    account_type: str = Field(
        description="Asset|Liability|Equity|Revenue|Expense|COGS|Other",
    )
    normal_balance: str | None = Field(
        default=None, description="debit or credit; inferred from type when None"
    )
    parent_code: str | None = None


@router.post("")
def upsert_account(
    body: UpsertAccountRequest,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    account_class = body.account_type.strip().title()
    if account_class not in _ACCOUNT_CLASSES:
        # Tolerate close variants ("Liabilities" → "Liability").
        if account_class == "Liabilities":
            account_class = "Liability"
        elif account_class == "Assets":
            account_class = "Asset"
        elif account_class == "Expenses":
            account_class = "Expense"
        else:
            raise HTTPException(
                400, f"account_type must be one of {sorted(_ACCOUNT_CLASSES)}"
            )

    with db_session() as session:
        entity = get_entity_by_code(session, body.entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity: {body.entity_code}")

        session.execute(
            text(
                """
                INSERT INTO accounts (
                    entity_id, account_code, account_name, account_class,
                    statement_type, is_active
                ) VALUES (
                    :eid, :code, :name, :class, :stmt, TRUE
                )
                ON CONFLICT (entity_id, account_code) DO UPDATE
                   SET account_name  = EXCLUDED.account_name,
                       account_class = EXCLUDED.account_class,
                       statement_type = EXCLUDED.statement_type,
                       is_active      = TRUE
                """
            ),
            {
                "eid": entity["id"],
                "code": body.account_code,
                "name": body.account_name,
                "class": account_class,
                "stmt": _statement_type_from_class(account_class),
            },
        )
        return {
            "entity_code": body.entity_code,
            "code": body.account_code,
            "name": body.account_name,
            "type": account_class,
            "normal_balance": body.normal_balance
                or _normal_balance_from_type(account_class),
        }
