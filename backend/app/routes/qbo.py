"""
Live QBO reads — currently bank-account balances for the dashboard
cash card. Distinct from /api/qbo-bank-sync (which imports bank
transactions) and /api/sync (which imports the chart-of-accounts).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from ..db import db_session
from ..quickbooks import QuickBooksClient, ensure_valid_access_token
from ..services import get_active_connection, get_entity_by_code
from ..services_auth import require_role

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/qbo", tags=["qbo"])


@router.get("/bank-balances")
async def bank_balances(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Live bank balances pulled from QuickBooks for the dashboard
    cash card. Returns an empty `accounts` list when the entity has
    no active QBO connection — frontend renders the "Connect
    QuickBooks" prompt off that signal.
    """
    try:
        with db_session() as session:
            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {entity_code}")

            connection = get_active_connection(session, entity["id"])
            if not connection:
                # Distinct from "balances are zero" — frontend should
                # show the connect-QBO CTA.
                return {
                    "entity_code": entity_code,
                    "connected": False,
                    "accounts": [],
                    "total_balance": 0.0,
                    "currency": "CAD",
                    "fetched_at": None,
                }

            connection = await ensure_valid_access_token(session, dict(connection))
            qb = QuickBooksClient()
            try:
                rows = await qb.get_account_balances(
                    connection["realm_id"],
                    connection["access_token"],
                )
            except Exception as exc:
                # Token expired beyond refresh, network failure, etc.
                # Returning connected=False makes the frontend offer
                # re-connection rather than showing a fake zero.
                logger.warning(
                    "QBO bank-balances pull failed for %s: %r",
                    entity_code,
                    exc,
                )
                return {
                    "entity_code": entity_code,
                    "connected": False,
                    "accounts": [],
                    "total_balance": 0.0,
                    "currency": "CAD",
                    "fetched_at": None,
                    "error": str(exc)[:200],
                }

        # Sum + normalize. QBO returns Decimal-shaped numbers via the
        # parser; convert to float for the JSON response.
        total = sum((r["current_balance"] for r in rows), start=0)
        # Best-effort currency from the first account; QBO realms are
        # typically single-currency for the dealers we serve.
        currency = rows[0]["currency"] if rows else "CAD"
        # account_subtype lets the frontend categorize each account
        # (Checking vs Savings vs CreditLine/LineOfCredit). QBO uses
        # CamelCase values like "Checking" / "Savings" / "CreditCard" /
        # "LineOfCredit".
        accounts = [
            {
                "account_name": r["account_name"],
                "account_code": r["account_code"],
                "account_subtype": r["account_subtype"],
                "current_balance": float(r["current_balance"]),
                "currency": r["currency"],
            }
            for r in rows
        ]
        return {
            "entity_code": entity_code,
            "connected": True,
            "accounts": accounts,
            "total_balance": float(total),
            "currency": currency,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("bank_balances unexpected error")
        raise HTTPException(400, str(exc)) from exc
