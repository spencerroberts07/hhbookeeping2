import base64
import calendar
import logging
import secrets
from datetime import date as DateType, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable
from urllib.parse import urlencode

import httpx
from sqlalchemy import text

from .config import settings

logger = logging.getLogger(__name__)

BANK_ACCOUNT_TYPES = {
    "Bank",
    "CashOnHand",
    "CreditCard",
    "OtherCurrentAsset",
}


class QuickBooksClient:
    def __init__(self) -> None:
        self.auth_url = settings.qbo_auth_url
        self.token_url = settings.qbo_token_url
        self.api_base_url = settings.qbo_base_url.rstrip("/")
        self.scope = settings.qbo_scope
        self.redirect_uri = settings.qbo_redirect_uri
        self.minor_version = settings.qbo_minor_version

    @staticmethod
    def new_state() -> str:
        return secrets.token_urlsafe(24)

    def build_authorization_url(self, state: str) -> str:
        query = urlencode(
            {
                "client_id": settings.qbo_client_id,
                "response_type": "code",
                "scope": self.scope,
                "redirect_uri": self.redirect_uri,
                "state": state,
            }
        )
        return f"{self.auth_url}?{query}"

    def _basic_auth_header(self) -> str:
        raw = f"{settings.qbo_client_id}:{settings.qbo_client_secret}".encode("utf-8")
        return "Basic " + base64.b64encode(raw).decode("utf-8")

    async def exchange_code(self, code: str) -> dict[str, Any]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": self._basic_auth_header(),
        }
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(self.token_url, headers=headers, data=data)
            response.raise_for_status()
            return response.json()

    async def refresh_access_token(self, refresh_token: str) -> dict[str, Any]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": self._basic_auth_header(),
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(self.token_url, headers=headers, data=data)
            response.raise_for_status()
            return response.json()

    async def get_company_info(self, realm_id: str, access_token: str) -> dict[str, Any]:
        url = f"{self.api_base_url}/v3/company/{realm_id}/companyinfo/{realm_id}"
        params = {"minorversion": self.minor_version}
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()

    async def query(self, realm_id: str, access_token: str, query: str) -> dict[str, Any]:
        url = f"{self.api_base_url}/v3/company/{realm_id}/query"
        params = {"minorversion": self.minor_version}
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/text",
        }
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(url, headers=headers, params=params, content=query)
            response.raise_for_status()
            return response.json()

    async def query_all(
        self,
        realm_id: str,
        access_token: str,
        base_query: str,
        object_name: str,
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        url = f"{self.api_base_url}/v3/company/{realm_id}/query"
        params = {"minorversion": self.minor_version}
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/text",
        }

        all_rows: list[dict[str, Any]] = []
        start_pos = 1

        async with httpx.AsyncClient(timeout=120) as client:
            while True:
                query = f"{base_query} STARTPOSITION {start_pos} MAXRESULTS {page_size}"
                response = await client.post(url, headers=headers, params=params, content=query)
                response.raise_for_status()

                data = response.json()
                query_response = data.get("QueryResponse", {})
                rows = query_response.get(object_name, []) or []

                if isinstance(rows, dict):
                    rows = [rows]

                if not rows:
                    break

                all_rows.extend(rows)

                if len(rows) < page_size:
                    break

                start_pos += page_size

        return all_rows

    async def cdc(
        self,
        realm_id: str,
        access_token: str,
        changed_since_iso: str,
        entities: list[str],
    ) -> dict[str, Any]:
        url = f"{self.api_base_url}/v3/company/{realm_id}/cdc"
        params = {
            "changedSince": changed_since_iso,
            "entities": ",".join(entities),
            "minorversion": self.minor_version,
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()

    async def get_account_balances(
        self,
        realm_id: str,
        access_token: str,
    ) -> list[dict[str, Any]]:
        """Pull every active bank-type account with its current balance
        directly from QBO's Account object. Used by the dashboard cash
        card to show a live number instead of last night's
        cash_balancing_days snapshot.

        Returns rows shaped:
            {
              "account_id": str,
              "account_name": str,
              "account_code": str,         # AcctNum (dealer-set number)
              "account_type": str,         # always 'Bank' for this caller
              "account_subtype": str,
              "current_balance": Decimal,
              "currency": str,
            }
        """
        rows = await self.query_all(
            realm_id=realm_id,
            access_token=access_token,
            base_query=(
                "SELECT * FROM Account "
                "WHERE AccountType = 'Bank' AND Active = true"
            ),
            object_name="Account",
        )
        out: list[dict[str, Any]] = []
        for r in rows:
            currency_ref = r.get("CurrencyRef") or {}
            currency = (
                currency_ref.get("value")
                if isinstance(currency_ref, dict)
                else None
            ) or "CAD"
            out.append({
                "account_id": str(r.get("Id") or ""),
                "account_name": (r.get("Name") or "").strip() or "Unnamed",
                "account_code": (r.get("AcctNum") or r.get("Id") or "").strip(),
                "account_type": r.get("AccountType") or "Bank",
                "account_subtype": r.get("AccountSubType") or "",
                "current_balance": _decimal(r.get("CurrentBalance")),
                "currency": currency,
            })
        return out

    # ----------------------------------------------------------------------
    # Report endpoints
    # ----------------------------------------------------------------------
    async def get_report(
        self,
        realm_id: str,
        access_token: str,
        report_name: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Generic QBO report fetcher. Hits
        /v3/company/{realm}/reports/{report_name}. Returns raw JSON.

        Caller is responsible for token freshness (use
        ensure_valid_access_token() before calling).
        """
        url = f"{self.api_base_url}/v3/company/{realm_id}/reports/{report_name}"
        query: dict[str, Any] = {"minorversion": self.minor_version}
        if params:
            # QBO accepts most report params as query-string values. Skip
            # None and stringify dates/decimals so httpx serializes cleanly.
            for k, v in params.items():
                if v is None:
                    continue
                if isinstance(v, (DateType, datetime)):
                    query[k] = v.isoformat()[:10]
                else:
                    query[k] = str(v)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.get(url, headers=headers, params=query)
            response.raise_for_status()
            return response.json()

    async def get_trial_balance(
        self,
        realm_id: str,
        access_token: str,
        as_of_date: DateType,
    ) -> list[dict[str, Any]]:
        """Pull TrialBalance and parse into a flat list of account rows.

        Returns rows shaped:
            {
              "account_id": str,
              "account_name": str,
              "account_type": str,    # derived from prefix when QBO omits
              "account_subtype": str, # may be empty
              "debit_balance": Decimal,
              "credit_balance": Decimal,
              "net_balance": Decimal, # debit - credit
            }
        """
        payload = await self.get_report(
            realm_id,
            access_token,
            "TrialBalance",
            {
                "date_macro": "custom",
                "start_date": as_of_date,
                "end_date": as_of_date,
            },
        )
        return _parse_trial_balance(payload)

    async def get_general_ledger(
        self,
        realm_id: str,
        access_token: str,
        date_from: DateType,
        date_to: DateType,
        account_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Pull GeneralLedger for a date range and flatten into journal-line
        rows. QBO's GL report is structured by account section; this
        helper traverses the section tree and emits one row per line.

        QBO's GL report can be slow and large; for multi-year imports the
        caller should chunk by month and accumulate.

        Returns rows shaped:
            {
              "transaction_date": date,
              "transaction_type": str,
              "reference_number": str,
              "account_id": str,
              "account_name": str,
              "description": str,
              "debit_amount": Decimal,
              "credit_amount": Decimal,
              "running_balance": Decimal,
            }
        """
        params: dict[str, Any] = {
            "start_date": date_from,
            "end_date": date_to,
            "columns": "tx_date,txn_type,doc_num,name,memo,subt_nat_amount,rbal_nat_amount",
        }
        if account_id:
            params["account"] = account_id

        payload = await self.get_report(
            realm_id,
            access_token,
            "GeneralLedger",
            params,
        )
        return _parse_general_ledger(payload)


# --------------------------------------------------------------------------
# Report parsers
# --------------------------------------------------------------------------


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _iter_rows(node: Any) -> Iterable[dict[str, Any]]:
    """Walk the recursive QBO Rows / Row tree. Yields each individual Row
    dict (both data rows and section headers — caller decides which to keep).
    """
    if not isinstance(node, dict):
        return
    rows_block = node.get("Rows") or {}
    for row in rows_block.get("Row", []) or []:
        yield row
        # Sections nest further rows under their own Rows key.
        if row.get("type") == "Section":
            yield from _iter_rows(row)


def _account_type_from_code(code: str) -> str:
    """Match the prefix convention used elsewhere in the codebase."""
    p = (code or "").strip()[:1]
    return {
        "1": "Asset",
        "2": "Liability",
        "3": "Equity",
        "4": "Revenue",
        "5": "COGS",
        "6": "Expense",
        "7": "Expense",
        "8": "Expense",
        "9": "Expense",
    }.get(p, "Other")


def _parse_trial_balance(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Walk a QBO TrialBalance JSON response and yield one row per account.

    QBO TB rows usually carry: account_id, account_name, debit, credit.
    The report layout uses ColData with positional columns. Column count
    can vary (some realms include a sub-account column); we defensively
    pull the last two numeric columns as debit/credit.
    """
    out: list[dict[str, Any]] = []
    for row in _iter_rows(payload):
        # Sections (header / totals) have no ColData; data rows do.
        if row.get("type") == "Section":
            continue
        col_data = row.get("ColData") or []
        if not col_data:
            continue
        # First column is always the account label. id may be present on
        # the same column under "id". Defensive: scan for the first
        # column with a non-empty value.
        first = col_data[0]
        account_name = (first.get("value") or "").strip()
        account_id = str(first.get("id") or "").strip() or account_name
        if not account_name:
            continue

        # The last two columns are debit / credit. Pull them positionally
        # — QBO omits a value when it's zero.
        numeric_values: list[Decimal] = []
        for col in col_data[1:]:
            raw = (col.get("value") or "").strip()
            if raw == "":
                numeric_values.append(Decimal("0"))
            else:
                numeric_values.append(_decimal(raw))

        if len(numeric_values) >= 2:
            debit_balance = numeric_values[-2]
            credit_balance = numeric_values[-1]
        elif len(numeric_values) == 1:
            # Some TB report layouts collapse to a single signed column
            # ("net debit"). Positive → debit, negative → credit.
            net = numeric_values[0]
            if net >= 0:
                debit_balance, credit_balance = net, Decimal("0")
            else:
                debit_balance, credit_balance = Decimal("0"), -net
        else:
            continue

        if debit_balance == 0 and credit_balance == 0:
            continue

        out.append({
            "account_id": account_id,
            "account_name": account_name,
            "account_type": _account_type_from_code(account_id),
            "account_subtype": "",
            "debit_balance": debit_balance,
            "credit_balance": credit_balance,
            "net_balance": debit_balance - credit_balance,
        })
    return out


def _parse_general_ledger(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Walk a GL report. Each Section is an account; rows under it are
    transaction lines for that account.

    Returns lines flattened — every line carries its parent account.
    """
    out: list[dict[str, Any]] = []
    rows_block = (payload.get("Rows") or {}).get("Row", []) or []
    for section in rows_block:
        if section.get("type") != "Section":
            continue
        # The section header is usually a single ColData with the account
        # name; the account id is on the Header's ColData[0].id.
        header = section.get("Header") or {}
        header_cols = header.get("ColData") or []
        if not header_cols:
            continue
        account_name = (header_cols[0].get("value") or "").strip()
        account_id = str(header_cols[0].get("id") or "").strip() or account_name

        for row in (section.get("Rows") or {}).get("Row", []) or []:
            if row.get("type") == "Section":
                # GL sometimes nests sub-account sections — skip nesting,
                # _iter_rows would double-emit.
                continue
            cols = row.get("ColData") or []
            if not cols:
                continue
            # columns: tx_date, txn_type, doc_num, name, memo,
            # subt_nat_amount, rbal_nat_amount
            txn_date_str = (cols[0].get("value") if len(cols) > 0 else "") or ""
            try:
                txn_date = datetime.strptime(txn_date_str, "%Y-%m-%d").date()
            except ValueError:
                # Beginning-of-period totals etc.
                continue
            txn_type = (cols[1].get("value") if len(cols) > 1 else "") or ""
            doc_num = (cols[2].get("value") if len(cols) > 2 else "") or ""
            name = (cols[3].get("value") if len(cols) > 3 else "") or ""
            memo = (cols[4].get("value") if len(cols) > 4 else "") or ""
            subt_raw = (cols[5].get("value") if len(cols) > 5 else "") or ""
            rbal_raw = (cols[6].get("value") if len(cols) > 6 else "") or ""

            subt = _decimal(subt_raw)
            running = _decimal(rbal_raw)
            if subt >= 0:
                debit_amount, credit_amount = subt, Decimal("0")
            else:
                debit_amount, credit_amount = Decimal("0"), -subt

            # Description: prefer memo; fall back to name; fall back to
            # txn_type so something always renders.
            description = memo.strip() or name.strip() or txn_type

            out.append({
                "transaction_date": txn_date,
                "transaction_type": txn_type,
                "reference_number": doc_num,
                "account_id": account_id,
                "account_name": account_name,
                "description": description,
                "counterparty_name": name.strip() or None,
                "debit_amount": debit_amount,
                "credit_amount": credit_amount,
                "running_balance": running,
            })
    return out


def month_chunks(date_from: DateType, date_to: DateType) -> list[tuple[DateType, DateType]]:
    """Yield (month_start, month_end) tuples spanning the date range.

    Used by the GL importer so we can pull a month at a time, log
    progress, and avoid timeouts on multi-year ranges.
    """
    if date_to < date_from:
        return []
    chunks: list[tuple[DateType, DateType]] = []
    cursor = DateType(date_from.year, date_from.month, 1)
    while cursor <= date_to:
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        month_end = DateType(cursor.year, cursor.month, last_day)
        start = max(cursor, date_from)
        end = min(month_end, date_to)
        chunks.append((start, end))
        # Advance to first of next month.
        if cursor.month == 12:
            cursor = DateType(cursor.year + 1, 1, 1)
        else:
            cursor = DateType(cursor.year, cursor.month + 1, 1)
    return chunks


def token_expiry_from_seconds(seconds: int | None) -> datetime | None:
    if not seconds:
        return None
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def upsert_connection(session, entity_id: str, realm_id: str, token_payload: dict[str, Any]) -> None:
    session.execute(
        text(
            """
            INSERT INTO quickbooks_connections (
                entity_id, realm_id, access_token, refresh_token,
                access_token_expires_at, refresh_token_expires_at, is_active
            )
            VALUES (
                :entity_id, :realm_id, :access_token, :refresh_token,
                :access_expiry, :refresh_expiry, TRUE
            )
            ON CONFLICT (entity_id, realm_id)
            DO UPDATE SET
                access_token = EXCLUDED.access_token,
                refresh_token = EXCLUDED.refresh_token,
                access_token_expires_at = EXCLUDED.access_token_expires_at,
                refresh_token_expires_at = EXCLUDED.refresh_token_expires_at,
                disconnected_at = NULL,
                is_active = TRUE
            """
        ),
        {
            "entity_id": entity_id,
            "realm_id": realm_id,
            "access_token": token_payload["access_token"],
            "refresh_token": token_payload["refresh_token"],
            "access_expiry": token_expiry_from_seconds(token_payload.get("expires_in")),
            "refresh_expiry": token_expiry_from_seconds(token_payload.get("x_refresh_token_expires_in")),
        },
    )


async def ensure_valid_access_token(session, connection: dict[str, Any]) -> dict[str, Any]:
    """
    Refresh the QuickBooks access token if expired or close to expiry.
    Returns an updated connection mapping.
    """
    expires_at = connection.get("access_token_expires_at")
    now_utc = datetime.now(timezone.utc)
    refresh_needed = False

    if not expires_at:
        refresh_needed = True
    else:
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= now_utc + timedelta(minutes=5):
            refresh_needed = True

    if not refresh_needed:
        return connection

    refresh_token = connection.get("refresh_token")
    if not refresh_token:
        raise ValueError("QuickBooks refresh token is missing")

    qb = QuickBooksClient()
    token_payload = await qb.refresh_access_token(refresh_token)

    session.execute(
        text(
            """
            UPDATE quickbooks_connections
            SET access_token = :access_token,
                refresh_token = :refresh_token,
                access_token_expires_at = :access_expiry,
                refresh_token_expires_at = :refresh_expiry,
                disconnected_at = NULL,
                is_active = TRUE
            WHERE id = :id
            """
        ),
        {
            "id": connection["id"],
            "access_token": token_payload["access_token"],
            "refresh_token": token_payload.get("refresh_token", refresh_token),
            "access_expiry": token_expiry_from_seconds(token_payload.get("expires_in")),
            "refresh_expiry": token_expiry_from_seconds(
                token_payload.get("x_refresh_token_expires_in")
            ),
        },
    )

    refreshed = dict(connection)
    refreshed["access_token"] = token_payload["access_token"]
    refreshed["refresh_token"] = token_payload.get("refresh_token", refresh_token)
    refreshed["access_token_expires_at"] = token_expiry_from_seconds(token_payload.get("expires_in"))
    refreshed["refresh_token_expires_at"] = token_expiry_from_seconds(
        token_payload.get("x_refresh_token_expires_in")
    )
    return refreshed
