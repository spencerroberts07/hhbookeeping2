import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote

import httpx
import jwt

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_SHEETS_BASE_URL = "https://sheets.googleapis.com/v4/spreadsheets"
GOOGLE_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"


@dataclass
class NormalizedCashRow:
    source_tab_name: str
    business_date: str | None
    row_number: int
    row_key: str
    row_hash: str
    notes: str | None
    sales_amount: float | None
    cash_amount: float | None
    debit_amount: float | None
    credit_amount: float | None
    ecommerce_amount: float | None
    gift_card_amount: float | None
    hst_amount: float | None
    over_short_amount: float | None
    raw_row_json: dict[str, Any]

class GoogleSheetsClient:
    def __init__(self, service_account_email: str, private_key: str) -> None:
        self.service_account_email = service_account_email
        self.private_key = private_key.replace("\\n", "\n")

    def _build_jwt(self) -> str:
        now = int(datetime.utcnow().timestamp())
        payload = {
            "iss": self.service_account_email,
            "scope": GOOGLE_SHEETS_SCOPE,
            "aud": GOOGLE_TOKEN_URL,
            "iat": now,
            "exp": now + 3600,
        }
        return jwt.encode(payload, self.private_key, algorithm="RS256")

    async def _get_access_token(self) -> str:
        assertion = self._build_jwt()
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(GOOGLE_TOKEN_URL, data=data)
            response.raise_for_status()
            return response.json()["access_token"]

    @staticmethod
    def normalize_sheet_range(tab_name: str) -> str:
        if "!" in tab_name:
            return tab_name

        escaped = tab_name.replace("'", "''")
        return f"'{escaped}'!A:ZZ"

    async def get_tab_values(self, spreadsheet_id: str, tab_name: str) -> list[list[str]]:
        access_token = await self._get_access_token()

        sheet_range = self.normalize_sheet_range(tab_name)
        encoded_range = quote(sheet_range, safe="!:")

        url = f"{GOOGLE_SHEETS_BASE_URL}/{spreadsheet_id}/values/{encoded_range}"

        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        params = {
            "majorDimension": "ROWS",
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=headers, params=params)

            print("GOOGLE SHEETS URL:", str(response.request.url))
            print("GOOGLE SHEETS STATUS:", response.status_code)
            print("GOOGLE SHEETS BODY:", response.text)

            if response.is_error:
                raise RuntimeError(
                    f"Google Sheets API error. "
                    f"URL={response.request.url} "
                    f"STATUS={response.status_code} "
                    f"BODY={response.text}"
                )

            payload = response.json()

        return payload.get("values", [])

def safe_decimal(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text == "":
        return None
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    try:
        return float(text)
    except ValueError:
        return None


def guess_date(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d", "%d-%b-%y", "%d-%b-%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None
@dataclass
class DailyCashLine:
    source_tab_name: str
    business_date: str
    line_label: str
    amount: float | None
    account_code: str | None
    day_name: str | None

def parse_weekly_cash_sheet(tab_name: str, rows: list[list[str]]) -> list[DailyCashLine]:
    if not rows or len(rows) < 6:
        return []

    date_row = rows[3] if len(rows) > 3 else []
    day_row = rows[4] if len(rows) > 4 else []

    daily_lines: list[DailyCashLine] = []

    # dates are across columns 2 to 8 in your current sheet layout
    for row in rows[5:]:
        label = str(row[0]).strip() if len(row) > 0 and row[0] is not None else ""

        if label in {"", "PAID OUTS", "Other info", "Total", "Total PAID OUTS"}:
            continue

        account_code = str(row[10]).strip() if len(row) > 10 and row[10] is not None else None

        for col_index in range(1, 8):
            if col_index >= len(date_row):
                continue

            raw_date = date_row[col_index] if col_index < len(date_row) else None
            raw_day = day_row[col_index] if col_index < len(day_row) else None
            raw_amount = row[col_index] if col_index < len(row) else None

            business_date = guess_date(raw_date)
            amount = safe_decimal(raw_amount)

            if business_date is None:
                continue

            daily_lines.append(
                DailyCashLine(
                    source_tab_name=tab_name,
                    business_date=business_date,
                    line_label=label,
                    amount=amount,
                    account_code=account_code,
                    day_name=str(raw_day).strip() if raw_day else None,
                )
            )

    return daily_lines

def normalize_cash_balancing_rows(tab_name: str, rows: list[list[str]]) -> list[NormalizedCashRow]:
    if not rows:
        return []

    header = [str(cell).strip().lower() for cell in rows[0]]
    normalized: list[NormalizedCashRow] = []

    def find_index(*candidates: str) -> int | None:
        for candidate in candidates:
            if candidate in header:
                return header.index(candidate)
        return None

    idx_date = find_index("date", "business date")
    idx_notes = find_index("notes", "memo", "comment")
    idx_sales = find_index("sales", "net sales", "sales amount")
    idx_cash = find_index("cash", "cash sales")
    idx_debit = find_index("debit", "debit amount")
    idx_credit = find_index("credit", "credit amount")
    idx_ecom = find_index("ecom", "e-commerce", "ecommerce")
    idx_gift = find_index("gift card", "gift cards")
    idx_hst = find_index("hst", "tax", "tax amount")
    idx_over_short = find_index("over short", "cash over short")

    for row_number, row in enumerate(rows[1:], start=2):
        raw = {f"col_{i+1}": row[i] if i < len(row) else None for i in range(max(len(header), len(row)))}
        business_date = guess_date(row[idx_date]) if idx_date is not None and idx_date < len(row) else None
        notes = row[idx_notes] if idx_notes is not None and idx_notes < len(row) else None
        row_key = f"{tab_name}|{business_date or 'no-date'}|{row_number}"
        row_hash = hashlib.sha256(json.dumps(row, ensure_ascii=False).encode("utf-8")).hexdigest()

        normalized.append(
            NormalizedCashRow(
                source_tab_name=tab_name,
                business_date=business_date,
                row_number=row_number,
                row_key=row_key,
                row_hash=row_hash,
                notes=notes,
                sales_amount=safe_decimal(row[idx_sales]) if idx_sales is not None and idx_sales < len(row) else None,
                cash_amount=safe_decimal(row[idx_cash]) if idx_cash is not None and idx_cash < len(row) else None,
                debit_amount=safe_decimal(row[idx_debit]) if idx_debit is not None and idx_debit < len(row) else None,
                credit_amount=safe_decimal(row[idx_credit]) if idx_credit is not None and idx_credit < len(row) else None,
                ecommerce_amount=safe_decimal(row[idx_ecom]) if idx_ecom is not None and idx_ecom < len(row) else None,
                gift_card_amount=safe_decimal(row[idx_gift]) if idx_gift is not None and idx_gift < len(row) else None,
                hst_amount=safe_decimal(row[idx_hst]) if idx_hst is not None and idx_hst < len(row) else None,
                over_short_amount=safe_decimal(row[idx_over_short]) if idx_over_short is not None and idx_over_short < len(row) else None,
                raw_row_json=raw,
            )
        )

    return normalized
