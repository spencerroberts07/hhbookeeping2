import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import httpx
import jwt

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_SHEETS_BASE_URL = "https://sheets.googleapis.com/v4/spreadsheets"
GOOGLE_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"

# Lines that should never become daily bookkeeping lines
EXCLUDED_DAILY_LABELS = {
    "",
    "Opening Cash",
    "Total",
    "Total PAID OUTS",
    "Cash to Account for",
    "Actual Cash count",
    "Closing Cash",
    "Weather",
    "Customer count",
    "PAID OUTS",
    "Other info",
}

EXPECTED_WEEKLY_LABELS = {
    "Opening Cash",
    "Item Sales",
    "Tax - HST",
    "Visa",
    "Mastercard",
    "Amex",
    "Debit Card",
    "Bank Deposit/EFT",
    "ECOM",
    "Cash over (short)",
    "House Acct Charge",
    "House Acct Payment",
    "Gift Card Issued",
    "Home Gift Cards",
    "e-gift cards",
    "Misc Cash Income",
    "Credit note issue",
    "Credit note redeemed",
    "Coupons",
    "Pre-pay Special Order",
    "Pre-pay Special order applied",
    "Purchases - Store use",
    "Loyalty Redemption",
    "Rounding",
}

EXPECTED_DAY_NAMES = {
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
}

BATCH_PREVIEW_SIZE = 25
MAX_RETRIES = 5


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


@dataclass
class DailyCashLine:
    source_tab_name: str
    business_date: str
    line_label: str
    amount: float | None
    account_code: str | None
    day_name: str | None


@dataclass
class TabDateRange:
    title: str
    start_date: date
    end_date: date
    covered_dates: list[date]
    label_score: int
    day_name_score: int


class GoogleSheetsClient:
    def __init__(self, service_account_email: str, private_key: str) -> None:
        self.service_account_email = service_account_email
        self.private_key = private_key.replace("\\n", "\n")
        self._cached_access_token: str | None = None
        self._cached_access_token_expires_at: datetime | None = None

    def _build_jwt(self) -> str:
        now = int(datetime.now(timezone.utc).timestamp())
        payload = {
            "iss": self.service_account_email,
            "scope": GOOGLE_SHEETS_SCOPE,
            "aud": GOOGLE_TOKEN_URL,
            "iat": now,
            "exp": now + 3600,
        }
        return jwt.encode(payload, self.private_key, algorithm="RS256")

    async def _get_access_token(self) -> str:
        now = datetime.now(timezone.utc)

        if (
            self._cached_access_token
            and self._cached_access_token_expires_at
            and now < self._cached_access_token_expires_at
        ):
            return self._cached_access_token

        assertion = self._build_jwt()
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion,
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(GOOGLE_TOKEN_URL, data=data)

            if response.is_error:
                raise RuntimeError(
                    f"Google token error. STATUS={response.status_code} BODY={response.text}"
                )

            payload = response.json()

        access_token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))

        self._cached_access_token = access_token
        self._cached_access_token_expires_at = now + timedelta(
            seconds=max(expires_in - 60, 60)
        )

        return access_token

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        params: Any | None = None,
        data: Any | None = None,
    ) -> dict[str, Any]:
        last_error_text = ""

        async with httpx.AsyncClient(timeout=30.0) as client:
            for attempt in range(MAX_RETRIES):
                response = await client.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    data=data,
                )

                if not response.is_error:
                    return response.json()

                last_error_text = (
                    f"URL={response.request.url} "
                    f"STATUS={response.status_code} "
                    f"BODY={response.text}"
                )

                # retry on rate limits and transient Google/server errors
                if response.status_code in {429, 500, 502, 503, 504} and attempt < MAX_RETRIES - 1:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        sleep_seconds = int(retry_after)
                    else:
                        sleep_seconds = min(2 ** attempt, 20)
                    await asyncio.sleep(sleep_seconds)
                    continue

                raise RuntimeError(f"Google Sheets API error. {last_error_text}")

        raise RuntimeError(f"Google Sheets API error. {last_error_text}")

    @staticmethod
    def build_tab_range(tab_name: str, a1_range: str = "A:ZZ") -> str:
        escaped = tab_name.replace("'", "''")
        return f"'{escaped}'!{a1_range}"

    @staticmethod
    def normalize_sheet_range(tab_name_or_range: str) -> str:
        if "!" in tab_name_or_range:
            return tab_name_or_range

        return GoogleSheetsClient.build_tab_range(tab_name_or_range, "A:ZZ")

    async def get_tab_values(
        self,
        spreadsheet_id: str,
        tab_name_or_range: str,
    ) -> list[list[str]]:
        access_token = await self._get_access_token()

        sheet_range = self.normalize_sheet_range(tab_name_or_range)
        encoded_range = quote(sheet_range, safe="!:'")

        url = f"{GOOGLE_SHEETS_BASE_URL}/{spreadsheet_id}/values/{encoded_range}"
        headers = {
            "Authorization": f"Bearer {access_token}",
        }
        params = {
            "majorDimension": "ROWS",
        }

        payload = await self._request_with_retry(
            "GET",
            url,
            headers=headers,
            params=params,
        )

        return payload.get("values", [])

    async def batch_get_ranges(
        self,
        spreadsheet_id: str,
        ranges: list[str],
    ) -> dict[str, list[list[str]]]:
        if not ranges:
            return {}

        access_token = await self._get_access_token()

        url = f"{GOOGLE_SHEETS_BASE_URL}/{spreadsheet_id}/values:batchGet"
        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        params: list[tuple[str, str]] = [("majorDimension", "ROWS")]
        for range_value in ranges:
            params.append(("ranges", range_value))

        payload = await self._request_with_retry(
            "GET",
            url,
            headers=headers,
            params=params,
        )

        results: dict[str, list[list[str]]] = {}
        for item in payload.get("valueRanges", []):
            results[item.get("range", "")] = item.get("values", [])

        return results

    async def get_sheet_titles(self, spreadsheet_id: str) -> list[str]:
        access_token = await self._get_access_token()

        url = f"{GOOGLE_SHEETS_BASE_URL}/{spreadsheet_id}"
        headers = {
            "Authorization": f"Bearer {access_token}",
        }
        params = {
            "fields": "sheets.properties.title",
        }

        payload = await self._request_with_retry(
            "GET",
            url,
            headers=headers,
            params=params,
        )

        return [
            sheet["properties"]["title"]
            for sheet in payload.get("sheets", [])
            if "properties" in sheet and "title" in sheet["properties"]
        ]

    @staticmethod
    def _analyze_weekly_preview(
        tab_name: str,
        rows: list[list[str]],
    ) -> TabDateRange | None:
        """
        Detect a weekly cash balancing tab by reading the actual cells, not the tab title.

        Expected preview range:
        - original row 4 contains dates across columns
        - original row 5 contains day names
        - original row 6 onward contains labels in column A
        """
        if not rows:
            return None

        date_row = rows[0] if len(rows) > 0 else []
        day_row = rows[1] if len(rows) > 1 else []
        body_rows = rows[2:] if len(rows) > 2 else []

        parsed_dates: list[date] = []
        for raw_value in date_row:
            iso_value = guess_date(raw_value)
            if iso_value:
                parsed_dates.append(datetime.strptime(iso_value, "%Y-%m-%d").date())

        unique_dates = sorted(set(parsed_dates))
        if len(unique_dates) < 5:
            return None

        span_days = (max(unique_dates) - min(unique_dates)).days
        if span_days > 7:
            return None

        label_values = {
            str(row[0]).strip()
            for row in body_rows
            if row and len(row) > 0 and row[0] is not None and str(row[0]).strip()
        }
        label_score = len(label_values.intersection(EXPECTED_WEEKLY_LABELS))

        day_name_score = 0
        for raw_value in day_row:
            if str(raw_value).strip().lower() in EXPECTED_DAY_NAMES:
                day_name_score += 1

        if label_score < 2 and day_name_score < 5:
            return None

        return TabDateRange(
            title=tab_name,
            start_date=min(unique_dates),
            end_date=max(unique_dates),
            covered_dates=unique_dates,
            label_score=label_score,
            day_name_score=day_name_score,
        )

    async def get_weekly_cash_tab_date_range(
        self,
        spreadsheet_id: str,
        tab_name: str,
    ) -> TabDateRange | None:
        preview_rows = await self.get_tab_values(
            spreadsheet_id,
            self.build_tab_range(tab_name, "A4:K20"),
        )
        return self._analyze_weekly_preview(tab_name, preview_rows)

    async def select_recent_weekly_tabs(
        self,
        spreadsheet_id: str,
        lookback_days: int,
        today: date | None = None,
    ) -> list[str]:
        if lookback_days < 1:
            raise ValueError("lookback_days must be at least 1")

        end_date = today or datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=lookback_days - 1)

        titles = await self.get_sheet_titles(spreadsheet_id)

        candidates: list[TabDateRange] = []
        seen_signatures: set[tuple[str, ...]] = set()

        preview_ranges_by_title = {
            title: self.build_tab_range(title, "A4:K20")
            for title in titles
        }

        title_list = list(preview_ranges_by_title.keys())

        for i in range(0, len(title_list), BATCH_PREVIEW_SIZE):
            batch_titles = title_list[i:i + BATCH_PREVIEW_SIZE]
            batch_ranges = [preview_ranges_by_title[title] for title in batch_titles]
            batch_results = await self.batch_get_ranges(spreadsheet_id, batch_ranges)

            for title in batch_titles:
                preview_range = preview_ranges_by_title[title]
                preview_rows = batch_results.get(preview_range, [])
                tab_range = self._analyze_weekly_preview(title, preview_rows)
                if not tab_range:
                    continue

                overlapping_dates = [
                    d for d in tab_range.covered_dates if start_date <= d <= end_date
                ]
                if not overlapping_dates:
                    continue

                signature = tuple(d.isoformat() for d in tab_range.covered_dates)
                if signature in seen_signatures:
                    continue

                seen_signatures.add(signature)
                candidates.append(tab_range)

        candidates.sort(
            key=lambda x: (
                x.start_date,
                x.end_date,
                -x.label_score,
                -x.day_name_score,
                x.title.lower(),
            )
        )

        return [item.title for item in candidates]


def safe_decimal(value: str | None) -> float | None:
    if value is None:
        return None

    text = str(value).strip().replace(",", "")

    if text == "":
        return None

    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]

    text = text.replace("$", "").strip()

    if text == "":
        return None

    try:
        return float(text)
    except ValueError:
        return None


def guess_date(value: str | None) -> str | None:
    if not value:
        return None

    text = str(value).strip()

    for fmt in (
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y/%m/%d",
        "%d-%b-%y",
        "%d-%b-%Y",
        "%d-%B-%y",
        "%d-%B-%Y",
    ):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue

    return None


def parse_weekly_cash_sheet(tab_name: str, rows: list[list[str]]) -> list[DailyCashLine]:
    """
    Parses the Bridlewood weekly sideways cash balancing sheet into one line per day per label.

    Expected weekly layout:
    - row 4 (index 3): dates across columns 2 to 8
    - row 5 (index 4): day names across columns 2 to 8
    - row 6 onward: line labels in column 1 and daily values across columns 2 to 8
    - account code is usually in column 11
    """
    if not rows or len(rows) < 6:
        return []

    date_row = rows[3] if len(rows) > 3 else []
    day_row = rows[4] if len(rows) > 4 else []

    daily_lines: list[DailyCashLine] = []

    for row in rows[5:]:
        label = str(row[0]).strip() if len(row) > 0 and row[0] is not None else ""

        if label in EXCLUDED_DAILY_LABELS:
            continue

        account_code = (
            str(row[10]).strip() if len(row) > 10 and row[10] is not None else None
        )
        if account_code == "":
            account_code = None

        for col_index in range(1, 8):
            raw_date = date_row[col_index] if col_index < len(date_row) else None
            raw_day = day_row[col_index] if col_index < len(day_row) else None
            raw_amount = row[col_index] if col_index < len(row) else None

            business_date = guess_date(raw_date)
            amount = safe_decimal(raw_amount)

            if business_date is None:
                continue

            if amount is None:
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


def normalize_cash_balancing_rows(
    tab_name: str,
    rows: list[list[str]],
) -> list[NormalizedCashRow]:
    """
    Raw row staging parser.
    Keeps the weekly sheet rows mostly as-is so the raw import table preserves source detail.
    """
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
        raw = {
            f"col_{i+1}": row[i] if i < len(row) else None
            for i in range(max(len(header), len(row)))
        }

        business_date = (
            guess_date(row[idx_date])
            if idx_date is not None and idx_date < len(row)
            else None
        )
        notes = row[idx_notes] if idx_notes is not None and idx_notes < len(row) else None
        row_key = f"{tab_name}|{business_date or 'no-date'}|{row_number}"
        row_hash = hashlib.sha256(
            json.dumps(row, ensure_ascii=False).encode("utf-8")
        ).hexdigest()

        normalized.append(
            NormalizedCashRow(
                source_tab_name=tab_name,
                business_date=business_date,
                row_number=row_number,
                row_key=row_key,
                row_hash=row_hash,
                notes=notes,
                sales_amount=(
                    safe_decimal(row[idx_sales])
                    if idx_sales is not None and idx_sales < len(row)
                    else None
                ),
                cash_amount=(
                    safe_decimal(row[idx_cash])
                    if idx_cash is not None and idx_cash < len(row)
                    else None
                ),
                debit_amount=(
                    safe_decimal(row[idx_debit])
                    if idx_debit is not None and idx_debit < len(row)
                    else None
                ),
                credit_amount=(
                    safe_decimal(row[idx_credit])
                    if idx_credit is not None and idx_credit < len(row)
                    else None
                ),
                ecommerce_amount=(
                    safe_decimal(row[idx_ecom])
                    if idx_ecom is not None and idx_ecom < len(row)
                    else None
                ),
                gift_card_amount=(
                    safe_decimal(row[idx_gift])
                    if idx_gift is not None and idx_gift < len(row)
                    else None
                ),
                hst_amount=(
                    safe_decimal(row[idx_hst])
                    if idx_hst is not None and idx_hst < len(row)
                    else None
                ),
                over_short_amount=(
                    safe_decimal(row[idx_over_short])
                    if idx_over_short is not None and idx_over_short < len(row)
                    else None
                ),
                raw_row_json=raw,
            )
        )

    return normalized
