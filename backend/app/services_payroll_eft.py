"""
CPA Standard 005 EFT file generator (Canadian Payments Association).

This is the file format Canadian financial institutions accept for
direct-deposit payroll. Bridlewood uploads the generated .txt file to
TD Business Banking, which credits each employee from the bank's
clearing account.

Spec notes (CPA 005 — Pre-Authorized Debit / Direct Deposit, 1464-byte
logical records):

  - Each logical record is exactly 1464 characters of ASCII text.
  - Lines are CR+LF terminated (we use \r\n to match TD's parser).
  - Field positions are 1-based in the published spec; this module uses
    Python's 0-based slicing internally and the helper `_pad` /
    `_zpad` to fix widths.
  - Amounts are in cents (no decimals) and zero-left-padded.
  - Dates are CRA-style Julian: '0YYDDD' (e.g. 2026-02-15 = '026046').

Record types we generate:

  A — file header (1 record, first)
  C — credit transaction (1 per employee paid)
  Z — trailer / control totals (1 record, last)

We don't generate D (debit) records — payroll is purely outbound credits
to employee accounts.

References:
  - Payments Canada (CPA) Standard 005, v2.3
  - TD EFT Originator Implementation Guide
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable


# CPA 005 fixed-width spec. Numbers are 1-based, matching the published
# standard. We translate to 0-based slices below.
RECORD_LENGTH = 1464

# Originator IDs assigned by your bank during EFT setup. For TD this is
# typically the 10-digit originator number padded to 10 characters. We
# leave the field as a parameter so non-Bridlewood dealers can plug in
# their own number.

TD_DESTINATION_DATA_CENTRE = "0040"  # TD's CPA destination code
# (Some bank guides quote '00450' / '0450'; the 4-char field expected by
# TD's portal is '0040'. If the bank rejects the file, this is the first
# field to double-check during onboarding.)

TRANSACTION_TYPE_PERSONAL_CHEQUING = "461"
TRANSACTION_TYPE_PERSONAL_SAVINGS = "463"
# CPA 005 transaction-type codes: 4xx = "payroll deposit (direct deposit)".
# 461 = chequing, 463 = savings. Some older docs cite 200/201 — that's
# the deprecated short-form scheme. Modern TD uses the 3-digit codes.


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _pad(value: str, width: int, *, right_justify: bool = False) -> str:
    """Pad `value` to `width` chars. Truncates if too long.

    EFT fields are space-padded text. Default is left-justified
    (right-padded). Numbers/IDs that need zero-fill use _zpad instead.
    """
    s = "" if value is None else str(value)
    s = s.upper()
    s = "".join(c for c in s if 32 <= ord(c) < 127)  # strip non-ASCII
    if len(s) >= width:
        return s[:width]
    if right_justify:
        return s.rjust(width, " ")
    return s.ljust(width, " ")


def _zpad(value: int | str, width: int) -> str:
    """Zero-pad a numeric value. Truncates from the left if too long
    (which is wrong — but we raise before then, in _amount_cents)."""
    s = str(value)
    if len(s) > width:
        raise ValueError(f"value {value!r} exceeds width {width}")
    return s.rjust(width, "0")


def _amount_cents(amount: Decimal | float | int) -> int:
    """Convert a dollar amount to cents (int). Half-up rounding."""
    if isinstance(amount, Decimal):
        cents = (amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return int(cents)
    return int(round(float(amount) * 100))


def julian_date(d: date) -> str:
    """CRA / CPA Julian date: 0YYDDD. 2026-02-15 → '026046'."""
    return f"0{d.year % 100:02d}{d.timetuple().tm_yday:03d}"


def _sanitize_account(account: str) -> str:
    """Strip non-digit characters from an account number. TD accepts up
    to 12 digits; the field on the C-record is 12 chars, space-padded."""
    return "".join(c for c in (account or "") if c.isdigit())[:12]


# ----------------------------------------------------------------------
# Data classes
# ----------------------------------------------------------------------


@dataclass
class EFTEmployee:
    """One employee credit. The amount is in dollars (will be converted
    to cents internally)."""

    name: str
    transit: str          # 5 digits
    institution: str      # 3 digits
    account: str          # up to 12 digits
    amount: Decimal | float
    transaction_type: str = TRANSACTION_TYPE_PERSONAL_CHEQUING

    def validate(self) -> list[str]:
        """Return a list of issues. Empty list = OK."""
        issues: list[str] = []
        t = (self.transit or "").strip()
        i = (self.institution or "").strip()
        a = _sanitize_account(self.account or "")
        if not (t.isdigit() and len(t) == 5):
            issues.append(f"transit must be 5 digits (got {t!r})")
        if not (i.isdigit() and len(i) == 3):
            issues.append(f"institution must be 3 digits (got {i!r})")
        if not a:
            issues.append(f"account is empty / non-numeric (got {self.account!r})")
        if _amount_cents(self.amount) <= 0:
            issues.append(f"amount must be positive (got {self.amount!r})")
        return issues


@dataclass
class EFTHeader:
    """A-record context."""

    originator_id: str             # 10-char originator number from TD
    file_creation_number: int      # monotonically increasing per originator
    creation_date: date
    originator_short_name: str     # 15 char, shown on payee statements
    originator_long_name: str      # 30 char, settlement / clearing label
    destination_data_centre: str = TD_DESTINATION_DATA_CENTRE
    currency_code: str = "CAD"


# ----------------------------------------------------------------------
# Builders
# ----------------------------------------------------------------------


def _build_a_record(header: EFTHeader) -> str:
    """A-record (file header). 1464 chars total. Most of the back half
    is reserved-blank space — that's per spec."""
    parts: list[str] = []
    parts.append("A")                                       # 1
    parts.append(_zpad(1, 9))                                # 2-10: record count (always 1 for A)
    parts.append(_pad(header.originator_id, 10))             # 11-20: originator ID
    parts.append(_zpad(header.file_creation_number, 4))      # 21-24: file creation number
    parts.append(julian_date(header.creation_date))          # 25-30: creation date
    parts.append(_pad(header.destination_data_centre, 5))    # 31-35: destination data centre
    parts.append(_pad("", 20))                               # 36-55: reserved
    parts.append(_pad(header.currency_code, 3))              # 56-58: currency
    parts.append(_pad("", RECORD_LENGTH - 58))               # 59-1464: blank
    return "".join(parts)[:RECORD_LENGTH].ljust(RECORD_LENGTH)


def _build_c_record(
    *,
    header: EFTHeader,
    sequence_number: int,
    employee: EFTEmployee,
    payment_date: date,
    cross_reference: str,
) -> str:
    """C-record (credit). One per employee.

    Pads to 1464 — a single C-record fills the whole logical record.
    (Some originator banks batch six C-segments per physical record;
    TD's portal accepts the simpler one-segment-per-record layout.)
    """
    parts: list[str] = []
    parts.append("C")                                                 # 1: record type
    parts.append(_zpad(sequence_number, 9))                           # 2-10
    parts.append(_pad(header.originator_id, 10))                      # 11-20
    parts.append(_zpad(header.file_creation_number, 4))               # 21-24
    parts.append(_pad(employee.transaction_type, 3))                  # 25-27: txn type
    parts.append(_zpad(_amount_cents(employee.amount), 10))           # 28-37: amount in cents
    parts.append(julian_date(payment_date))                           # 38-43: payment date
    institution = _pad(employee.institution.strip(), 4)               # 44-47
    transit = _pad(employee.transit.strip(), 5)                       # 48-52
    account = _pad(_sanitize_account(employee.account), 12)           # 53-64
    parts.append(institution)
    parts.append(transit)
    parts.append(account)
    parts.append(_zpad(0, 22))                                        # 65-86: item trace (zero-filled)
    parts.append(_pad("", 3))                                         # 87-89: stored txn type
    parts.append(_pad(header.originator_short_name, 15))              # 90-104
    parts.append(_pad(employee.name, 30))                             # 105-134: payee name
    parts.append(_pad(header.originator_long_name, 30))               # 135-164
    parts.append(_pad(header.originator_id, 10))                      # 165-174: originator ref
    parts.append(_pad(cross_reference, 19))                           # 175-193: cross-reference
    parts.append(_pad("", RECORD_LENGTH - 193))                       # 194-1464: reserved
    return "".join(parts)[:RECORD_LENGTH].ljust(RECORD_LENGTH)


def _build_z_record(
    *,
    header: EFTHeader,
    credit_count: int,
    credit_total_cents: int,
    record_count: int,
) -> str:
    """Z-record (trailer). Totals across the file."""
    parts: list[str] = []
    parts.append("Z")                                       # 1
    parts.append(_zpad(record_count, 9))                    # 2-10: total record count (incl A,C,Z)
    parts.append(_pad(header.originator_id, 10))            # 11-20
    parts.append(_zpad(header.file_creation_number, 4))     # 21-24
    parts.append(_zpad(credit_total_cents, 14))             # 25-38: total value of credits, cents
    parts.append(_zpad(credit_count, 8))                    # 39-46: count of credits
    parts.append(_zpad(0, 14))                              # 47-60: total value of debits (none)
    parts.append(_zpad(0, 8))                               # 61-68: count of debits (none)
    parts.append(_pad("", RECORD_LENGTH - 68))              # 69-1464: reserved
    return "".join(parts)[:RECORD_LENGTH].ljust(RECORD_LENGTH)


# ----------------------------------------------------------------------
# Public builder
# ----------------------------------------------------------------------


@dataclass
class EFTBuildResult:
    text: str                  # the full file contents (multi-line, CRLF)
    record_count: int          # total physical records (incl. A and Z)
    credit_count: int          # number of C-records
    total_amount: Decimal      # dollar total of credits


def build_eft_file(
    *,
    header: EFTHeader,
    employees: Iterable[EFTEmployee],
    payment_date: date,
    cross_reference: str,
) -> EFTBuildResult:
    """Render the full file. Validates each employee row first;
    raises ValueError listing every issue if any row is bad."""
    employees = list(employees)
    issues: list[str] = []
    for idx, e in enumerate(employees, start=1):
        for problem in e.validate():
            issues.append(f"employee #{idx} ({e.name!r}): {problem}")
    if issues:
        raise ValueError("EFT validation failed:\n  " + "\n  ".join(issues))
    if not employees:
        raise ValueError("EFT file requires at least one employee record")

    a = _build_a_record(header)
    c_records: list[str] = []
    total_cents = 0
    for n, emp in enumerate(employees, start=2):
        # Sequence starts at 2 because the A-record is sequence #1.
        c = _build_c_record(
            header=header,
            sequence_number=n,
            employee=emp,
            payment_date=payment_date,
            cross_reference=cross_reference,
        )
        c_records.append(c)
        total_cents += _amount_cents(emp.amount)
    record_count = 1 + len(c_records) + 1  # A + Cs + Z
    z = _build_z_record(
        header=header,
        credit_count=len(c_records),
        credit_total_cents=total_cents,
        record_count=record_count,
    )

    text = "\r\n".join([a, *c_records, z]) + "\r\n"
    return EFTBuildResult(
        text=text,
        record_count=record_count,
        credit_count=len(c_records),
        total_amount=Decimal(total_cents) / Decimal(100),
    )
