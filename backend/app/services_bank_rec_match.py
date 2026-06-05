"""
Bank reconciliation matching engine (Phase 3B). Read/link only — NO GL writes.

Matches `bank_transactions` (a cash account, a period) against the GL cash-account
`journal_lines`, in confidence tiers:
  Tier 0  direct link via source_json.bank_transaction_id  -> auto-clear (100)
  Tier 1  exact amount + same direction + date window       -> auto-clear (>=90)
  Tier 2  one-to-many aggregate (cash_balancing lump <-> set of daily deposits;
          split deposits) via bounded subset-sum              -> suggest
  Tier 3  fuzzy amount/date/description                       -> suggest
  Tier 4  manual (leftover)                                   -> outstanding

Persists to `bank_transaction_matches` (target_table_name='journal_lines',
match_group_id for one-to-many) and updates `bank_transactions.review_status`.
Idempotent: clears prior rec matches for the scoped bank txns before re-linking.
"""
from __future__ import annotations

import json
import uuid
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import text

_POSTED = ("draft", "voided", "rejected")  # excluded book batch statuses
_CENT = Decimal("0.01")
_AUTO_CLEAR = 90
_DATE_WINDOW = 2
_AGG_MAX_N = 25


def _D(v) -> Decimal:
    return Decimal(str(v or 0))


def _load_bank(session, entity_id, account_code, period_start, period_end) -> list[dict]:
    rows = session.execute(
        text(
            """
            SELECT id, transaction_date, amount, direction, description,
                   normalized_description, reference_number, review_status
              FROM bank_transactions
             WHERE entity_id = :e AND source_account_code = :ac
               AND transaction_date BETWEEN :ps AND :pe
          ORDER BY transaction_date, id
            """
        ),
        {"e": entity_id, "ac": account_code, "ps": period_start, "pe": period_end},
    ).mappings().all()
    return [dict(r) for r in rows]


def _load_book(session, entity_id, account_code, period_start, period_end) -> list[dict]:
    rows = session.execute(
        text(
            f"""
            SELECT jl.id, jl.journal_batch_id, jb.source_module,
                   (jl.debit_amount - jl.credit_amount) AS signed_amount,
                   jl.memo, jl.source_json,
                   (jl.source_json->>'bank_transaction_id') AS linked_bank_txn_id,
                   ap.period_end AS period_end
              FROM journal_lines jl
              JOIN journal_batches jb ON jb.id = jl.journal_batch_id
              JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
             WHERE jb.entity_id = :e AND jl.account_code = :ac
               AND jb.status NOT IN {_POSTED}
               AND ap.period_start >= :ps AND ap.period_end <= :pe
            """
        ),
        {"e": entity_id, "ac": account_code, "ps": period_start, "pe": period_end},
    ).mappings().all()
    return [dict(r) for r in rows]


def _greedy_subset(targets_pool: list[dict], target: Decimal) -> list[dict] | None:
    """Date-ordered prefix that sums to target (the realistic daily-deposit case),
    then a small bounded subset-sum. Integer cents to avoid float drift."""
    pool = sorted(targets_pool, key=lambda x: (x["transaction_date"], x["id"]))
    tgt = int((target * 100).to_integral_value())
    # greedy prefix
    run = 0
    acc: list[dict] = []
    for b in pool:
        run += int((_D(b["amount"]) * 100).to_integral_value())
        acc.append(b)
        if run == tgt:
            return list(acc)
        if run > tgt:
            break
    # bounded subset-sum DP over the (capped) pool
    cand = pool[:_AGG_MAX_N]
    cents = [int((_D(b["amount"]) * 100).to_integral_value()) for b in cand]
    reachable: dict[int, list[int]] = {0: []}
    for idx, c in enumerate(cents):
        for s, picks in list(reachable.items()):
            ns = s + c
            if ns not in reachable and abs(ns) <= abs(tgt) + 100:
                reachable[ns] = picks + [idx]
        if tgt in reachable:
            break
    if tgt in reachable:
        return [cand[i] for i in reachable[tgt]]
    return None


def run_match(session, *, entity_id: str, source_account_code: str,
              period_start: date, period_end: date) -> dict[str, Any]:
    bank = _load_bank(session, entity_id, source_account_code, period_start, period_end)
    book = _load_book(session, entity_id, source_account_code, period_start, period_end)
    bank_by_id = {str(b["id"]): b for b in bank}

    # Bank txns already matched elsewhere (e.g. HH AP remittances) are ALREADY
    # cleared — only one active match per bank txn is allowed. Treat them as
    # pre-cleared: skip re-linking, but mark their book counterpart matched.
    pre_matched: set[str] = set()
    if bank_by_id:
        rows = session.execute(
            text(
                "SELECT bank_transaction_id FROM bank_transaction_matches "
                "WHERE entity_id=:e AND active=TRUE AND COALESCE(target_table_name,'') <> 'journal_lines' "
                "AND bank_transaction_id = ANY(:ids)"
            ),
            {"e": entity_id, "ids": list(bank_by_id.keys())},
        ).scalars().all()
        pre_matched = {str(r) for r in rows}

    matched_bank: set[str] = set(pre_matched)
    matched_book: set[str] = set()
    links: list[dict] = []  # {bank_id, book_id, match_type, confidence, group_id, amount}

    # ---- Tier 0: direct link via source_json.bank_transaction_id ----
    for ln in book:
        bid = ln.get("linked_bank_txn_id")
        if not (bid and bid in bank_by_id):
            continue
        if bid in pre_matched:
            matched_book.add(str(ln["id"]))  # already cleared via its existing match
            continue
        if bid not in matched_bank:
            links.append({"bank_id": bid, "book_id": str(ln["id"]), "match_type": "direct_link",
                          "confidence": 100, "group_id": None, "amount": float(bank_by_id[bid]["amount"])})
            matched_bank.add(bid)
            matched_book.add(str(ln["id"]))

    def book_date(ln) -> tuple[date, bool]:
        bid = ln.get("linked_bank_txn_id")
        if bid and bid in bank_by_id:
            return bank_by_id[bid]["transaction_date"], False
        return ln["period_end"], True  # month-bucketed

    # ---- Tier 1: exact amount + same direction + date window ----
    book_un = [ln for ln in book if str(ln["id"]) not in matched_book]
    for b in bank:
        if str(b["id"]) in matched_bank:
            continue
        amt = _D(b["amount"])
        best = None
        for ln in book_un:
            if str(ln["id"]) in matched_book:
                continue
            sa = _D(ln["signed_amount"])
            if abs(sa - amt) > _CENT or (sa >= 0) != (amt >= 0):
                continue
            bd, bucketed = book_date(ln)
            gap = abs((b["transaction_date"] - bd).days)
            if gap > _DATE_WINDOW and not bucketed:
                continue
            conf = 95 - (5 * gap) - (10 if bucketed else 0)
            if best is None or conf > best[0]:
                best = (conf, ln)
        if best and best[0] >= _AUTO_CLEAR:
            conf, ln = best
            links.append({"bank_id": str(b["id"]), "book_id": str(ln["id"]), "match_type": "exact",
                          "confidence": conf, "group_id": None, "amount": float(amt)})
            matched_bank.add(str(b["id"]))
            matched_book.add(str(ln["id"]))

    # ---- Tier 2: one-to-many aggregate (book lump <-> set of bank deposits) ----
    for ln in book:
        if str(ln["id"]) in matched_book:
            continue
        target = _D(ln["signed_amount"])
        if abs(target) < Decimal("1"):
            continue
        pool = [b for b in bank if str(b["id"]) not in matched_bank
                and (_D(b["amount"]) >= 0) == (target >= 0)]
        if len(pool) < 2:
            continue
        subset = _greedy_subset(pool, target)
        if subset and len(subset) >= 2:
            gid = str(uuid.uuid4())
            for b in subset:
                links.append({"bank_id": str(b["id"]), "book_id": str(ln["id"]),
                              "match_type": "aggregate_group", "confidence": 78,
                              "group_id": gid, "amount": float(b["amount"])})
                matched_bank.add(str(b["id"]))
            matched_book.add(str(ln["id"]))

    # ---- Tier 3: fuzzy (amount within tolerance + date + description overlap) ----
    book_un = [ln for ln in book if str(ln["id"]) not in matched_book]
    for b in bank:
        if str(b["id"]) in matched_bank:
            continue
        amt = _D(b["amount"])
        tol = max(Decimal("0.05"), abs(amt) * Decimal("0.005"))
        bdesc = set((b.get("normalized_description") or b.get("description") or "").upper().split())
        best = None
        for ln in book_un:
            if str(ln["id"]) in matched_book:
                continue
            sa = _D(ln["signed_amount"])
            if abs(sa - amt) > tol or (sa >= 0) != (amt >= 0):
                continue
            bd, bucketed = book_date(ln)
            gap = abs((b["transaction_date"] - bd).days)
            if gap > 7 and not bucketed:
                continue
            ldesc = set((ln.get("memo") or "").upper().split())
            overlap = len(bdesc & ldesc)
            score = 50 - min(50, int(abs(sa - amt) / (tol or Decimal("1")) * 50)) \
                + max(0, 25 - 3 * gap) + min(15, overlap * 5)
            if best is None or score > best[0]:
                best = (score, ln)
        if best and 30 <= best[0] < _AUTO_CLEAR:
            score, ln = best
            links.append({"bank_id": str(b["id"]), "book_id": str(ln["id"]), "match_type": "fuzzy",
                          "confidence": score, "group_id": None, "amount": float(amt)})
            # fuzzy is a suggestion: mark book consumed for this pass but not auto-clear
            matched_book.add(str(ln["id"]))

    _persist(session, entity_id, bank, links)

    auto = [l for l in links if l["match_type"] in ("direct_link", "exact")]
    return {
        "bank_count": len(bank), "book_count": len(book),
        "pre_cleared": len(pre_matched),
        "auto_cleared": len(auto),
        "aggregate_groups": len({l["group_id"] for l in links if l["group_id"]}),
        "suggested": len([l for l in links if l["match_type"] in ("aggregate_group", "fuzzy")]),
        "unmatched_bank": [str(b["id"]) for b in bank if str(b["id"]) not in matched_bank],
        "unmatched_book": [str(ln["id"]) for ln in book if str(ln["id"]) not in matched_book],
        "links": links,
    }


def _persist(session, entity_id, bank, links) -> None:
    bank_ids = [str(b["id"]) for b in bank]
    if bank_ids:
        # idempotent: drop prior rec matches (target=journal_lines) for these bank txns
        session.execute(
            text(
                "DELETE FROM bank_transaction_matches "
                "WHERE entity_id = :e AND target_table_name = 'journal_lines' "
                "AND bank_transaction_id = ANY(:ids)"
            ),
            {"e": entity_id, "ids": bank_ids},
        )
    auto_clear_types = {"direct_link", "exact"}
    cleared_bank: set[str] = set()
    for l in links:
        status = "cleared" if l["match_type"] in auto_clear_types else "suggested"
        session.execute(
            text(
                """
                INSERT INTO bank_transaction_matches (
                    bank_transaction_id, entity_id, match_type, match_status,
                    matched_amount, target_table_name, target_record_id, target_label,
                    active, match_group_id, payload_json, created_by
                ) VALUES (
                    :btid, :e, :mt, :ms, :amt, 'journal_lines', :rec, :label,
                    TRUE, :gid, CAST(:pj AS jsonb), 'system'
                )
                """
            ),
            {
                "btid": l["bank_id"], "e": entity_id, "mt": l["match_type"], "ms": status,
                "amt": l["amount"], "rec": l["book_id"],
                "label": f"journal_line {l['book_id'][:8]}",
                "gid": l["group_id"], "pj": json.dumps({"confidence": l["confidence"]}),
            },
        )
        if l["match_type"] in auto_clear_types:
            cleared_bank.add(l["bank_id"])
    # update review_status for auto-cleared bank txns + audit event
    for bid in cleared_bank:
        cur = session.execute(
            text("SELECT review_status FROM bank_transactions WHERE id=:id"), {"id": bid}
        ).scalar()
        if cur != "cleared":
            session.execute(
                text("UPDATE bank_transactions SET review_status='cleared', last_reviewed_at=NOW() WHERE id=:id"),
                {"id": bid},
            )
            session.execute(
                text(
                    "INSERT INTO bank_transaction_review_events (bank_transaction_id, entity_id, action, "
                    "actor_email, from_review_status, to_review_status, note) "
                    "VALUES (:id,:e,'auto_match','system',:frm,'cleared','bank rec auto-clear')"
                ),
                {"id": bid, "e": entity_id, "frm": cur},
            )
