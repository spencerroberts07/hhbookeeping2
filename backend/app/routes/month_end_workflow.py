from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import db_session
from ..journal_batch_workflow import (
    get_journal_batch,
    get_workflow_events,
    money,
    money_float,
    parse_summary_json,
    resolve_workflow_status,
    serialize_workflow,
    transition_journal_batch_workflow,
)

router = APIRouter(prefix="/api/month-end/workflow", tags=["month-end", "month-end-workflow"])


class JournalBatchWorkflowLocator(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    period_end: str = Field(..., examples=["2026-02-28"])
    source_module: str = Field(..., examples=["hh_ap"])
    batch_label: str = Field(..., examples=["hh_ap_month_end"])


class JournalBatchWorkflowActionRequest(JournalBatchWorkflowLocator):
    actor_email: str = Field(..., examples=["controller@bridlewood.ca"])
    note: str | None = Field(default=None, examples=["February approved with minor support allocation variance remaining"])



def get_entity(session, entity_code: str):
    entity = session.execute(
        text(
            """
            SELECT id, entity_code, entity_name
            FROM entities
            WHERE entity_code = :entity_code
            """
        ),
        {"entity_code": entity_code},
    ).mappings().first()

    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")

    return entity



def get_accounting_period(session, entity_id: str, period_end: str):
    period = session.execute(
        text(
            """
            SELECT id, period_label, period_start, period_end, fiscal_year, fiscal_period_number, status
            FROM accounting_periods
            WHERE entity_id = :entity_id
              AND period_end = :period_end
            LIMIT 1
            """
        ),
        {"entity_id": entity_id, "period_end": period_end},
    ).mappings().first()

    if not period:
        raise HTTPException(status_code=404, detail=f"No accounting period found for period_end={period_end}")

    return period



def build_workflow_response(session, locator: JournalBatchWorkflowLocator | JournalBatchWorkflowActionRequest):
    entity = get_entity(session, locator.entity_code)
    period = get_accounting_period(session, entity["id"], locator.period_end)
    batch = get_journal_batch(
        session,
        entity_id=entity["id"],
        accounting_period_id=period["id"],
        source_module=locator.source_module,
        batch_label=locator.batch_label,
    )

    if not batch:
        raise HTTPException(status_code=404, detail="No journal batch found for that entity / period / source_module / batch_label")

    history = get_workflow_events(session, str(batch["id"]))
    total_debits = money(batch["total_debits"])
    total_credits = money(batch["total_credits"])
    balance_difference = money(total_debits - total_credits)

    return {
        "entity_code": entity["entity_code"],
        "entity_name": entity.get("entity_name"),
        "accounting_period": {
            "id": str(period["id"]),
            "period_label": period["period_label"],
            "period_start": str(period["period_start"]),
            "period_end": str(period["period_end"]),
            "fiscal_year": period.get("fiscal_year"),
            "fiscal_period_number": period.get("fiscal_period_number"),
            "status": period.get("status"),
        },
        "journal_batch": {
            "id": str(batch["id"]),
            "source_module": batch["source_module"],
            "batch_label": batch["batch_label"],
            "status": batch["status"],
            "workflow_status": resolve_workflow_status(batch),
            "total_debits": money_float(total_debits),
            "total_credits": money_float(total_credits),
            "balance_difference": money_float(balance_difference),
            "is_balanced": balance_difference == 0,
            "created_at": batch["created_at"].isoformat() if batch["created_at"] else None,
            "updated_at": batch["updated_at"].isoformat() if batch["updated_at"] else None,
        },
        "workflow": serialize_workflow(batch, history=history),
        "summary_json": parse_summary_json(batch.get("summary_json")),
    }


@router.get("/batch")
def get_month_end_workflow_batch(
    entity_code: str,
    period_end: str,
    source_module: str,
    batch_label: str,
):
    locator = JournalBatchWorkflowLocator(
        entity_code=entity_code,
        period_end=period_end,
        source_module=source_module,
        batch_label=batch_label,
    )

    with db_session() as session:
        return build_workflow_response(session, locator)


@router.post("/submit")
def submit_month_end_batch_for_review(payload: JournalBatchWorkflowActionRequest):
    with db_session() as session:
        entity = get_entity(session, payload.entity_code)
        period = get_accounting_period(session, entity["id"], payload.period_end)
        batch = get_journal_batch(
            session,
            entity_id=entity["id"],
            accounting_period_id=period["id"],
            source_module=payload.source_module,
            batch_label=payload.batch_label,
        )

        if not batch:
            raise HTTPException(status_code=404, detail="No journal batch found for that entity / period / source_module / batch_label")

        transition_journal_batch_workflow(
            session,
            batch_row=batch,
            action="submit",
            actor_email=payload.actor_email,
            note=payload.note,
        )

        return build_workflow_response(session, payload)


@router.post("/approve")
def approve_month_end_batch(payload: JournalBatchWorkflowActionRequest):
    with db_session() as session:
        entity = get_entity(session, payload.entity_code)
        period = get_accounting_period(session, entity["id"], payload.period_end)
        batch = get_journal_batch(
            session,
            entity_id=entity["id"],
            accounting_period_id=period["id"],
            source_module=payload.source_module,
            batch_label=payload.batch_label,
        )

        if not batch:
            raise HTTPException(status_code=404, detail="No journal batch found for that entity / period / source_module / batch_label")

        transition_journal_batch_workflow(
            session,
            batch_row=batch,
            action="approve",
            actor_email=payload.actor_email,
            note=payload.note,
        )

        return build_workflow_response(session, payload)


@router.post("/reject")
def reject_month_end_batch(payload: JournalBatchWorkflowActionRequest):
    with db_session() as session:
        entity = get_entity(session, payload.entity_code)
        period = get_accounting_period(session, entity["id"], payload.period_end)
        batch = get_journal_batch(
            session,
            entity_id=entity["id"],
            accounting_period_id=period["id"],
            source_module=payload.source_module,
            batch_label=payload.batch_label,
        )

        if not batch:
            raise HTTPException(status_code=404, detail="No journal batch found for that entity / period / source_module / batch_label")

        transition_journal_batch_workflow(
            session,
            batch_row=batch,
            action="reject",
            actor_email=payload.actor_email,
            note=payload.note,
        )

        return build_workflow_response(session, payload)


@router.post("/reopen")
def reopen_month_end_batch(payload: JournalBatchWorkflowActionRequest):
    with db_session() as session:
        entity = get_entity(session, payload.entity_code)
        period = get_accounting_period(session, entity["id"], payload.period_end)
        batch = get_journal_batch(
            session,
            entity_id=entity["id"],
            accounting_period_id=period["id"],
            source_module=payload.source_module,
            batch_label=payload.batch_label,
        )

        if not batch:
            raise HTTPException(status_code=404, detail="No journal batch found for that entity / period / source_module / batch_label")

        transition_journal_batch_workflow(
            session,
            batch_row=batch,
            action="reopen",
            actor_email=payload.actor_email,
            note=payload.note,
        )

        return build_workflow_response(session, payload)
