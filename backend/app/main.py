from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from .config import settings
from .routes.accruals import router as accruals_router
from .routes.assistant import router as assistant_router
from .routes.auth import router as auth_router
from .routes.auto_match import router as auto_match_router
from .routes.bank_auto_journal import router as bank_auto_journal_router
from .routes.bank_csv import router as bank_csv_router
from .routes.bank_pdf import router as bank_pdf_router
from .routes.billing import router as billing_router, webhook_router as stripe_webhook_router
from .routes.card_settlement import router as card_settlement_router
from .routes.cash_balancing import router as cash_balancing_router
from .routes.clerk_webhook import router as clerk_webhook_router
from .routes.cogs import router as cogs_router
from .routes.dashboard import router as dashboard_router
from .routes.depreciation import router as depreciation_router
from .routes.direct_vendor_ap import router as direct_vendor_ap_router
from .routes.entities import me_router as me_router, router as entities_router
from .routes.gl_import import router as gl_import_router
from .routes.hh_ap import router as hh_ap_router
from .routes import hh_ap_overrides
from .routes.invoice_documents import router as invoice_documents_router
from .routes.hh_ap_remittance_bank_match import (
    clearing_router as hh_ap_remittance_clearing_router,
    router as hh_ap_remittance_bank_match_router,
)
from .routes.month_end import router as month_end_router
from .routes.month_end_close import router as month_end_close_router
from .routes.month_end_hh_ap import router as month_end_hh_ap_router
from .routes.month_end_workflow import router as month_end_workflow_router
from .routes.payroll import router as payroll_router
from .routes.period_close import router as period_close_router
from .routes.pos_import import router as pos_import_router
from .routes.reports import router as reports_router
from .routes.qbo_auth import router as qbo_auth_router
from .routes.qbo_bank_sync import router as qbo_bank_sync_router
from .routes.sync import router as sync_router
from .routes.vendor_classification import router as vendor_classification_router
from .schemas import HealthResponse

app = FastAPI(title="Bridlewood Bookkeeping Control Layer", version="0.7.0")

# --------------------------------------------------------------------------
# CORS — Clerk-issued session tokens come from a browser. The frontend
# domains in this list are the only ones allowed to call the API with
# credentials.
# --------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://bookwize.ca",
        "https://www.bookwize.ca",
        "https://bookwize.onrender.com",
        "https://bookwize-frontend.onrender.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Webhook receivers — must be reachable without an auth header.
# Registered first so they appear at the top of /docs.
app.include_router(clerk_webhook_router)
app.include_router(stripe_webhook_router)

# Billing endpoints (Clerk admins only)
app.include_router(billing_router)

# Auth (user) and QBO OAuth (formerly the only auth router)
app.include_router(auth_router)
app.include_router(qbo_auth_router)

# Entity management (admin) + caller-scoped entity lookup
app.include_router(entities_router)
app.include_router(me_router)

app.include_router(sync_router)
app.include_router(qbo_bank_sync_router)
app.include_router(bank_csv_router)
app.include_router(dashboard_router)
app.include_router(cash_balancing_router)
app.include_router(month_end_router)
app.include_router(hh_ap_router)
app.include_router(month_end_hh_ap_router)
app.include_router(month_end_workflow_router)
app.include_router(month_end_close_router)
app.include_router(hh_ap_remittance_bank_match_router)
app.include_router(hh_ap_remittance_clearing_router)
app.include_router(direct_vendor_ap_router)
app.include_router(card_settlement_router)
app.include_router(hh_ap_overrides.router)

# v0.7 modules: period close lock workflow, post-import auto-match runner,
# payroll control.
app.include_router(period_close_router)
app.include_router(auto_match_router)
app.include_router(payroll_router)

# v0.8 module: month-end POS imports + inventory adjustment journal builders.
app.include_router(pos_import_router)

# v0.9 modules: GL trial-balance comparison, PDF bank parser, fixed-asset
# depreciation, monthly accruals.
app.include_router(gl_import_router)
app.include_router(bank_pdf_router)
app.include_router(depreciation_router)
app.include_router(accruals_router)
app.include_router(bank_auto_journal_router)

# v1.0 module: self-improving vendor classification (Layers 2 + 3 of
# the bank auto-journal classifier — vendor memory + Claude API).
app.include_router(vendor_classification_router)

# v1.2 module: monthly COGS journal builder (POS COGS + HHSL dating
# carry-forward).
app.include_router(cogs_router)

# Invoice audit trail: PDF upload, auto-matching to bank/HH-AP/journal,
# unmatched queue, post-to-AP workflow.
app.include_router(invoice_documents_router)

# Live financial reports (Income Statement, Balance Sheet, Trial Balance).
app.include_router(reports_router)

# BookWize AI assistant — conversational classifier + entity memory.
app.include_router(assistant_router)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(environment=settings.app_env)


# --------------------------------------------------------------------------
# OpenAPI / Swagger UI — register a Bearer security scheme so the
# Authorize button at /docs sends `Authorization: Bearer <token>` on every
# request. Without this, protected endpoints can only be exercised via
# curl/PowerShell. The scheme name matches HTTPBearer(scheme_name=...) in
# services_auth.py so per-operation security and the global default agree.
# --------------------------------------------------------------------------


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    schema = get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
    )

    components = schema.setdefault("components", {})
    security_schemes = components.setdefault("securitySchemes", {})
    security_schemes["BearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": (
            "Paste the access_token returned by POST /api/auth/login. "
            "Do not include the 'Bearer ' prefix — Swagger adds it."
        ),
    }
    schema["security"] = [{"BearerAuth": []}]

    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi
