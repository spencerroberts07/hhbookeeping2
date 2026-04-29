from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from .config import settings
from .routes.accruals import router as accruals_router
from .routes.auth import router as auth_router
from .routes.auto_match import router as auto_match_router
from .routes.bank_auto_journal import router as bank_auto_journal_router
from .routes.bank_csv import router as bank_csv_router
from .routes.bank_pdf import router as bank_pdf_router
from .routes.card_settlement import router as card_settlement_router
from .routes.cash_balancing import router as cash_balancing_router
from .routes.dashboard import router as dashboard_router
from .routes.depreciation import router as depreciation_router
from .routes.direct_vendor_ap import router as direct_vendor_ap_router
from .routes.gl_import import router as gl_import_router
from .routes.hh_ap import router as hh_ap_router
from .routes import hh_ap_overrides
from .routes.hh_ap_remittance_bank_match import router as hh_ap_remittance_bank_match_router
from .routes.month_end import router as month_end_router
from .routes.month_end_close import router as month_end_close_router
from .routes.month_end_hh_ap import router as month_end_hh_ap_router
from .routes.month_end_workflow import router as month_end_workflow_router
from .routes.payroll import router as payroll_router
from .routes.period_close import router as period_close_router
from .routes.pos_import import router as pos_import_router
from .routes.qbo_auth import router as qbo_auth_router
from .routes.qbo_bank_sync import router as qbo_bank_sync_router
from .routes.sync import router as sync_router
from .schemas import HealthResponse

app = FastAPI(title="Bridlewood Bookkeeping Control Layer", version="0.7.0")

# Auth (user) and QBO OAuth (formerly the only auth router)
app.include_router(auth_router)
app.include_router(qbo_auth_router)

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
