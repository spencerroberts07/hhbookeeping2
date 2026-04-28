from fastapi import FastAPI

from .config import settings
from .routes.auth import router as auth_router
from .routes.auto_match import router as auto_match_router
from .routes.bank_csv import router as bank_csv_router
from .routes.card_settlement import router as card_settlement_router
from .routes.cash_balancing import router as cash_balancing_router
from .routes.dashboard import router as dashboard_router
from .routes.direct_vendor_ap import router as direct_vendor_ap_router
from .routes.hh_ap import router as hh_ap_router
from .routes import hh_ap_overrides
from .routes.hh_ap_remittance_bank_match import router as hh_ap_remittance_bank_match_router
from .routes.month_end import router as month_end_router
from .routes.month_end_close import router as month_end_close_router
from .routes.month_end_hh_ap import router as month_end_hh_ap_router
from .routes.month_end_workflow import router as month_end_workflow_router
from .routes.payroll import router as payroll_router
from .routes.period_close import router as period_close_router
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


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(environment=settings.app_env)
