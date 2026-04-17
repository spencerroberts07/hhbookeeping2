from fastapi import FastAPI

from .config import settings
from .routes.auth import router as auth_router
from .routes.cash_balancing import router as cash_balancing_router
from .routes.dashboard import router as dashboard_router
from .routes.month_end import router as month_end_router
from .routes.sync import router as sync_router
from .schemas import HealthResponse
from .routes.hh_ap import router as hh_ap_router
from .routes.month_end_hh_ap import router as month_end_hh_ap_router

app = FastAPI(title="Bridlewood Bookkeeping Control Layer", version="0.2.0")

app.include_router(auth_router)
app.include_router(sync_router)
app.include_router(dashboard_router)
app.include_router(cash_balancing_router)
app.include_router(month_end_router)
app.include_router(hh_ap_router)
app.include_router(month_end_hh_ap_router)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(environment=settings.app_env)
