from fastapi import FastAPI

from .config import settings
from .routes.auth import router as auth_router
from .routes.dashboard import router as dashboard_router
from .routes.sync import router as sync_router
from .schemas import HealthResponse


app = FastAPI(title="Bridlewood Bookkeeping Control Layer", version="0.2.0")
app.include_router(auth_router)
app.include_router(sync_router)
app.include_router(dashboard_router)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(environment=settings.app_env)
