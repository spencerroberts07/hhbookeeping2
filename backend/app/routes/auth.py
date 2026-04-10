from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ..db import db_session
from ..quickbooks import QuickBooksClient
from ..schemas import ConnectResponse
from ..services import connect_company, get_entity_by_code

router = APIRouter(prefix="/api/auth/quickbooks", tags=["quickbooks-auth"])


@router.get("/connect", response_model=ConnectResponse)
def start_connect(entity_code: str = Query(default="1877-8")) -> ConnectResponse:
    qb = QuickBooksClient()
    state = qb.new_state()
    return ConnectResponse(
        entity_code=entity_code,
        authorization_url=qb.build_authorization_url(state),
        state=state,
    )


@router.get("/callback")
async def callback(
    code: str,
    realmId: str,
    state: str,
    entity_code: str = Query(default="1877-8"),
):
    try:
        with db_session() as session:
            if not get_entity_by_code(session, entity_code):
                raise HTTPException(status_code=404, detail=f"Unknown entity code: {entity_code}")
            result = await connect_company(session, entity_code, realmId, code)
            return JSONResponse(
                {
                    "ok": True,
                    "entity_code": entity_code,
                    "state_echo": state,
                    "realm_id": result["realm_id"],
                    "company_name": result["company_info"].get("CompanyName"),
                    "legal_name": result["company_info"].get("LegalName"),
                }
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
