# Bridlewood bookkeeping control layer - QuickBooks step

This package extends the earlier scaffold into a runnable backend starter for the first live integration step:

- Connect to QuickBooks Online using OAuth 2.0
- Store the connected company (realmId) and refresh token
- Pull chart of accounts
- Pull transaction-like data for a date range using CDC
- Normalize those records into Bridlewood staging tables
- Leave all posting as review-only

## What is in here

- `backend/app/` - FastAPI app with QuickBooks auth and sync routes
- `backend/schema.sql` - original schema
- `backend/sql/001_quickbooks_connection.sql` - additional tables for OAuth tokens and sync history
- `backend/requirements.txt` - Python dependencies
- `backend/.env.example` - environment template
- `docs/quickbooks_setup.md` - setup walkthrough

## Local run

1. Create a Postgres database.
2. Run `backend/schema.sql`
3. Run `backend/sql/001_quickbooks_connection.sql`
4. Copy `backend/.env.example` to `backend/.env` and fill in your values.
5. Install Python dependencies.
6. Start the API with uvicorn.

Example:

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Then open:
- `GET /health`
- `GET /api/auth/quickbooks/connect?entity_code=1877-8`

## Important

This is a review-first control layer.
It does not auto-post into QuickBooks.
