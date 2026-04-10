# QuickBooks step - setup walkthrough

## Goal
Connect the Bridlewood control layer to QuickBooks Online so the app can:
- read chart of accounts
- stage transactions for review
- support rule-based reclass and month-end suggestions

## 1. Create the Intuit app
In the Intuit developer portal:
- create a QuickBooks Online app
- add a redirect URI that matches your local or hosted callback
- use the accounting scope only for this first version

Suggested local callback:
- `http://localhost:8000/api/auth/quickbooks/callback`

## 2. Set environment values
Copy `backend/.env.example` to `backend/.env` and fill in:
- `QBO_CLIENT_ID`
- `QBO_CLIENT_SECRET`
- `QBO_REDIRECT_URI`
- `DATABASE_URL`

## 3. Create the database
Run:
- `backend/schema.sql`
- `backend/sql/001_quickbooks_connection.sql`

Make sure the `entities` table contains Bridlewood `1877-8`.

## 4. Start the API
```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

## 5. Start the OAuth flow
Open:
- `http://localhost:8000/api/auth/quickbooks/connect?entity_code=1877-8`

Copy the `authorization_url` into a browser and approve the connection.

After approval, QuickBooks redirects back to:
- `/api/auth/quickbooks/callback`

That callback stores the realmId, access token, and refresh token in `quickbooks_connections`.

## 6. Pull the chart of accounts
Use:
```bash
curl -X POST http://localhost:8000/api/sync/chart-of-accounts \
  -H 'Content-Type: application/json' \
  -d '{"entity_code":"1877-8","date_from":"2026-02-01","date_to":"2026-02-28"}'
```

## 7. Pull transaction-like records into staging
Use:
```bash
curl -X POST http://localhost:8000/api/sync/transactions \
  -H 'Content-Type: application/json' \
  -d '{"entity_code":"1877-8","date_from":"2026-02-01","date_to":"2026-02-28"}'
```

## 8. What still needs to be built after this step
- refresh-token renewal before expiry
- full GL detail importer, not just CDC staging
- bank and credit-card specific import logic from QuickBooks
- exception queue UI
- suggested journal entry writer
- one-click export back to QuickBooks as draft journals only

## 9. Safe rollout order
1. connect QuickBooks
2. import accounts
3. import February staging data
4. compare to actual GL
5. tighten rules
6. only then build suggested draft journals
