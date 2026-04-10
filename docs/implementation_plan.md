# Next practical step: make this real

## 1. Freeze the Bridlewood rulebook
Use the posting rules matrix and recurring month-end sheets as the approved accounting design.
Do not start live automation before this rulebook is signed off.

## 2. Create the database
Use a managed PostgreSQL instance.
Recommended:
- Neon
- Supabase
- Railway

Run the SQL in `backend/schema.sql`.

## 3. Load the seed data
Load:
- chart of accounts
- posting rules
- recurring month-end rules
- cash balancing map

These seeds are in `backend/seeds/`.

## 4. Connect QuickBooks as the book of record
The first integration should pull:
- chart of accounts
- general ledger detail
- bank feed transactions
- credit-card transactions
- journal entries

The first outbound integration should only push:
- suggested reclass journals
- suggested month-end journals

Do not auto-post without review in the first release.

## 5. Build the importers in this order
### Importer A
Daily cash balancing workbook and POS support

### Importer B
HH AP statement, remittances, and invoice-number matching

### Importer C
AP Direct invoice OCR and line splitting

### Importer D
E-commerce pickup gateway CSVs and payout matching

### Importer E
Payroll support and payroll batch tie-out

## 6. Build the exception queue
Open exceptions should include:
- unknown legacy cash line
- unmatched HH invoice
- AP Direct split below OCR confidence threshold
- e-commerce payout mismatch
- payroll support mismatch
- recurring JE amount materially different from rule

## 7. Use February 2026 as the proof month
Do not use it yet as the final acceptance run.
Use it first as the validation month:
- import everything
- evaluate rules
- inspect exceptions
- tighten mappings
- rerun

Then repeat on January or March before moving to live use.

## 8. Production controls before auto-post
You need:
- audit log
- approver on every posted suggestion
- source-document links
- confidence scores
- closed-month lock
- rollback process for imported transactions
