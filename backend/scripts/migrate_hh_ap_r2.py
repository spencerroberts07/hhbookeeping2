"""
One-shot migration: hh_ap_documents.file_bytes → R2.

Move every PDF currently held inline in hh_ap_documents.file_bytes
to Cloudflare R2 and record its object key in r2_object_key. The
script is idempotent — it only processes rows where file_bytes IS
NOT NULL AND r2_object_key IS NULL.

Run from a Render shell where R2_* env vars are populated:

    cd backend && python -m scripts.migrate_hh_ap_r2

The script reports per-batch progress and ends with a summary:
migrated rows, failed rows, MB freed from Postgres.

Safe to re-run if it dies mid-way: rows already migrated have
file_bytes=NULL and r2_object_key populated, so the WHERE filter
skips them.

After this script reports 100 % success across all rows, you can
schedule the followup:

    ALTER TABLE hh_ap_documents DROP COLUMN file_bytes;
"""
from __future__ import annotations

import sys
import time
from typing import Any

from sqlalchemy import text

# Add backend/ to sys.path when invoked as `python scripts/migrate_hh_ap_r2.py`
# (the `python -m scripts.migrate_hh_ap_r2` form handles this automatically).
if __name__ == "__main__" and not __package__:
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.db import db_session
from app.services_storage import storage_service


BATCH_SIZE = 10  # rows per progress print


def _human_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / 1024 / 1024:.1f} MB"
    return f"{n / 1024 / 1024 / 1024:.2f} GB"


def main() -> int:
    if not settings.r2_enabled:
        print(
            "ERROR: R2 is not configured in the environment.\n"
            "Set R2_BUCKET_NAME / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / "
            "R2_ENDPOINT_URL before running.",
            file=sys.stderr,
        )
        return 2

    # Pull every row needing migration.
    with db_session() as session:
        pending = session.execute(
            text(
                """
                SELECT d.id, d.source_filename, d.document_type,
                       d.file_size_bytes, d.file_bytes,
                       e.entity_code
                  FROM hh_ap_documents d
                  JOIN entities e ON e.id = d.entity_id
                 WHERE d.file_bytes IS NOT NULL
                   AND d.r2_object_key IS NULL
              ORDER BY d.created_at ASC
                """
            )
        ).mappings().all()

    total = len(pending)
    print(f"Found {total} hh_ap_documents rows to migrate to R2.")
    if total == 0:
        print("Nothing to do.")
        return 0

    migrated = 0
    failed = 0
    bytes_freed = 0
    started_at = time.time()

    for idx, row in enumerate(pending, start=1):
        doc_id = row["id"]
        filename = row["source_filename"] or "document.pdf"
        file_bytes: bytes = row["file_bytes"]
        size = row["file_size_bytes"] or len(file_bytes or b"")
        entity_code = row["entity_code"] or "unknown"

        # Determine content_type from filename for R2 metadata. PDFs
        # are by far the common case here.
        if filename.lower().endswith(".pdf"):
            content_type = "application/pdf"
        elif filename.lower().endswith(".csv"):
            content_type = "text/csv"
        else:
            content_type = "application/octet-stream"

        try:
            object_key = storage_service.upload_file(
                file_bytes=file_bytes,
                original_filename=filename,
                entity_code=entity_code,
                document_type="hh-ap-documents",
                content_type=content_type,
            )
        except Exception as exc:
            object_key = None
            print(f"  [{idx}/{total}] {filename}: upload raised {exc!r}")

        if not object_key:
            failed += 1
            print(f"  [{idx}/{total}] {filename}: upload returned None — skipped")
            continue

        # Update the row in its own short transaction so a later
        # failure doesn't roll back the work already done.
        try:
            with db_session() as session:
                session.execute(
                    text(
                        """
                        UPDATE hh_ap_documents
                           SET r2_object_key = :key,
                               file_bytes = NULL,
                               updated_at = NOW()
                         WHERE id = :id
                        """
                    ),
                    {"key": object_key, "id": doc_id},
                )
        except Exception as exc:
            # R2 has the bytes but DB update failed. Don't double-upload
            # — try to delete the freshly written R2 object so the
            # next run starts clean.
            print(
                f"  [{idx}/{total}] {filename}: DB update failed ({exc!r}); "
                f"rolling back R2 object {object_key}"
            )
            storage_service.delete_file(object_key)
            failed += 1
            continue

        migrated += 1
        bytes_freed += int(size)
        if idx % BATCH_SIZE == 0 or idx == total:
            elapsed = time.time() - started_at
            rate = idx / elapsed if elapsed > 0 else 0
            print(
                f"  [{idx}/{total}] migrated={migrated} failed={failed} "
                f"freed={_human_bytes(bytes_freed)} ({rate:.1f} rows/s)"
            )

    print()
    print("=" * 60)
    print(f"Migration complete in {time.time() - started_at:.1f}s")
    print(f"  migrated:    {migrated} rows")
    print(f"  failed:      {failed} rows")
    print(f"  bytes_freed: {_human_bytes(bytes_freed)}")
    if failed == 0 and migrated > 0:
        print()
        print("All rows migrated. Safe to schedule:")
        print("  ALTER TABLE hh_ap_documents DROP COLUMN file_bytes;")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
