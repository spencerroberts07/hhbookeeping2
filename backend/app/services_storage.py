"""
Cloudflare R2 object storage (S3-compatible).

Every method is failure-tolerant by design:
  - If R2 isn't configured, all methods return None / False without raising.
  - If the underlying boto3 call raises, we log + swallow and return None.
The single rule the rest of the app relies on: storage failures must NEVER
fail a parse / upload endpoint. The parsed result is the load-bearing
artifact; the archived PDF is nice-to-have.

Object-key convention:
    {entity_code}/{document_type}/{year}/{month}/{uuid8}_{sanitized_filename}

Examples:
    1877-8/hh-ap-invoices/2026/02/a3f2c1d4_invoice-52390204.pdf
    1877-8/bank-statements/2026/02/9b1de4ff_FebStatement.pdf
"""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone

from .config import settings

logger = logging.getLogger(__name__)


def content_type_for(filename: str) -> str:
    """Map a filename suffix to a Content-Type for R2 uploads. Defaults
    to application/octet-stream when no suffix matches."""
    name = (filename or "").lower()
    if name.endswith(".csv"):
        return "text/csv"
    if name.endswith((".xlsx", ".xls", ".xlsm")):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if name.endswith(".ods"):
        return "application/vnd.oasis.opendocument.spreadsheet"
    if name.endswith(".pdf"):
        return "application/pdf"
    if name.endswith(".txt"):
        return "text/plain"
    if name.endswith(".json"):
        return "application/json"
    return "application/octet-stream"


def _sanitize_filename(name: str) -> str:
    """Strip path separators and tame whitespace. Keep the extension."""
    if not name:
        return "file"
    # Drop any leading path components a multipart client may have included.
    name = name.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    # Collapse runs of whitespace and disallowed chars.
    name = re.sub(r"\s+", "-", name.strip())
    name = re.sub(r"[^A-Za-z0-9._\-]+", "_", name)
    return name[:120] or "file"


class StorageService:
    """Thin wrapper around an S3 client pointed at Cloudflare R2."""

    def __init__(self) -> None:
        self._client = None
        self._init_attempted = False

    @property
    def client(self):
        """Lazy-init the boto3 client so a misconfigured R2 doesn't crash
        imports during dev. Recomputes once if the config changes (e.g. in
        tests that flip env vars between calls)."""
        if self._init_attempted and self._client is not None:
            return self._client
        if not settings.r2_enabled:
            self._init_attempted = True
            return None
        try:
            import boto3
            from botocore.config import Config
        except ImportError:
            logger.warning("boto3 is not installed — R2 storage disabled")
            self._init_attempted = True
            return None
        try:
            self._client = boto3.client(
                "s3",
                endpoint_url=settings.r2_endpoint_url,
                aws_access_key_id=settings.r2_access_key_id,
                aws_secret_access_key=settings.r2_secret_access_key,
                config=Config(signature_version="s3v4"),
                region_name="auto",
            )
        except Exception as exc:
            logger.warning("R2 client init failed: %r", exc)
            self._client = None
        self._init_attempted = True
        return self._client

    @property
    def bucket(self) -> str:
        return settings.r2_bucket_name

    @property
    def enabled(self) -> bool:
        return settings.r2_enabled and self.client is not None

    def upload_file(
        self,
        *,
        file_bytes: bytes,
        original_filename: str,
        entity_code: str,
        document_type: str,
        content_type: str = "application/pdf",
    ) -> str | None:
        """
        Upload to R2 and return the object_key. None on failure or when R2
        isn't configured — callers should accept None and continue.
        """
        client = self.client
        if not client:
            return None
        now = datetime.now(timezone.utc)
        safe_name = _sanitize_filename(original_filename)
        object_key = (
            f"{entity_code}/{document_type}/"
            f"{now.year}/{now.month:02d}/"
            f"{uuid.uuid4().hex[:8]}_{safe_name}"
        )
        try:
            client.put_object(
                Bucket=self.bucket,
                Key=object_key,
                Body=file_bytes,
                ContentType=content_type,
            )
            return object_key
        except Exception as exc:
            logger.error("R2 upload failed for %s: %r", object_key, exc)
            return None

    def get_presigned_url(
        self,
        object_key: str | None,
        expires_in: int = 3600,
    ) -> str | None:
        """Generate a time-limited GET URL. None on failure or no key."""
        client = self.client
        if not client or not object_key:
            return None
        try:
            return client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": object_key},
                ExpiresIn=expires_in,
            )
        except Exception as exc:
            logger.error("R2 presign failed for %s: %r", object_key, exc)
            return None

    def delete_file(self, object_key: str | None) -> bool:
        """Best-effort delete. Returns True on success; never raises."""
        client = self.client
        if not client or not object_key:
            return False
        try:
            client.delete_object(Bucket=self.bucket, Key=object_key)
            return True
        except Exception as exc:
            logger.error("R2 delete failed for %s: %r", object_key, exc)
            return False


# Module-level singleton — import once, use anywhere.
storage_service = StorageService()
