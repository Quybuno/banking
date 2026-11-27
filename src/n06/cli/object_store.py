"""Lớp tương thích cho mã quản trị bucket của CLI `n06` (T03b, ADR-0012 quyết định 2).

Theo ADR-0012 (Accepted), bộ ký AWS SigV4 và client HTTP S3 đã được chuyển sang
`n06.storage.object_store` để dùng chung giữa CLI (`init-local`, `doctor`) và tác vụ ghi object
(`n06 ingest-statements`, T23). Module này giữ nguyên các biểu tượng cũ cho `n06.cli`.
"""

from __future__ import annotations

from n06.storage.object_store import (
    ALGORITHM,
    BUCKET_NAME_PATTERN,
    DEFAULT_TIMEOUT_SECONDS,
    EMPTY_PAYLOAD_SHA256,
    S3_SERVICE,
    STATUS_CREATED,
    STATUS_ERROR,
    STATUS_EXISTS,
    STATUS_MISSING,
    STATUS_UNREACHABLE,
    BucketNameError,
    BucketResult,
    ObjectStoreConfigError,
    ObjectStoreTarget,
    authorization_header,
    canonical_request,
    create_bucket,
    credential_scope,
    ensure_bucket,
    head_bucket,
    resolve_host_endpoint,
    signing_key,
    string_to_sign,
    target_from_config,
    validate_bucket_name,
)

__all__ = [
    "ALGORITHM",
    "BUCKET_NAME_PATTERN",
    "BucketNameError",
    "BucketResult",
    "DEFAULT_TIMEOUT_SECONDS",
    "EMPTY_PAYLOAD_SHA256",
    "ObjectStoreConfigError",
    "ObjectStoreTarget",
    "S3_SERVICE",
    "STATUS_CREATED",
    "STATUS_ERROR",
    "STATUS_EXISTS",
    "STATUS_MISSING",
    "STATUS_UNREACHABLE",
    "authorization_header",
    "canonical_request",
    "create_bucket",
    "credential_scope",
    "ensure_bucket",
    "head_bucket",
    "resolve_host_endpoint",
    "signing_key",
    "string_to_sign",
    "target_from_config",
    "validate_bucket_name",
]
