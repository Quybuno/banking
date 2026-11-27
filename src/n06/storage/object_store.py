"""Client S3/MinIO bằng thư viện chuẩn (AWS SigV4 + `http.client`) cho `n06` (T03b, T23, ADR-0012).

Không thêm dependency: ký AWS Signature Version 4 bằng `hmac`/`hashlib` và gửi yêu cầu qua
`http.client`. Hỗ trợ quản trị bucket (`HEAD`/`PUT` bucket) và ghi/kiểm object bất biến
(`HEAD`/`PUT` object với `Content-Length`, `x-amz-content-sha256`, `x-amz-meta-sha256` và thử lại
có giới hạn cho lỗi tạm thời).

Quy tắc bắt buộc:
- Endpoint, region, credential và tên bucket lấy từ cấu hình T01 và biến môi trường.
- Không bao giờ đưa secret vào thông báo lỗi: chỉ gồm mã HTTP, mã lỗi S3 (`<Code>`) hoặc tên lỗi.
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import re
import socket
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, urlparse

from n06.config import DEFAULT_CONFIG_DIR, AppConfig, load_config

ALGORITHM = "AWS4-HMAC-SHA256"
S3_SERVICE = "s3"
EMPTY_PAYLOAD_SHA256 = hashlib.sha256(b"").hexdigest()
DEFAULT_TIMEOUT_SECONDS = 5.0
DEFAULT_OBJECT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 0.25
_STREAM_CHUNK_BYTES = 1 << 20  # 1 MiB
_MAX_ERROR_BODY_BYTES = 65536
_RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})

# Quy tắc đặt tên bucket S3 (3-63 ký tự, chữ thường, số, dấu gạch ngang; không dấu chấm để tránh
# tên giống địa chỉ IP và vấn đề chứng chỉ).
BUCKET_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")
_S3_ERROR_CODE_PATTERN = re.compile(rb"<Code>([A-Za-z][A-Za-z0-9]{2,63})</Code>")
_RESERVED_BUCKET_PREFIXES = ("xn--", "sthree-")
_RESERVED_BUCKET_SUFFIXES = ("-s3alias", "--ol-s3")
_REGION_PATTERN = re.compile(r"^[a-z0-9-]{1,64}$")
_CREDENTIAL_PATTERN = re.compile(r"^[\x21-\x7e]{1,256}$")
_META_KEY_PATTERN = re.compile(r"^[a-z0-9-]+$")

STATUS_CREATED = "created"
STATUS_EXISTS = "exists"
STATUS_MISSING = "missing"
STATUS_UNREACHABLE = "unreachable"
STATUS_ERROR = "error"


class ObjectStoreConfigError(ValueError):
    """Cấu hình object store không dùng được (mã thoát 10)."""


class BucketNameError(ObjectStoreConfigError):
    """Tên bucket vi phạm quy tắc đặt tên của S3."""


class ObjectStoreError(RuntimeError):
    """Lỗi truyền tải hoặc HTTP khi thao tác object trên MinIO/S3 (không chứa secret)."""


class ObjectIntegrityError(ObjectStoreError):
    """Vi phạm tính bất biến: object đã tồn tại nhưng thiếu hoặc lệch `sha256`."""


@dataclass(frozen=True)
class ObjectStoreTarget:
    """Địa chỉ truy cập tới object store cùng thông tin ký."""

    scheme: str
    host: str
    port: int
    region: str
    access_key: str
    secret_key: str = field(repr=False)

    @property
    def netloc(self) -> str:
        """Giá trị header `Host`: bỏ cổng khi là cổng mặc định của scheme."""
        default_port = 443 if self.scheme == "https" else 80
        return self.host if self.port == default_port else f"{self.host}:{self.port}"


@dataclass(frozen=True)
class BucketResult:
    """Trạng thái một bucket sau khi kiểm hoặc tạo; `detail` không bao giờ chứa secret."""

    bucket: str
    status: str
    detail: str

    @property
    def ready(self) -> bool:
        return self.status in (STATUS_CREATED, STATUS_EXISTS)


@dataclass(frozen=True)
class ObjectHead:
    """Kết quả `HEAD` một object trên S3/MinIO."""

    exists: bool
    content_length: int | None
    metadata: dict[str, str]

    @property
    def sha256(self) -> str | None:
        return self.metadata.get("sha256")


# --- AWS Signature Version 4 (chỉ phần cần cho S3) ---------------------------------------------


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_sha256_and_size(path: Path) -> tuple[str, int]:
    """Tính `(sha256_hex, kích thước byte)` của một file bằng cách đọc theo khối."""
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(_STREAM_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _hmac_sha256(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def canonical_request(
    *,
    method: str,
    path: str,
    query: Sequence[tuple[str, str]],
    headers: Mapping[str, str],
    payload_sha256: str,
) -> tuple[str, str]:
    """Trả `(CanonicalRequest, SignedHeaders)` theo SigV4; mọi header trong `headers` đều được ký.

    `path` là đường dẫn chưa mã hóa (ví dụ `/test$file.text`); S3 không mã hóa hai lần.
    """
    canonical_uri = quote(path or "/", safe="/")
    encoded_query = sorted((quote(k, safe=""), quote(v, safe="")) for k, v in query)
    canonical_query = "&".join(f"{k}={v}" for k, v in encoded_query)
    normalized = sorted((k.strip().lower(), " ".join(v.split())) for k, v in headers.items())
    canonical_headers = "".join(f"{k}:{v}\n" for k, v in normalized)
    signed_headers = ";".join(k for k, _ in normalized)
    text = "\n".join(
        [
            method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_sha256,
        ]
    )
    return text, signed_headers


def credential_scope(amz_date: str, region: str) -> str:
    return f"{amz_date[:8]}/{region}/{S3_SERVICE}/aws4_request"


def string_to_sign(*, amz_date: str, region: str, canonical_request_text: str) -> str:
    return "\n".join(
        [
            ALGORITHM,
            amz_date,
            credential_scope(amz_date, region),
            _sha256_hex(canonical_request_text.encode("utf-8")),
        ]
    )


def signing_key(*, secret_key: str, amz_date: str, region: str) -> bytes:
    key = _hmac_sha256(("AWS4" + secret_key).encode("utf-8"), amz_date[:8])
    key = _hmac_sha256(key, region)
    key = _hmac_sha256(key, S3_SERVICE)
    return _hmac_sha256(key, "aws4_request")


def authorization_header(
    *,
    access_key: str,
    secret_key: str,
    region: str,
    amz_date: str,
    method: str,
    path: str,
    query: Sequence[tuple[str, str]],
    headers: Mapping[str, str],
    payload_sha256: str,
) -> str:
    """Giá trị header `Authorization` cho một yêu cầu S3."""
    request_text, signed_headers = canonical_request(
        method=method, path=path, query=query, headers=headers, payload_sha256=payload_sha256
    )
    to_sign = string_to_sign(amz_date=amz_date, region=region, canonical_request_text=request_text)
    key = signing_key(secret_key=secret_key, amz_date=amz_date, region=region)
    signature = hmac.new(key, to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    return (
        f"{ALGORITHM} Credential={access_key}/{credential_scope(amz_date, region)},"
        f"SignedHeaders={signed_headers},Signature={signature}"
    )


# --- Cấu hình và địa chỉ từ host ---------------------------------------------------------------


def validate_bucket_name(name: str) -> str:
    """Trả `name` nếu hợp lệ theo quy tắc S3, ngược lại ném `BucketNameError`."""
    if not BUCKET_NAME_PATTERN.fullmatch(name):
        raise BucketNameError(
            f"Tên bucket {name!r} không hợp lệ: cần 3-63 ký tự gồm chữ thường, số và dấu gạch "
            "ngang, bắt đầu và kết thúc bằng chữ hoặc số (object_store.bucket)."
        )
    if name.startswith(_RESERVED_BUCKET_PREFIXES) or name.endswith(_RESERVED_BUCKET_SUFFIXES):
        raise BucketNameError(
            f"Tên bucket {name!r} dùng tiền tố hoặc hậu tố mà S3 dành riêng "
            "(xn--, sthree-, -s3alias, --ol-s3) (object_store.bucket)."
        )
    return name


def _validate_object_key(key: str) -> str:
    cleaned = key.lstrip("/")
    if not cleaned or cleaned.endswith("/") or ".." in cleaned.split("/"):
        raise ObjectStoreConfigError(f"Khóa object không hợp lệ: {key!r}.")
    return cleaned


def resolve_host_endpoint(
    env: Mapping[str, str], *, config_dir: Path | None = None
) -> tuple[str, str, int]:
    """Endpoint object store truy cập từ host: `(scheme, host, port)`."""
    host_cfg = load_config(
        profile="test",
        config_dir=config_dir if config_dir is not None else DEFAULT_CONFIG_DIR,
        env=env,
    )
    parsed = urlparse(host_cfg.object_store.endpoint)
    host = parsed.hostname or host_cfg.kafka.bootstrap_servers.partition(":")[0]
    raw_port = env.get("N06_MINIO_API_HOST_PORT", str(parsed.port or 0)).strip()
    if not raw_port.isascii() or not raw_port.isdigit() or not 1 <= int(raw_port) <= 65535:
        raise ObjectStoreConfigError(
            "N06_MINIO_API_HOST_PORT phải là số nguyên từ 1 đến 65535 "
            "(cổng MinIO truy cập từ host)."
        )
    return parsed.scheme or "http", host, int(raw_port)


def target_from_config(
    cfg: AppConfig, env: Mapping[str, str], *, config_dir: Path | None = None
) -> ObjectStoreTarget:
    """Dựng `ObjectStoreTarget` từ cấu hình của profile đang dùng."""
    store = cfg.object_store
    if not store.path_style_access:
        raise ObjectStoreConfigError(
            "object_store.path_style_access=false chưa được hỗ trợ "
            "(chỉ hỗ trợ path-style, đúng cấu hình MinIO của dự án)."
        )
    for field_path, value, pattern in (
        ("object_store.region", store.region, _REGION_PATTERN),
        ("object_store.access_key", store.access_key, _CREDENTIAL_PATTERN),
        ("object_store.secret_key", store.secret_key, _CREDENTIAL_PATTERN),
    ):
        if not pattern.fullmatch(value):
            raise ObjectStoreConfigError(
                f"{field_path} chứa ký tự không hợp lệ hoặc có độ dài ngoài giới hạn "
                "(chỉ nhận ký tự ASCII in được, không khoảng trắng)."
            )
    scheme, host, port = resolve_host_endpoint(env, config_dir=config_dir)
    return ObjectStoreTarget(
        scheme=scheme,
        host=host,
        port=port,
        region=store.region,
        access_key=store.access_key,
        secret_key=store.secret_key,
    )


# --- Gọi HTTP ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Response:
    status: int
    s3_code: str | None
    headers: dict[str, str] = field(default_factory=dict)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _send_request(
    target: ObjectStoreTarget,
    method: str,
    path: str,
    *,
    timeout: float,
    now: Callable[[], datetime],
    payload_sha256: str = EMPTY_PAYLOAD_SHA256,
    content_length: int | None = None,
    extra_headers: Mapping[str, str] | None = None,
    body_bytes: bytes | None = None,
    body_file: Path | None = None,
) -> _Response:
    """Gửi yêu cầu S3 có ký tới `path` (chưa mã hóa URI); hỗ trợ truyền thân theo luồng."""
    amz_date = now().astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    headers: dict[str, str] = {
        "host": target.netloc,
        "x-amz-content-sha256": payload_sha256,
        "x-amz-date": amz_date,
    }
    if content_length is not None:
        headers["content-length"] = str(content_length)
    if extra_headers:
        for k, v in extra_headers.items():
            headers[k.strip().lower()] = v
    headers["Authorization"] = authorization_header(
        access_key=target.access_key,
        secret_key=target.secret_key,
        region=target.region,
        amz_date=amz_date,
        method=method,
        path=path,
        query=(),
        headers=headers,
        payload_sha256=payload_sha256,
    )
    encoded_path = quote(path, safe="/")
    connection_class = (
        http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
    )
    connection = connection_class(target.host, target.port, timeout=timeout)
    try:
        connection.putrequest(method, encoded_path, skip_host=True)
        for name, value in headers.items():
            connection.putheader(name, value)
        connection.endheaders()
        if body_bytes is not None:
            if body_bytes:
                connection.send(body_bytes)
        elif body_file is not None:
            with body_file.open("rb") as handle:
                while chunk := handle.read(_STREAM_CHUNK_BYTES):
                    connection.send(chunk)
        response = connection.getresponse()
        body = response.read(_MAX_ERROR_BODY_BYTES) if method != "HEAD" else b""
        match = _S3_ERROR_CODE_PATTERN.search(body)
        resp_headers = {k.lower(): v for k, v in response.getheaders()}
        return _Response(
            response.status,
            match.group(1).decode("ascii") if match else None,
            resp_headers,
        )
    finally:
        connection.close()


def _send(
    target: ObjectStoreTarget,
    method: str,
    bucket: str,
    *,
    timeout: float,
    now: Callable[[], datetime],
) -> _Response:
    """Gửi yêu cầu rỗng có ký tới `/<bucket>`."""
    return _send_request(target, method, f"/{bucket}", timeout=timeout, now=now)


def _transport_failure(target: ObjectStoreTarget, bucket: str, exc: Exception) -> BucketResult:
    where = f"{target.host}:{target.port} ({type(exc).__name__})"
    if isinstance(exc, ConnectionRefusedError | socket.gaierror):
        return BucketResult(
            bucket,
            STATUS_UNREACHABLE,
            f"không kết nối được object store tại {where}; MinIO chưa chạy?",
        )
    if isinstance(exc, ValueError):
        return BucketResult(
            bucket, STATUS_ERROR, f"không gửi được yêu cầu tới object store ({type(exc).__name__})"
        )
    return BucketResult(
        bucket,
        STATUS_ERROR,
        f"object store tại {where} không trả lời đúng (timeout, phản hồi lỗi hoặc lỗi TLS)",
    )


def _http_error(response: _Response, action: str) -> BucketResult:
    code = f", mã lỗi S3 {response.s3_code}" if response.s3_code else ""
    if response.status == 403:
        reason = "bị từ chối (HTTP 403): sai access key/secret key hoặc không đủ quyền"
    else:
        reason = f"HTTP {response.status}"
    return BucketResult("", STATUS_ERROR, f"{action} {reason}{code}")


# --- Quản trị bucket (T03b) --------------------------------------------------------------------


def head_bucket(
    target: ObjectStoreTarget,
    bucket: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    now: Callable[[], datetime] = _utc_now,
) -> BucketResult:
    """Kiểm bucket có tồn tại không: `exists`, `missing`, `unreachable` hoặc `error`."""
    validate_bucket_name(bucket)
    try:
        response = _send(target, "HEAD", bucket, timeout=timeout, now=now)
    except (OSError, http.client.HTTPException, ValueError) as exc:
        return _transport_failure(target, bucket, exc)
    if response.status == 200:
        return BucketResult(bucket, STATUS_EXISTS, "bucket đã tồn tại")
    if response.status == 404:
        return BucketResult(bucket, STATUS_MISSING, "bucket chưa tồn tại")
    result = _http_error(response, "kiểm bucket")
    return BucketResult(bucket, result.status, result.detail)


def create_bucket(
    target: ObjectStoreTarget,
    bucket: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    now: Callable[[], datetime] = _utc_now,
) -> BucketResult:
    """Tạo bucket (`PUT /<bucket>`). Đã có do chính credential này sở hữu thì coi là `exists`."""
    validate_bucket_name(bucket)
    try:
        response = _send(target, "PUT", bucket, timeout=timeout, now=now)
    except (OSError, http.client.HTTPException, ValueError) as exc:
        return _transport_failure(target, bucket, exc)
    if response.status == 200:
        return BucketResult(bucket, STATUS_CREATED, "đã tạo bucket")
    if response.status == 409 and response.s3_code == "BucketAlreadyOwnedByYou":
        return BucketResult(bucket, STATUS_EXISTS, "bucket đã tồn tại")
    if response.status == 409 and response.s3_code == "BucketAlreadyExists":
        return BucketResult(
            bucket, STATUS_ERROR, "tên bucket đã được tài khoản khác sở hữu (BucketAlreadyExists)"
        )
    result = _http_error(response, "tạo bucket")
    return BucketResult(bucket, result.status, result.detail)


def ensure_bucket(
    target: ObjectStoreTarget,
    bucket: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    now: Callable[[], datetime] = _utc_now,
) -> BucketResult:
    """Đảm bảo bucket tồn tại, an toàn khi chạy lại: có rồi thì không gửi `PUT`."""
    current = head_bucket(target, bucket, timeout=timeout, now=now)
    if current.status != STATUS_MISSING:
        return current
    return create_bucket(target, bucket, timeout=timeout, now=now)


# --- Thao tác object (T23, ADR-0012 quyết định 2) ----------------------------------------------


def _send_with_retry(
    send_once: Callable[[], _Response],
    *,
    action: str,
    max_retries: int,
    retry_backoff_seconds: float,
    sleep: Callable[[float], None],
) -> _Response:
    attempts = max(1, max_retries)
    for attempt in range(1, attempts + 1):
        try:
            response = send_once()
        except (OSError, http.client.HTTPException) as exc:
            if attempt >= attempts:
                raise ObjectStoreError(
                    f"{action} thất bại sau {attempts} lần thử ({type(exc).__name__})."
                ) from exc
            sleep(retry_backoff_seconds * attempt)
            continue
        if response.status in _RETRYABLE_HTTP_STATUSES and attempt < attempts:
            sleep(retry_backoff_seconds * attempt)
            continue
        return response
    raise ObjectStoreError(f"{action} thất bại.")  # pragma: no cover


def _object_http_error(response: _Response, action: str) -> ObjectStoreError:
    code = f", mã lỗi S3 {response.s3_code}" if response.s3_code else ""
    if response.status == 403:
        return ObjectStoreError(
            f"{action} bị từ chối (HTTP 403): sai access key/secret key hoặc không đủ quyền{code}"
        )
    return ObjectStoreError(f"{action} thất bại: HTTP {response.status}{code}")


def _metadata_headers(metadata: Mapping[str, str] | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    if not metadata:
        return headers
    for key, value in metadata.items():
        clean_key = key.strip().lower()
        if not _META_KEY_PATTERN.fullmatch(clean_key):
            raise ObjectStoreConfigError(f"Tên metadata S3 không hợp lệ: {key!r}.")
        headers[f"x-amz-meta-{clean_key}"] = str(value)
    return headers


def head_object(
    target: ObjectStoreTarget,
    bucket: str,
    key: str,
    *,
    timeout: float = DEFAULT_OBJECT_TIMEOUT_SECONDS,
    now: Callable[[], datetime] = _utc_now,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> ObjectHead:
    """`HEAD /<bucket>/<key>`; trả `ObjectHead(exists=False)` khi 404."""
    validate_bucket_name(bucket)
    clean_key = _validate_object_key(key)
    path = f"/{bucket}/{clean_key}"
    response = _send_with_retry(
        lambda: _send_request(target, "HEAD", path, timeout=timeout, now=now),
        action=f"HEAD object {bucket}/{clean_key}",
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        sleep=sleep,
    )
    if response.status == 404:
        return ObjectHead(exists=False, content_length=None, metadata={})
    if response.status != 200:
        raise _object_http_error(response, f"HEAD object {bucket}/{clean_key}")
    raw_len = response.headers.get("content-length")
    content_length = int(raw_len) if raw_len is not None and raw_len.isdigit() else None
    metadata = {
        k.removeprefix("x-amz-meta-"): v
        for k, v in response.headers.items()
        if k.startswith("x-amz-meta-")
    }
    return ObjectHead(exists=True, content_length=content_length, metadata=metadata)


def put_object_bytes(
    target: ObjectStoreTarget,
    bucket: str,
    key: str,
    data: bytes,
    *,
    content_type: str = "application/octet-stream",
    metadata: Mapping[str, str] | None = None,
    timeout: float = DEFAULT_OBJECT_TIMEOUT_SECONDS,
    now: Callable[[], datetime] = _utc_now,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """`PUT` object từ `bytes` với `Content-Length` và `x-amz-meta-sha256`; trả `sha256`."""
    validate_bucket_name(bucket)
    clean_key = _validate_object_key(key)
    sha256 = _sha256_hex(data)
    meta = {**(metadata or {}), "sha256": sha256}
    extra = {"content-type": content_type, **_metadata_headers(meta)}
    path = f"/{bucket}/{clean_key}"
    response = _send_with_retry(
        lambda: _send_request(
            target,
            "PUT",
            path,
            timeout=timeout,
            now=now,
            payload_sha256=sha256,
            content_length=len(data),
            extra_headers=extra,
            body_bytes=data,
        ),
        action=f"PUT object {bucket}/{clean_key}",
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        sleep=sleep,
    )
    if response.status not in (200, 201):
        raise _object_http_error(response, f"PUT object {bucket}/{clean_key}")
    return sha256


def put_object_file(
    target: ObjectStoreTarget,
    bucket: str,
    key: str,
    file_path: Path,
    *,
    content_type: str = "application/octet-stream",
    metadata: Mapping[str, str] | None = None,
    sha256: str | None = None,
    size: int | None = None,
    timeout: float = DEFAULT_OBJECT_TIMEOUT_SECONDS,
    now: Callable[[], datetime] = _utc_now,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """`PUT` object đọc theo luồng từ file đĩa với `Content-Length` và `x-amz-meta-sha256`."""
    validate_bucket_name(bucket)
    clean_key = _validate_object_key(key)
    if sha256 is None or size is None:
        computed_sha, computed_size = file_sha256_and_size(file_path)
        sha256 = sha256 or computed_sha
        size = size if size is not None else computed_size
    meta = {**(metadata or {}), "sha256": sha256}
    extra = {"content-type": content_type, **_metadata_headers(meta)}
    path = f"/{bucket}/{clean_key}"
    response = _send_with_retry(
        lambda: _send_request(
            target,
            "PUT",
            path,
            timeout=timeout,
            now=now,
            payload_sha256=sha256,
            content_length=size,
            extra_headers=extra,
            body_file=file_path,
        ),
        action=f"PUT object {bucket}/{clean_key}",
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        sleep=sleep,
    )
    if response.status not in (200, 201):
        raise _object_http_error(response, f"PUT object {bucket}/{clean_key}")
    return sha256


def _check_existing_immutable(
    bucket: str, key: str, head: ObjectHead, expected_sha256: str
) -> None:
    if head.sha256 is None:
        raise ObjectIntegrityError(
            f"Object {bucket}/{key} đã tồn tại nhưng thiếu metadata x-amz-meta-sha256; "
            "từ chối ghi đè (ADR-0012)."
        )
    if head.sha256 != expected_sha256:
        raise ObjectIntegrityError(
            f"Object {bucket}/{key} đã tồn tại với sha256 khác "
            f"({head.sha256} != {expected_sha256}); từ chối ghi đè (ADR-0002 mục 4)."
        )


def put_object_immutable_bytes(
    target: ObjectStoreTarget,
    bucket: str,
    key: str,
    data: bytes,
    *,
    content_type: str = "application/octet-stream",
    metadata: Mapping[str, str] | None = None,
    timeout: float = DEFAULT_OBJECT_TIMEOUT_SECONDS,
    now: Callable[[], datetime] = _utc_now,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """`HEAD` rồi `PUT` bất biến cho `bytes`: trả `'uploaded'` hoặc `'skipped'`."""
    expected_sha = _sha256_hex(data)
    head = head_object(
        target,
        bucket,
        key,
        timeout=timeout,
        now=now,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        sleep=sleep,
    )
    if head.exists:
        _check_existing_immutable(bucket, key, head, expected_sha)
        return "skipped"
    put_object_bytes(
        target,
        bucket,
        key,
        data,
        content_type=content_type,
        metadata=metadata,
        timeout=timeout,
        now=now,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        sleep=sleep,
    )
    return "uploaded"


def put_object_immutable_file(
    target: ObjectStoreTarget,
    bucket: str,
    key: str,
    file_path: Path,
    *,
    content_type: str = "application/octet-stream",
    metadata: Mapping[str, str] | None = None,
    expected_sha256: str | None = None,
    timeout: float = DEFAULT_OBJECT_TIMEOUT_SECONDS,
    now: Callable[[], datetime] = _utc_now,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """`HEAD` rồi `PUT` bất biến từ file đĩa: trả `'uploaded'` hoặc `'skipped'`."""
    actual_sha, size = file_sha256_and_size(file_path)
    if expected_sha256 is not None and actual_sha != expected_sha256:
        raise ObjectIntegrityError(
            f"File cục bộ {file_path.name} có sha256={actual_sha} khác kỳ vọng {expected_sha256}."
        )
    head = head_object(
        target,
        bucket,
        key,
        timeout=timeout,
        now=now,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        sleep=sleep,
    )
    if head.exists:
        _check_existing_immutable(bucket, key, head, actual_sha)
        return "skipped"
    put_object_file(
        target,
        bucket,
        key,
        file_path,
        content_type=content_type,
        metadata=metadata,
        sha256=actual_sha,
        size=size,
        timeout=timeout,
        now=now,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        sleep=sleep,
    )
    return "uploaded"
