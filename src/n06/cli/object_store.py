"""Quản trị bucket cho CLI `n06` (TASK T03b, ADR-0002 mục 1).

Phạm vi hẹp: chỉ phục vụ `n06 init-local` (tạo bucket) và `n06 doctor` (kiểm bucket tồn tại).
Đây không phải thư viện S3 dùng chung: mã đọc/ghi dữ liệu (T22, T23, T25) không được dựa vào
module này.

Không thêm dependency: ký AWS Signature Version 4 bằng thư viện chuẩn (`hmac`, `hashlib`) và gửi
yêu cầu bằng `http.client`. Các hàm ký được kiểm bằng ví dụ ký S3 trong tài liệu AWS (xem
`tests/unit/cli/test_object_store_sigv4.py`) và bằng MinIO thật.

Quy tắc bắt buộc:
- Endpoint, region, credential và tên bucket đều lấy từ cấu hình T01 và biến môi trường.
- Không bao giờ đưa secret vào thông báo: lỗi chỉ gồm mã HTTP và mã lỗi S3 (`<Code>`).
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import re
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
_MAX_ERROR_BODY_BYTES = 65536

# Quy tắc đặt tên bucket S3 (3-63 ký tự, chữ thường, số, dấu gạch ngang; không dấu chấm để tránh
# tên giống địa chỉ IP và vấn đề chứng chỉ).
BUCKET_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")
_S3_ERROR_CODE_PATTERN = re.compile(rb"<Code>([A-Za-z][A-Za-z0-9]{2,63})</Code>")

STATUS_CREATED = "created"
STATUS_EXISTS = "exists"
STATUS_MISSING = "missing"
STATUS_UNREACHABLE = "unreachable"
STATUS_ERROR = "error"


class ObjectStoreConfigError(ValueError):
    """Cấu hình object store không dùng được cho việc quản trị bucket (mã thoát 10)."""


class BucketNameError(ObjectStoreConfigError):
    """Tên bucket vi phạm quy tắc đặt tên của S3."""


@dataclass(frozen=True)
class ObjectStoreTarget:
    """Địa chỉ truy cập từ host tới object store cùng thông tin ký."""

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


# --- AWS Signature Version 4 (chỉ phần cần cho S3) ---------------------------------------------


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
    return name


def resolve_host_endpoint(
    env: Mapping[str, str], *, config_dir: Path | None = None
) -> tuple[str, str, int]:
    """Endpoint object store truy cập từ host: `(scheme, host, port)`.

    Endpoint trong cấu hình thường là tên service trong Docker network, không với tới
    được từ host. Cách suy ra giống `n06 doctor`: host và scheme lấy từ profile `test`, cổng lấy
    từ `N06_MINIO_API_HOST_PORT` (nếu không có thì dùng cổng của profile `test`).
    """
    host_cfg = load_config(
        profile="test",
        config_dir=config_dir if config_dir is not None else DEFAULT_CONFIG_DIR,
        env=env,
    )
    parsed = urlparse(host_cfg.object_store.endpoint)
    host = parsed.hostname or host_cfg.kafka.bootstrap_servers.partition(":")[0]
    port = int(env.get("N06_MINIO_API_HOST_PORT", str(parsed.port or 0)))
    return parsed.scheme or "http", host, port


def target_from_config(
    cfg: AppConfig, env: Mapping[str, str], *, config_dir: Path | None = None
) -> ObjectStoreTarget:
    """Dựng `ObjectStoreTarget` từ cấu hình của profile đang dùng."""
    store = cfg.object_store
    if not store.path_style_access:
        raise ObjectStoreConfigError(
            "object_store.path_style_access=false chưa được hỗ trợ khi quản trị bucket "
            "(chỉ hỗ trợ path-style, đúng cấu hình MinIO của dự án)."
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


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _send(
    target: ObjectStoreTarget,
    method: str,
    bucket: str,
    *,
    timeout: float,
    now: Callable[[], datetime],
) -> _Response:
    """Gửi một yêu cầu có ký tới `/<bucket>`; ném `OSError` khi không kết nối được."""
    path = f"/{bucket}"
    amz_date = now().astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    headers = {
        "host": target.netloc,
        "x-amz-content-sha256": EMPTY_PAYLOAD_SHA256,
        "x-amz-date": amz_date,
    }
    headers["Authorization"] = authorization_header(
        access_key=target.access_key,
        secret_key=target.secret_key,
        region=target.region,
        amz_date=amz_date,
        method=method,
        path=path,
        query=(),
        headers=headers,
        payload_sha256=EMPTY_PAYLOAD_SHA256,
    )
    connection_class = (
        http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
    )
    connection = connection_class(target.host, target.port, timeout=timeout)
    try:
        connection.request(method, path, headers=headers)
        response = connection.getresponse()
        body = response.read(_MAX_ERROR_BODY_BYTES) if method != "HEAD" else b""
        match = _S3_ERROR_CODE_PATTERN.search(body)
        return _Response(response.status, match.group(1).decode("ascii") if match else None)
    finally:
        connection.close()


def _unreachable(target: ObjectStoreTarget, bucket: str, exc: Exception) -> BucketResult:
    return BucketResult(
        bucket,
        STATUS_UNREACHABLE,
        f"không kết nối được object store tại {target.host}:{target.port} "
        f"({type(exc).__name__}); MinIO chưa chạy?",
    )


def _http_error(response: _Response, action: str) -> BucketResult:
    code = f", mã lỗi S3 {response.s3_code}" if response.s3_code else ""
    if response.status == 403:
        reason = "bị từ chối (HTTP 403): sai access key/secret key hoặc không đủ quyền"
    else:
        reason = f"HTTP {response.status}"
    return BucketResult("", STATUS_ERROR, f"{action} {reason}{code}")


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
    except (OSError, http.client.HTTPException) as exc:
        return _unreachable(target, bucket, exc)
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
    except (OSError, http.client.HTTPException) as exc:
        return _unreachable(target, bucket, exc)
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
