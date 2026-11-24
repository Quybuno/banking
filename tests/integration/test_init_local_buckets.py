"""Integration test `n06 init-local` tạo bucket và `n06 doctor` kiểm bucket trên MinIO thật (T03b).

Yêu cầu: `docker compose --profile core up -d` và file `.env` có credential khớp với Compose.
Không skip khi thiếu hạ tầng (thiếu MinIO là lỗi môi trường kiểm thử).

Test chạy trên profile `benchmark` nhưng dùng bucket tên duy nhất qua `N06__OBJECT_STORE__BUCKET`,
nên không tạo hay xóa bucket thật `n06-benchmark-lakehouse` (tên bucket đúng theo profile đã có
test cấu hình khóa ở `tests/unit/config/test_config.py`). Bucket test được xóa trong `finally`.
"""

from __future__ import annotations

import http.client
import io
import json
import os
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path

import pytest

from n06.cli.doctor import run_doctor
from n06.cli.init_local import parse_dotenv_file, run_init_local
from n06.cli.object_store import (
    EMPTY_PAYLOAD_SHA256,
    STATUS_EXISTS,
    STATUS_MISSING,
    ObjectStoreTarget,
    authorization_header,
    head_bucket,
    target_from_config,
)
from n06.config import REPO_ROOT, load_config

ENV_FILE = REPO_ROOT / ".env"
S3_NAMESPACE = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def _merged_env(bucket: str) -> dict[str, str]:
    env: dict[str, str] = {}
    if ENV_FILE.is_file():
        env.update(parse_dotenv_file(ENV_FILE))
    env.update(os.environ)
    env["N06__OBJECT_STORE__BUCKET"] = bucket
    return env


def _signed_request(target: ObjectStoreTarget, method: str, path: str) -> tuple[int, bytes]:
    """Yêu cầu có ký tới MinIO thật, chỉ dùng trong test để liệt kê và dọn bucket."""
    amz_date = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
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
    connection = http.client.HTTPConnection(target.host, target.port, timeout=10)
    try:
        connection.request(method, path, headers=headers)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def _creation_date(target: ObjectStoreTarget, bucket: str) -> str:
    status, body = _signed_request(target, "GET", "/")
    assert status == 200, f"ListBuckets thất bại: HTTP {status}"
    for node in ET.fromstring(body).iter(f"{S3_NAMESPACE}Bucket"):
        if node.findtext(f"{S3_NAMESPACE}Name") == bucket:
            return node.findtext(f"{S3_NAMESPACE}CreationDate") or ""
    raise AssertionError(f"Bucket {bucket} không có trong ListBuckets")


def _init_local(bucket: str, *, as_json: bool = True) -> tuple[int, str]:
    out = io.StringIO()
    code = run_init_local(
        profile="benchmark",
        target_path=ENV_FILE,
        env=_merged_env(bucket),
        as_json=as_json,
        out=out,
    )
    return code, out.getvalue()


def _doctor_bucket_item(bucket: str, monkeypatch: pytest.MonkeyPatch) -> tuple[dict, str]:
    monkeypatch.setenv("N06__OBJECT_STORE__BUCKET", bucket)
    out = io.StringIO()
    run_doctor(profile="benchmark", env_file=ENV_FILE, as_json=True, out=out)
    report = json.loads(out.getvalue())
    items = [c for c in report["checks"] if c["check_id"].startswith("bucket.")]
    assert len(items) == 1, f"Doctor phải có đúng một check bucket, nhận {items}"
    return items[0], out.getvalue()


def test_init_local_creates_bucket_idempotently_and_doctor_reports_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if not ENV_FILE.is_file():
        pytest.fail(f"Thiếu {ENV_FILE}: chạy `n06 init-local` hoặc `bash scripts/smoke.sh` trước.")
    bucket = f"n06-it-{uuid.uuid4().hex[:10]}"
    env = _merged_env(bucket)
    cfg = load_config(profile="benchmark", env=env)
    assert cfg.object_store.bucket == bucket  # bucket lấy từ cấu hình, không hardcode
    target = target_from_config(cfg, env)

    precondition = head_bucket(target, bucket)
    if precondition.status != STATUS_MISSING:
        pytest.fail(f"MinIO không sẵn sàng hoặc bucket đã có sẵn: {precondition}")

    secrets = [
        v for k, v in env.items() if k.startswith("N06_") and ("SECRET" in k or "PASSWORD" in k)
    ] + [cfg.object_store.secret_key]
    try:
        # Lần 1: tạo bucket.
        code1, text1 = _init_local(bucket)
        first = json.loads(text1)
        assert code1 == 0 and first["status"] == "OK", text1
        assert first["bucket_profile"] == "benchmark"
        assert [(b["name"], b["status"]) for b in first["buckets"]] == [(bucket, "created")]
        assert head_bucket(target, bucket).status == STATUS_EXISTS
        created_at = _creation_date(target, bucket)

        # Lần 2: không lỗi, không đổi gì (bucket giữ nguyên thời điểm tạo).
        code2, text2 = _init_local(bucket)
        second = json.loads(text2)
        assert code2 == 0 and second["status"] == "OK", text2
        assert [(b["name"], b["status"]) for b in second["buckets"]] == [(bucket, "exists")]
        assert _creation_date(target, bucket) == created_at

        # Dạng text cũng chạy lại an toàn.
        code3, text3 = _init_local(bucket, as_json=False)
        assert code3 == 0 and f"bucket '{bucket}': exists" in text3

        # Doctor thấy bucket tồn tại.
        item, raw = _doctor_bucket_item(bucket, monkeypatch)
        assert item["check_id"] == f"bucket.{bucket}" and item["passed"] is True, item

        # Xóa bucket: doctor phải báo đúng tên bucket thiếu, mã 14.
        status, _ = _signed_request(target, "DELETE", f"/{bucket}")
        assert status == 204, f"Xóa bucket test thất bại: HTTP {status}"
        item, raw = _doctor_bucket_item(bucket, monkeypatch)
        assert item["passed"] is False and item["exit_code"] == 14, item
        assert bucket in item["summary"] and "chưa tồn tại" in item["summary"]
        assert "n06 init-local --profile benchmark" in item["detail"]

        # Không secret nào lọt vào output của init-local và doctor.
        for text in (text1, text2, text3, raw):
            for secret in secrets:
                assert secret not in text
    finally:
        if head_bucket(target, bucket).status == STATUS_EXISTS:
            _signed_request(target, "DELETE", f"/{bucket}")
        assert head_bucket(target, bucket).status == STATUS_MISSING, "Chưa dọn được bucket test"


def test_init_local_require_buckets_fails_when_minio_is_unreachable(tmp_path: Path) -> None:
    """Cờ `--require-buckets` đổi cảnh báo thành mã 14 khi cổng MinIO không có ai lắng nghe."""
    if not ENV_FILE.is_file():
        pytest.fail(f"Thiếu {ENV_FILE}: chạy `bash scripts/smoke.sh` trước.")
    env = _merged_env(f"n06-it-{uuid.uuid4().hex[:10]}")
    env["N06_MINIO_API_HOST_PORT"] = "1"  # cổng không có dịch vụ
    out_warn, out_strict = io.StringIO(), io.StringIO()
    warn = run_init_local(
        profile="benchmark", target_path=ENV_FILE, env=env, as_json=True, out=out_warn
    )
    strict = run_init_local(
        profile="benchmark",
        target_path=ENV_FILE,
        env=env,
        require_buckets=True,
        as_json=True,
        out=out_strict,
    )
    assert warn == 0 and json.loads(out_warn.getvalue())["buckets"][0]["status"] == "unreachable"
    assert strict == 14 and json.loads(out_strict.getvalue())["status"] == "ERROR"
