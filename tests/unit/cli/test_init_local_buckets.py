"""Test `n06 init-local` tạo bucket và `n06 doctor` kiểm bucket với máy chủ S3 giả (TASK T03b).

Máy chủ HTTP giả (thư viện chuẩn) chỉ dùng cho các ca lỗi khó gây trên MinIO thật: 403, 409,
không kết nối được. Bằng chứng chính nằm ở `tests/integration/test_init_local_buckets.py`.
"""

from __future__ import annotations

import io
import json
import socket
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import pytest

from n06.cli.doctor import (
    CheckItem,
    SystemResourceSnapshot,
    build_doctor_report,
    check_object_store_buckets,
)
from n06.cli.init_local import parse_dotenv_file, run_init_local
from n06.cli.object_store import (
    STATUS_CREATED,
    STATUS_ERROR,
    STATUS_EXISTS,
    STATUS_MISSING,
    STATUS_UNREACHABLE,
    BucketNameError,
    BucketResult,
    ObjectStoreTarget,
    create_bucket,
    ensure_bucket,
    head_bucket,
)
from n06.config import load_config

Responder = Callable[[str, str, dict[str, str]], tuple[int, bytes]]


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, responder: Responder) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.responder = responder
        self.requests: list[tuple[str, str, dict[str, str]]] = []


class _Handler(BaseHTTPRequestHandler):
    def _handle(self) -> None:
        server: _Server = self.server  # type: ignore[assignment]
        headers = {k.lower(): v for k, v in self.headers.items()}
        server.requests.append((self.command, self.path, headers))
        status, body = server.responder(self.command, self.path, headers)
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)) if self.command != "HEAD" else "0")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    do_HEAD = do_PUT = do_GET = _handle  # noqa: N815

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


@contextmanager
def fake_s3(responder: Responder) -> Iterator[_Server]:
    server = _Server(responder)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _stateful_responder() -> Responder:
    """Hành vi giống S3: HEAD 404 cho tới khi PUT tạo bucket."""
    buckets: set[str] = set()

    def respond(method: str, path: str, headers: dict[str, str]) -> tuple[int, bytes]:
        bucket = path.strip("/")
        if method == "PUT":
            buckets.add(bucket)
            return 200, b""
        return (200, b"") if bucket in buckets else (404, b"")

    return respond


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _target(port: int, region: str = "eu-test-1") -> ObjectStoreTarget:
    return ObjectStoreTarget(
        "http", "127.0.0.1", port, region, "AKIATESTKEYEXAMPLE", "s3cr3t-value"
    )


# --- object_store: ensure_bucket ---------------------------------------------------------------


def test_ensure_bucket_creates_once_and_second_call_does_not_put() -> None:
    with fake_s3(_stateful_responder()) as server:
        target = _target(server.server_address[1])
        first = ensure_bucket(target, "n06-lakehouse")
        second = ensure_bucket(target, "n06-lakehouse")
    assert (first.status, second.status) == (STATUS_CREATED, STATUS_EXISTS)
    assert [m for m, _, _ in server.requests] == ["HEAD", "PUT", "HEAD"]


def test_requests_are_signed_with_region_and_credential_from_target() -> None:
    with fake_s3(_stateful_responder()) as server:
        ensure_bucket(_target(server.server_address[1], region="eu-test-1"), "n06-lakehouse")
    for _method, path, headers in server.requests:
        assert path == "/n06-lakehouse"
        auth = headers["authorization"]
        assert auth.startswith("AWS4-HMAC-SHA256 Credential=AKIATESTKEYEXAMPLE/")
        assert "/eu-test-1/s3/aws4_request," in auth
        assert "SignedHeaders=host;x-amz-content-sha256;x-amz-date," in auth
        assert headers["x-amz-date"].endswith("Z")
        assert "s3cr3t-value" not in auth


def test_forbidden_is_error_and_detail_never_leaks_credentials() -> None:
    body = (
        b"<Error><Code>SignatureDoesNotMatch</Code><AWSAccessKeyId>AKIATESTKEYEXAMPLE"
        b"</AWSAccessKeyId><StringToSign>SECRET-STRING-TO-SIGN</StringToSign></Error>"
    )
    with fake_s3(lambda m, p, h: (403, body)) as server:
        target = _target(server.server_address[1])
        head = head_bucket(target, "n06-lakehouse")
        put = create_bucket(target, "n06-lakehouse")
    for result in (head, put):
        assert result.status == STATUS_ERROR
        assert "403" in result.detail
        for forbidden in ("AKIATESTKEYEXAMPLE", "s3cr3t-value", "SECRET-STRING-TO-SIGN"):
            assert forbidden not in result.detail
    assert "SignatureDoesNotMatch" in put.detail


@pytest.mark.parametrize(
    ("status", "code", "expected"),
    [
        (409, "BucketAlreadyOwnedByYou", STATUS_EXISTS),
        (409, "BucketAlreadyExists", STATUS_ERROR),
        (500, "InternalError", STATUS_ERROR),
    ],
)
def test_create_bucket_maps_s3_errors(status: int, code: str, expected: str) -> None:
    body = f"<Error><Code>{code}</Code></Error>".encode()
    with fake_s3(lambda m, p, h: (status, body)) as server:
        result = create_bucket(_target(server.server_address[1]), "n06-lakehouse")
    assert result.status == expected


def test_unreachable_server_is_reported_as_unreachable_not_exception() -> None:
    result = ensure_bucket(_target(_closed_port()), "n06-lakehouse")
    assert result.status == STATUS_UNREACHABLE
    assert "MinIO chưa chạy" in result.detail
    assert "s3cr3t-value" not in result.detail


def test_invalid_bucket_name_is_rejected_before_any_request() -> None:
    with fake_s3(_stateful_responder()) as server:
        with pytest.raises(BucketNameError):
            ensure_bucket(_target(server.server_address[1]), "Bad_Name")
    assert server.requests == []


# --- init-local --------------------------------------------------------------------------------


def _init(
    tmp_path: Path, port: int, *, profile: str = "benchmark", **kwargs: object
) -> tuple[int, str, Path]:
    out = io.StringIO()
    env_file = tmp_path / ".env"
    extra_env = {"N06_MINIO_API_HOST_PORT": str(port), **kwargs.pop("env", {})}  # type: ignore[call-overload]
    code = run_init_local(
        profile=profile,
        target_path=env_file,
        env=extra_env,
        out=out,
        **kwargs,  # type: ignore[arg-type]
    )
    return code, out.getvalue(), env_file


def test_init_local_creates_profile_bucket_then_second_run_changes_nothing(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        port = server.server_address[1]
        code1, text1, env_file = _init(tmp_path, port, as_json=True)
        code2, text2, _ = _init(tmp_path, port, as_json=True)
    first, second = json.loads(text1), json.loads(text2)
    assert (code1, code2) == (0, 0)
    # Expected viết tay theo ADR-0002 mục 5: profile benchmark dùng bucket riêng.
    assert first["buckets"] == [
        {"name": "n06-benchmark-lakehouse", "status": "created", "detail": "đã tạo bucket"}
    ]
    assert [b["status"] for b in second["buckets"]] == ["exists"]
    assert first["status"] == second["status"] == "OK" and second["buckets_ready"] is True
    assert second["created"] is False  # .env không bị tạo lại
    assert [m for m, _, _ in server.requests] == ["HEAD", "PUT", "HEAD"]
    secrets = [v for k, v in parse_dotenv_file(env_file).items() if "SECRET" in k or "KEY" in k]
    assert secrets and all(s not in text1 + text2 for s in secrets)


def test_init_local_default_profile_creates_default_bucket(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        code, text, _ = _init(tmp_path, server.server_address[1], profile="demo", as_json=True)
    assert code == 0
    assert json.loads(text)["buckets"][0]["name"] == "n06-lakehouse"


def test_init_local_uses_bucket_from_configuration_override(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        code, text, _ = _init(
            tmp_path,
            server.server_address[1],
            env={"N06__OBJECT_STORE__BUCKET": "n06-custom-bucket"},
            as_json=True,
        )
    assert code == 0
    assert json.loads(text)["buckets"][0]["name"] == "n06-custom-bucket"
    assert {p for _, p, _ in server.requests} == {"/n06-custom-bucket"}


def test_init_local_minio_not_running_exits_zero_with_clear_warning(tmp_path: Path) -> None:
    port = _closed_port()
    code_text, text, _ = _init(tmp_path, port)
    code_json, raw, _ = _init(tmp_path, port, as_json=True)
    assert code_text == code_json == 0
    assert "unreachable" in text and "CẢNH BÁO" in text
    assert "docker compose --profile core up -d" in text
    payload = json.loads(raw)
    assert payload["status"] == "WARN" and payload["buckets_ready"] is False
    assert payload["buckets"][0]["name"] == "n06-benchmark-lakehouse"
    assert payload["buckets"][0]["status"] == "unreachable"


def test_init_local_require_buckets_fails_when_not_ready(tmp_path: Path) -> None:
    code, text, _ = _init(tmp_path, _closed_port(), require_buckets=True, as_json=True)
    assert code == 14
    assert json.loads(text)["status"] == "ERROR"


def test_init_local_require_buckets_passes_when_ready(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        code, _, _ = _init(tmp_path, server.server_address[1], require_buckets=True)
    assert code == 0


def test_init_local_forbidden_exits_14_even_without_require_flag(tmp_path: Path) -> None:
    with fake_s3(lambda m, p, h: (403, b"<Error><Code>AccessDenied</Code></Error>")) as server:
        code, text, env_file = _init(tmp_path, server.server_address[1])
    assert code == 14
    # HEAD không có body nên chỉ có mã HTTP; thông báo nêu rõ lý do và gợi ý sai khóa/quyền.
    assert "HTTP 403" in text and "sai access key/secret key" in text
    assert all(
        v not in text for k, v in parse_dotenv_file(env_file).items() if "SECRET" in k or "KEY" in k
    )


def test_init_local_invalid_bucket_name_exits_10_without_request(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        code, text, _ = _init(
            tmp_path,
            server.server_address[1],
            env={"N06__OBJECT_STORE__BUCKET": "Bad_Name"},
        )
    assert code == 10
    assert "Bad_Name" in text and "không hợp lệ" in text
    assert server.requests == []


def test_init_local_skip_buckets_makes_no_request(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        code, text, _ = _init(tmp_path, server.server_address[1], skip_buckets=True, as_json=True)
    assert code == 0
    payload = json.loads(text)
    assert payload["buckets_skipped"] is True and payload["buckets"] == []
    assert server.requests == []


# --- doctor ------------------------------------------------------------------------------------


def _doctor_cfg(tmp_path: Path, port: int, profile: str = "benchmark") -> tuple[object, dict]:
    env_file = tmp_path / ".env"
    run_init_local(target_path=env_file, skip_buckets=True, out=io.StringIO())
    env = {**parse_dotenv_file(env_file), "N06_MINIO_API_HOST_PORT": str(port)}
    return load_config(profile=profile, env=env), env


def test_doctor_bucket_check_passes_when_bucket_exists(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        cfg, env = _doctor_cfg(tmp_path, server.server_address[1])
        ensure_bucket(_target(server.server_address[1]), cfg.object_store.bucket)  # type: ignore[attr-defined]
        (item,) = check_object_store_buckets(cfg=cfg, merged_env=env)  # type: ignore[arg-type]
    assert item.passed and item.check_id == "bucket.n06-benchmark-lakehouse"


def test_doctor_names_the_missing_bucket(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        cfg, env = _doctor_cfg(tmp_path, server.server_address[1])
        (item,) = check_object_store_buckets(cfg=cfg, merged_env=env)  # type: ignore[arg-type]
    assert not item.passed and item.exit_code == 14
    assert item.check_id == "bucket.n06-benchmark-lakehouse"
    assert "n06-benchmark-lakehouse" in item.summary
    assert "chưa tồn tại" in item.summary
    assert "n06 init-local --profile benchmark" in item.detail


def test_doctor_forbidden_bucket_check_does_not_leak_secrets(tmp_path: Path) -> None:
    with fake_s3(lambda m, p, h: (403, b"<Error><Code>AccessDenied</Code></Error>")) as server:
        cfg, env = _doctor_cfg(tmp_path, server.server_address[1])
        (item,) = check_object_store_buckets(cfg=cfg, merged_env=env)  # type: ignore[arg-type]
    assert not item.passed and item.exit_code == 14
    secret_values = [v for k, v in env.items() if "SECRET" in k or "KEY" in k or "PASSWORD" in k]
    assert all(v not in item.summary + item.detail for v in secret_values)


def _passing(check_id: str, category: str = "service") -> CheckItem:
    return CheckItem(check_id, category, True, 0, "ok", "ok")


def _snapshot() -> SystemResourceSnapshot:
    return SystemResourceSnapshot(
        python_major=3,
        python_minor=11,
        python_micro=11,
        cpu_cores=4,
        total_ram_gib=8.0,
        free_disk_gib=25.0,
    )


def _build_report(env: dict[str, str], services: list[CheckItem], prober: object, **kw: object):
    with (
        patch(
            "n06.cli.doctor.check_docker_engine",
            return_value=(_passing("docker.engine", "docker"), "/usr/bin/docker"),
        ),
        patch("n06.cli.doctor.check_services", return_value=services),
    ):
        return build_doctor_report(
            profile="benchmark",
            env_file=Path("/nonexistent/.env"),
            env=env,
            resource_snapshot=_snapshot(),
            bucket_prober=prober,  # type: ignore[arg-type]
            **kw,  # type: ignore[arg-type]
        )


def test_doctor_report_includes_bucket_check_after_minio_is_healthy(tmp_path: Path) -> None:
    _, env = _doctor_cfg(tmp_path, 1)
    calls: list[str] = []

    def prober(target: ObjectStoreTarget, bucket: str) -> BucketResult:
        calls.append(bucket)
        return BucketResult(bucket, STATUS_MISSING, "bucket chưa tồn tại")

    services = [_passing(f"service.{s}") for s in ("kafka", "minio", "postgres", "spark")]
    report = _build_report(env, services, prober)
    by_id = {c.check_id: c for c in report.checks}
    assert calls == ["n06-benchmark-lakehouse"]
    assert by_id["bucket.n06-benchmark-lakehouse"].passed is False
    assert report.exit_code == 14


def test_doctor_report_skips_bucket_check_when_minio_service_failed(tmp_path: Path) -> None:
    _, env = _doctor_cfg(tmp_path, 1)
    failed = CheckItem("service.minio", "service", False, 14, "minio down", "x")
    prober_calls: list[str] = []

    def prober(target: ObjectStoreTarget, bucket: str) -> BucketResult:
        prober_calls.append(bucket)
        return BucketResult(bucket, STATUS_EXISTS, "")

    report = _build_report(env, [_passing("service.kafka"), failed], prober)
    assert prober_calls == []
    assert not any(c.check_id.startswith("bucket.") for c in report.checks)


def test_doctor_report_skips_bucket_check_with_skip_services(tmp_path: Path) -> None:
    _, env = _doctor_cfg(tmp_path, 1)
    called: list[str] = []

    def prober(target: ObjectStoreTarget, bucket: str) -> BucketResult:
        called.append(bucket)
        return BucketResult(bucket, STATUS_EXISTS, "")

    report = build_doctor_report(
        profile="benchmark",
        env_file=Path("/nonexistent/.env"),
        env=env,
        resource_snapshot=_snapshot(),
        skip_services=True,
        bucket_prober=prober,
    )
    assert called == [] and not any(c.check_id.startswith("bucket.") for c in report.checks)
