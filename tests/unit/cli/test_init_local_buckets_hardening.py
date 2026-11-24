"""Test bổ sung T03b sau review độc lập: cấu hình sai, phân loại lỗi truyền tải, thứ tự profile.

Mọi expected viết tay theo yêu cầu của task (không gọi lại chính hàm đang test):
- cấu hình hoặc tên bucket sai: mã 10, không traceback, không in access key hay secret key;
- không có ai lắng nghe: `unreachable` (mã 0 kèm cảnh báo); máy chủ có kết nối nhưng trả lời hỏng
  hoặc quá thời gian: `error` (mã 14);
- thứ tự chọn profile: tham số tường minh, rồi biến môi trường `N06_PROFILE`, rồi `.env`.
"""

from __future__ import annotations

import io
import json
import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from n06.cli import main
from n06.cli.doctor import check_object_store_buckets
from n06.cli.init_local import parse_dotenv_file, run_init_local
from n06.cli.object_store import (
    STATUS_ERROR,
    STATUS_UNREACHABLE,
    BucketNameError,
    BucketResult,
    ObjectStoreTarget,
    ensure_bucket,
    validate_bucket_name,
)
from n06.config import load_config

from .test_init_local_buckets import (
    _closed_port,
    _init,
    _stateful_responder,
    _target,
    fake_s3,
)

ACCESS_KEY_MARKER = "AKIAHARDENINGMARKER"


@contextmanager
def junk_server() -> Iterator[int]:
    """Máy chủ TCP nhận kết nối rồi trả dữ liệu không phải HTTP."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    stop = threading.Event()

    def serve() -> None:
        listener.settimeout(0.2)
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except OSError:
                continue
            with conn:
                conn.recv(4096)
                conn.sendall(b"this is not http\r\n\r\n")

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield int(listener.getsockname()[1])
    finally:
        stop.set()
        thread.join(timeout=5)
        listener.close()


# --- Cấu hình sai: mã 10, không traceback, không lộ credential ---------------------------------


@pytest.mark.parametrize("bad_port", ["abc", "", "0", "70000", "-1", "1e3", "８０８０"])
def test_invalid_host_port_exits_10_without_traceback(tmp_path: Path, bad_port: str) -> None:
    code, text, _ = _init(tmp_path, 1, env={"N06_MINIO_API_HOST_PORT": bad_port})
    assert code == 10
    assert "N06_MINIO_API_HOST_PORT" in text and "1 đến 65535" in text


@pytest.mark.parametrize(
    ("field_env", "value"),
    [
        ("N06__OBJECT_STORE__REGION", "us-east-1\r\nX: y"),
        ("N06__OBJECT_STORE__REGION", "US_EAST"),
        ("N06_OBJECT_STORE_ACCESS_KEY", f"{ACCESS_KEY_MARKER}\nInjected: 1"),
        ("N06_OBJECT_STORE_ACCESS_KEY", f"{ACCESS_KEY_MARKER}-é"),
        ("N06_OBJECT_STORE_SECRET_KEY", "secret with space"),
    ],
)
def test_unsafe_region_or_credential_exits_10_and_never_echoes_value(
    tmp_path: Path, field_env: str, value: str
) -> None:
    with fake_s3(_stateful_responder()) as server:
        code, text, _ = _init(tmp_path, server.server_address[1], env={field_env: value})
    assert code == 10
    assert server.requests == []  # bị chặn trước khi gửi bất kỳ yêu cầu nào
    assert ACCESS_KEY_MARKER not in text and "Injected" not in text
    assert "secret with space" not in text


def test_path_style_disabled_exits_10(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        code, text, _ = _init(
            tmp_path,
            server.server_address[1],
            env={"N06__OBJECT_STORE__PATH_STYLE_ACCESS": "false"},
        )
    assert code == 10 and "path_style_access" in text
    assert server.requests == []


def test_configuration_validation_error_exits_10_naming_only_the_field(tmp_path: Path) -> None:
    code, text, _ = _init(tmp_path, 1, env={"N06__OBJECT_STORE__BUCKET": ""})
    assert code == 10
    assert "object_store.bucket" in text


@pytest.mark.parametrize("name", ["xn--abc", "sthree-bucket", "mybucket-s3alias", "my--ol-s3"])
def test_reserved_bucket_names_are_rejected(name: str) -> None:
    with pytest.raises(BucketNameError):
        validate_bucket_name(name)


# --- Phân loại lỗi truyền tải ------------------------------------------------------------------


def test_connection_refused_is_unreachable() -> None:
    assert ensure_bucket(_target(_closed_port()), "n06-lakehouse").status == STATUS_UNREACHABLE


def test_timeout_is_error_not_unreachable() -> None:
    def slow(method: str, path: str, headers: dict[str, str]) -> tuple[int, bytes]:
        time.sleep(1.0)
        return 200, b""

    with fake_s3(slow) as server:
        result = ensure_bucket(_target(server.server_address[1]), "n06-lakehouse", timeout=0.2)
    assert result.status == STATUS_ERROR
    assert "MinIO chưa chạy" not in result.detail
    assert "s3cr3t-value" not in result.detail


def test_non_http_response_is_error_and_init_local_exits_14(tmp_path: Path) -> None:
    with junk_server() as port:
        assert ensure_bucket(_target(port), "n06-lakehouse").status == STATUS_ERROR
        code, text, _ = _init(tmp_path, port, as_json=True)
    assert code == 14
    payload = json.loads(text)
    assert payload["status"] == "ERROR"
    assert payload["buckets"][0]["status"] == "error"


def test_https_against_plain_http_server_is_error() -> None:
    with fake_s3(_stateful_responder()) as server:
        target = ObjectStoreTarget(
            "https", "127.0.0.1", server.server_address[1], "eu-test-1", "AKIATESTKEY", "s3cr3t"
        )
        result = ensure_bucket(target, "n06-lakehouse", timeout=2.0)
    assert result.status == STATUS_ERROR
    assert "MinIO chưa chạy" not in result.detail


# --- Thứ tự chọn profile: tham số > N06_PROFILE trong môi trường > .env ------------------------


def _bucket_profile(raw: str) -> str:
    return str(json.loads(raw)["bucket_profile"])


def test_explicit_profile_beats_value_written_in_env_file(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        port = server.server_address[1]
        _init(tmp_path, port, profile="demo", skip_buckets=True)
        assert parse_dotenv_file(tmp_path / ".env")["N06_PROFILE"] == "demo"
        code, raw, _ = _init(tmp_path, port, profile="benchmark", as_json=True)
    assert code == 0
    assert _bucket_profile(raw) == "benchmark"
    assert json.loads(raw)["buckets"][0]["name"] == "n06-benchmark-lakehouse"


def test_without_explicit_profile_environment_beats_env_file(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        port = server.server_address[1]
        _init(tmp_path, port, profile="demo", skip_buckets=True)  # .env ghi demo
        code, raw, _ = _init(
            tmp_path, port, profile=None, env={"N06_PROFILE": "benchmark"}, as_json=True
        )
    assert code == 0
    assert _bucket_profile(raw) == "benchmark"


def test_without_explicit_profile_or_environment_follow_env_file(tmp_path: Path) -> None:
    with fake_s3(_stateful_responder()) as server:
        port = server.server_address[1]
        _init(tmp_path, port, profile="demo", skip_buckets=True)
        code, raw, _ = _init(tmp_path, port, profile=None, as_json=True)
    assert code == 0
    assert _bucket_profile(raw) == "demo"
    assert json.loads(raw)["buckets"][0]["name"] == "n06-lakehouse"


# --- Cờ CLI ------------------------------------------------------------------------------------


def test_skip_and_require_buckets_are_mutually_exclusive_via_main() -> None:
    out, err = io.StringIO(), io.StringIO()
    code = main(["init-local", "--skip-buckets", "--require-buckets"], out=out, err=err)
    assert code == 2
    assert "not allowed with" in err.getvalue()


# --- doctor: các nhánh còn lại -----------------------------------------------------------------


def _cfg_and_env(tmp_path: Path, **env_extra: str) -> tuple[object, dict[str, str]]:
    env_file = tmp_path / ".env"
    run_init_local(target_path=env_file, skip_buckets=True, out=io.StringIO())
    env = {**parse_dotenv_file(env_file), **env_extra}
    return load_config(profile="benchmark", env=env), env


def test_doctor_bucket_check_reports_config_error_with_exit_10(tmp_path: Path) -> None:
    cfg, env = _cfg_and_env(tmp_path, N06_MINIO_API_HOST_PORT="abc")
    (item,) = check_object_store_buckets(cfg=cfg, merged_env=env)  # type: ignore[arg-type]
    assert not item.passed and item.exit_code == 10
    assert "N06_MINIO_API_HOST_PORT" in item.detail


@pytest.mark.parametrize(
    ("status", "expected_exit"), [(STATUS_UNREACHABLE, 14), (STATUS_ERROR, 14)]
)
def test_doctor_bucket_check_fails_with_14_when_store_is_unreachable_or_errors(
    tmp_path: Path, status: str, expected_exit: int
) -> None:
    cfg, env = _cfg_and_env(tmp_path, N06_MINIO_API_HOST_PORT="9")

    def fake_prober(target: ObjectStoreTarget, bucket: str) -> BucketResult:
        return BucketResult(bucket, status, "chi tiết giả")

    (item,) = check_object_store_buckets(
        cfg=cfg,  # type: ignore[arg-type]
        merged_env=env,
        prober=fake_prober,
    )
    assert not item.passed and item.exit_code == expected_exit
    assert item.check_id == "bucket.n06-benchmark-lakehouse"
    assert "Không kiểm được bucket" in item.summary
    assert item.detail == "chi tiết giả"
