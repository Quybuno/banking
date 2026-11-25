"""Probe của `n06 doctor` không bao giờ ném: dịch vụ nhận kết nối rồi im lặng là FAIL mã 14.

Máy chủ giả viết bằng thư viện chuẩn: `silent_server` nhận kết nối và giữ mở không trả lời;
`reply_server` đọc yêu cầu rồi trả đúng những byte do test đưa. Expected viết tay theo giao thức:
PostgreSQL trả `S` hoặc `N` cho `SSLRequest`; Kafka `ApiVersions` v0 trả độ dài, correlation_id
của yêu cầu và mã lỗi.
"""

from __future__ import annotations

import io
import socket
import struct
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from unittest.mock import patch

import pytest

from n06.cli.doctor import (
    ExitCode,
    _kafka_handshake,
    _postgres_handshake,
    _probe_http,
    _probe_tcp,
    check_object_store_buckets,
    check_services,
)
from n06.cli.init_local import parse_dotenv_file, run_init_local
from n06.cli.object_store import head_bucket
from n06.config import load_config

SHORT = 0.3  # giây; đủ cho máy chủ cục bộ, đủ ngắn để test nhanh


@contextmanager
def _server(handle: Callable[[socket.socket], None] | None) -> Iterator[int]:
    """Máy chủ TCP cục bộ; `handle=None` nghĩa là nhận kết nối rồi im lặng cho tới khi test xong."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    listener.settimeout(0.05)
    stop = threading.Event()
    held: list[socket.socket] = []

    def serve() -> None:
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except OSError:
                continue
            if handle is None:
                held.append(conn)
                continue
            with conn:
                try:
                    handle(conn)
                except OSError:
                    continue  # phía probe đã đóng kết nối (hết hạn); không phải lỗi của test

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield int(listener.getsockname()[1])
    finally:
        stop.set()
        thread.join(timeout=5)
        for conn in held:
            conn.close()
        listener.close()


silent_server = partial(_server, None)


def reply_server(make_reply: Callable[[bytes], bytes]):  # type: ignore[no-untyped-def]
    def handle(conn: socket.socket) -> None:
        conn.settimeout(2)
        request = conn.recv(4096)
        conn.sendall(make_reply(request))

    return _server(handle)


def _kafka_ok(request: bytes) -> bytes:
    correlation_id = request[8:12]  # size(4) api_key(2) version(2) correlation_id(4)
    body = correlation_id + struct.pack("!h", 0) + struct.pack("!i", 0)
    return struct.pack("!i", len(body)) + body


# --- Im lặng: mọi probe trả FAIL, không ném ----------------------------------------------------


def test_http_probe_on_silent_server_fails_without_raising() -> None:
    with silent_server() as port:
        ok, message = _probe_http(f"http://127.0.0.1:{port}/health", timeout_sec=SHORT)
    assert ok is False and "TimeoutError" in message


@pytest.mark.parametrize("handshake", [_kafka_handshake, _postgres_handshake])
def test_protocol_probe_on_silent_server_fails_without_raising(
    handshake: Callable[[socket.socket], None],
) -> None:
    with silent_server() as port:
        ok, message = _probe_tcp("127.0.0.1", port, timeout_sec=SHORT, exchange=handshake)
    assert ok is False and "TimeoutError" in message


def test_plain_tcp_probe_alone_would_have_passed_the_silent_server() -> None:
    """Lý do cần bắt tay giao thức: chỉ kết nối TCP thì máy chủ im lặng vẫn "sống"."""
    with silent_server() as port:
        ok, _ = _probe_tcp("127.0.0.1", port, timeout_sec=SHORT)
    assert ok is True


def _minio_env(tmp_path: Path, port: int) -> tuple[object, dict[str, str]]:
    env_file = tmp_path / ".env"
    run_init_local(target_path=env_file, skip_buckets=True, out=io.StringIO())
    env = {**parse_dotenv_file(env_file), "N06_MINIO_API_HOST_PORT": str(port)}
    return load_config(profile="demo", env=env), env


def test_bucket_probe_on_silent_server_fails_with_exit_14_and_names_the_service(
    tmp_path: Path,
) -> None:
    with silent_server() as port:
        cfg, env = _minio_env(tmp_path, port)
        (item,) = check_object_store_buckets(
            cfg=cfg,  # type: ignore[arg-type]
            merged_env=env,
            prober=partial(head_bucket, timeout=SHORT),
        )
    assert not item.passed and item.exit_code == int(ExitCode.SERVICE_ERROR) == 14
    assert "MinIO" in item.summary and "n06-lakehouse" in item.summary
    assert "TimeoutError" in item.detail


@pytest.mark.parametrize("silent_service", ["kafka", "minio", "postgres", "spark"])
def test_check_services_reports_exit_14_and_service_name_for_each_silent_service(
    tmp_path: Path, silent_service: str
) -> None:
    """Compose báo healthy nhưng đúng một dịch vụ im lặng: chỉ dịch vụ đó FAIL mã 14."""
    states = {
        name: {"state": "running", "health": "healthy", "name": f"n06-{name}-1"}
        for name in ("kafka", "minio", "postgres", "spark")
    }
    port_variable = {
        "kafka": "N06_KAFKA_HOST_PORT",
        "minio": "N06_MINIO_API_HOST_PORT",
        "postgres": "N06_POSTGRES_HOST_PORT",
        "spark": "N06_SPARK_UI_HOST_PORT",
    }
    http_ok = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    replies: dict[str, Callable[[bytes], bytes]] = {
        "kafka": _kafka_ok,
        "minio": lambda _request: http_ok,
        "postgres": lambda _request: b"N",
        "spark": lambda _request: http_ok,
    }
    env_file = tmp_path / ".env"
    run_init_local(target_path=env_file, skip_buckets=True, out=io.StringIO())
    env = parse_dotenv_file(env_file)

    servers = []
    try:
        for name, variable in port_variable.items():
            server = silent_server() if name == silent_service else reply_server(replies[name])
            env[variable] = str(server.__enter__())
            servers.append(server)
        with patch("n06.cli.doctor._get_compose_service_states", return_value=states):
            results = check_services(
                docker_bin="docker", compose_file=tmp_path / "compose.yml", merged_env=env
            )
    finally:
        for server in servers:
            server.__exit__(None, None, None)

    by_id = {item.check_id: item for item in results}
    failed = by_id[f"service.{silent_service}"]
    assert not failed.passed and failed.exit_code == 14
    assert f"'{silent_service}'" in failed.summary and "không phản hồi" in failed.summary
    others = [item for check_id, item in by_id.items() if check_id != failed.check_id]
    assert len(others) == 3 and all(item.passed for item in others), others


# --- Phản hồi đúng và sai ----------------------------------------------------------------------


@pytest.mark.parametrize("reply", [b"S", b"N"])
def test_postgres_handshake_accepts_both_ssl_answers(reply: bytes) -> None:
    seen: list[bytes] = []

    def answer(request: bytes) -> bytes:
        seen.append(request)
        return reply

    with reply_server(answer) as port:
        ok, message = _probe_tcp("127.0.0.1", port, exchange=_postgres_handshake)
    assert ok is True and "đúng giao thức" in message
    assert seen == [bytes.fromhex("0000000804d2162f")]  # SSLRequest theo giao thức PostgreSQL


def test_kafka_handshake_accepts_a_matching_api_versions_response() -> None:
    with reply_server(_kafka_ok) as port:
        ok, _ = _probe_tcp("127.0.0.1", port, exchange=_kafka_handshake)
    assert ok is True


@pytest.mark.parametrize(
    ("handshake", "reply"),
    [
        (_postgres_handshake, b"HTTP/1.1 400 Bad Request\r\n\r\n"),
        (_postgres_handshake, b""),  # đóng kết nối ngay
        (_kafka_handshake, b"HTTP/1.1 400 Bad Request\r\n\r\n"),
        (_kafka_handshake, b""),
        (_kafka_handshake, struct.pack("!iih", 6, 1, 0)),  # sai correlation_id
        (_kafka_handshake, struct.pack("!iih", 6, 0x4E3036, 35)),  # mã lỗi khác 0
    ],
)
def test_protocol_probe_rejects_wrong_or_missing_answers(
    handshake: Callable[[socket.socket], None], reply: bytes
) -> None:
    with reply_server(lambda _request: reply) as port:
        ok, message = _probe_tcp("127.0.0.1", port, timeout_sec=SHORT, exchange=handshake)
    assert ok is False and "không phản hồi đúng" in message


@pytest.mark.parametrize("reply", [b"this is not http\r\n\r\n", b""])
def test_http_probe_on_garbage_or_dropped_connection_fails_without_raising(reply: bytes) -> None:
    with reply_server(lambda _request: reply) as port:
        ok, message = _probe_http(f"http://127.0.0.1:{port}/health", timeout_sec=SHORT)
    assert ok is False and "Không gọi được HTTP" in message


def test_probes_still_report_connection_refused() -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    assert _probe_tcp("127.0.0.1", port, exchange=_kafka_handshake)[0] is False
    assert _probe_http(f"http://127.0.0.1:{port}/", timeout_sec=SHORT)[0] is False


# --- Bổ sung sau review độc lập ----------------------------------------------------------------


def test_kafka_request_bytes_follow_the_protocol() -> None:
    """Yêu cầu viết tay: độ dài 20, api_key 18, version 0, correlation_id, client_id 10 byte."""
    seen: list[bytes] = []

    def answer(request: bytes) -> bytes:
        seen.append(request)
        return _kafka_ok(request)

    with reply_server(answer) as port:
        ok, _ = _probe_tcp("127.0.0.1", port, exchange=_kafka_handshake)
    assert ok is True
    expected = "0000001400120000004e3036000a" + b"n06-doctor".hex()
    assert seen == [bytes.fromhex(expected)]


def test_kafka_response_split_into_fragments_is_read_completely() -> None:
    def handle(conn: socket.socket) -> None:
        conn.settimeout(2)
        reply = _kafka_ok(conn.recv(4096))
        for index in range(0, len(reply), 3):
            conn.sendall(reply[index : index + 3])
            time.sleep(0.02)

    with _server(handle) as port:
        ok, message = _probe_tcp("127.0.0.1", port, exchange=_kafka_handshake)
    assert ok is True, message


@pytest.mark.parametrize(
    ("reply", "fragment"),
    [
        (struct.pack("!iih", 5, 0x4E3036, 0), "không phải của Kafka"),  # size quá nhỏ
        (struct.pack("!iih", 6, 0x4E3036, 35), "mã lỗi 35"),
    ],
)
def test_kafka_error_messages_are_specific(reply: bytes, fragment: str) -> None:
    with reply_server(lambda _request: reply) as port:
        ok, message = _probe_tcp("127.0.0.1", port, exchange=_kafka_handshake)
    assert ok is False and fragment in message


def test_service_banner_starting_with_s_is_not_postgres() -> None:
    with reply_server(lambda _request: b"SSH-2.0-OpenSSH\r\n") as port:
        ok, _ = _probe_tcp("127.0.0.1", port, exchange=_postgres_handshake)
    assert ok is False


@pytest.mark.parametrize("host", ["a..b", "x" * 300])
def test_unencodable_host_name_fails_without_raising(host: str) -> None:
    tcp_ok, tcp_message = _probe_tcp(host, 9, timeout_sec=SHORT)
    http_ok, http_message = _probe_http(f"http://{host}:9/health", timeout_sec=SHORT)
    assert (tcp_ok, http_ok) == (False, False)
    assert "không phản hồi đúng" in tcp_message and "Không gọi được HTTP" in http_message


@pytest.mark.parametrize("port", [0, -1, 65536, 84628, 2**63, 10**30])
def test_port_outside_range_fails_without_connecting(port: int) -> None:
    ok, message = _probe_tcp("127.0.0.1", port)
    assert ok is False and "ngoài miền" in message
    assert _probe_http(f"http://127.0.0.1:{port}/", timeout_sec=SHORT)[0] is False


@pytest.mark.parametrize("url", ["http://[abc", "not a url", "http://a..b/"])
def test_broken_url_fails_without_raising(url: str) -> None:
    ok, _ = _probe_http(url, timeout_sec=SHORT)
    assert ok is False


@pytest.mark.parametrize(
    "location", ["http://[abc", "http://a..b/", "http://127.0.0.1:99999999999999999999/"]
)
def test_redirect_is_a_failure_and_is_not_followed(location: str) -> None:
    reply = (
        f"HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    with reply_server(lambda _request: reply) as port:
        ok, message = _probe_http(f"http://127.0.0.1:{port}/health", timeout_sec=1.0)
    assert ok is False and "mã 302" in message


@pytest.mark.parametrize(
    "proxy",
    [
        "http://127.0.0.1:1",
        "http:/probeuser:ProxySecretMarker@proxyhost",
        "http://probeuser:ProxySecretMarker",
    ],
)
def test_http_probe_ignores_proxy_environment(monkeypatch: pytest.MonkeyPatch, proxy: str) -> None:
    for name in ("http_proxy", "HTTP_PROXY", "https_proxy", "all_proxy"):
        monkeypatch.setenv(name, proxy)
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.delenv("NO_PROXY", raising=False)
    ok_reply = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    with reply_server(lambda _request: ok_reply) as port:
        ok, message = _probe_http(f"http://127.0.0.1:{port}/health", timeout_sec=1.0)
    assert ok is True and "ProxySecretMarker" not in message
    dead = _probe_http("http://127.0.0.1:1/health", timeout_sec=SHORT)
    assert dead[0] is False and "ProxySecretMarker" not in dead[1]


def _trickle(prefix: bytes) -> Callable[[socket.socket], None]:
    """Gửi từng byte cách 0,15 s: mỗi thao tác socket đều kịp, nhưng tổng thời gian rất dài."""

    def handle(conn: socket.socket) -> None:
        conn.settimeout(2)
        conn.recv(4096)
        try:
            for byte in prefix + b"x" * 40:
                conn.sendall(bytes([byte]))
                time.sleep(0.15)
        except OSError:
            return

    return handle


@pytest.mark.parametrize("kind", ["kafka", "http"])
def test_trickling_service_is_cut_off_by_the_total_deadline(kind: str) -> None:
    prefix = b"\x00\x00\x00" if kind == "kafka" else b"HTTP/1.1 200 OK\r\nX: "
    with _server(_trickle(prefix)) as port:
        started = time.monotonic()
        if kind == "kafka":
            ok, message = _probe_tcp(
                "127.0.0.1", port, timeout_sec=0.5, exchange=_kafka_handshake, deadline_sec=0.6
            )
        else:
            ok, message = _probe_http(
                f"http://127.0.0.1:{port}/h", timeout_sec=0.5, deadline_sec=0.6
            )
        elapsed = time.monotonic() - started
    assert ok is False and "quá hạn tổng" in message
    assert elapsed < 2.0, elapsed


def test_trickling_object_store_is_cut_off_by_the_total_deadline(tmp_path: Path) -> None:
    with _server(_trickle(b"HTTP/1.1 200 OK\r\nX: ")) as port:
        cfg, env = _minio_env(tmp_path, port)
        started = time.monotonic()
        (item,) = check_object_store_buckets(
            cfg=cfg,  # type: ignore[arg-type]
            merged_env=env,
            prober=partial(head_bucket, timeout=0.5),
            deadline_sec=0.6,
        )
        elapsed = time.monotonic() - started
    assert not item.passed and item.exit_code == 14 and "hạn tổng" in item.detail
    assert elapsed < 2.0, elapsed


def test_server_supplied_control_characters_are_not_echoed() -> None:
    with reply_server(lambda _request: b"\x1b[31mRED\r\n\r\n") as port:
        ok, message = _probe_http(f"http://127.0.0.1:{port}/", timeout_sec=1.0)
    assert ok is False and "\x1b" not in message


@pytest.mark.parametrize(
    "variable",
    ["N06_KAFKA_HOST_PORT", "N06_MINIO_API_HOST_PORT", "N06_POSTGRES_HOST_PORT"],
)
def test_non_numeric_port_variable_is_a_config_failure_for_each_service(
    tmp_path: Path, variable: str
) -> None:
    env_file = tmp_path / ".env"
    run_init_local(target_path=env_file, skip_buckets=True, out=io.StringIO())
    env = {**parse_dotenv_file(env_file), variable: "abc"}
    with patch("n06.cli.doctor._get_compose_service_states", return_value={}):
        results = check_services(
            docker_bin="docker", compose_file=tmp_path / "compose.yml", merged_env=env
        )
    assert [item.check_id for item in results] == [
        "service.kafka",
        "service.minio",
        "service.postgres",
        "service.spark",
    ]
    assert all(not item.passed and item.exit_code == 10 for item in results)


def test_unexpected_programming_errors_still_surface() -> None:
    """Hạn tổng chạy trong luồng nền nhưng không được nuốt lỗi lập trình."""

    def broken(_sock: socket.socket) -> None:
        raise KeyError("lỗi lập trình")

    with silent_server() as port, pytest.raises(KeyError):
        _probe_tcp("127.0.0.1", port, exchange=broken)
