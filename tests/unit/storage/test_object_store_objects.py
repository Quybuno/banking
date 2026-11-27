"""Unit test cho `PUT`/`HEAD` object và bất biến trên máy chủ S3 giả đọc thân (ADR-0012)."""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from n06.storage.object_store import (
    ObjectIntegrityError,
    ObjectStoreError,
    ObjectStoreTarget,
    head_object,
    put_object_bytes,
    put_object_file,
    put_object_immutable_bytes,
    put_object_immutable_file,
)


class _ObjectServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _ObjectHandler)
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}
        self.requests: list[tuple[str, str, dict[str, str], bytes]] = []
        self.fail_next_statuses: list[int] = []


class _ObjectHandler(BaseHTTPRequestHandler):
    def _handle(self) -> None:
        server: _ObjectServer = self.server  # type: ignore[assignment]
        headers = {k.lower(): v for k, v in self.headers.items()}
        length = int(headers.get("content-length", "0"))
        body = self.rfile.read(length) if length > 0 else b""
        server.requests.append((self.command, self.path, headers, body))

        if server.fail_next_statuses:
            status = server.fail_next_statuses.pop(0)
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if self.command == "PUT":
            meta = {k: v for k, v in headers.items() if k.startswith("x-amz-meta-")}
            server.objects[self.path] = (body, meta)
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if self.command == "HEAD":
            stored = server.objects.get(self.path)
            if stored is None:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            data, meta = stored
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            for k, v in meta.items():
                self.send_header(k, v)
            self.end_headers()
            return

        self.send_response(405)
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_PUT = do_HEAD = do_GET = _handle  # noqa: N815

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


@contextmanager
def fake_object_s3() -> Iterator[_ObjectServer]:
    server = _ObjectServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _target(port: int) -> ObjectStoreTarget:
    return ObjectStoreTarget(
        "http", "127.0.0.1", port, "us-east-1", "AKIATESTKEYEXAMPLE", "s3cr3t-do-not-leak"
    )


def test_put_and_head_object_bytes_and_file_with_content_length_and_sha256(tmp_path: Path) -> None:
    payload = b"Welcome to Amazon S3.\n"
    expected_sha = hashlib.sha256(payload).hexdigest()
    file_path = tmp_path / "sample.jsonl"
    file_path.write_bytes(payload)

    with fake_object_s3() as server:
        target = _target(server.server_address[1])
        assert head_object(target, "n06-lakehouse", "statements/ds/a.jsonl").exists is False

        sha1 = put_object_bytes(target, "n06-lakehouse", "statements/ds/a.jsonl", payload)
        sha2 = put_object_file(target, "n06-lakehouse", "statements/ds/b.jsonl", file_path)
        head_a = head_object(target, "n06-lakehouse", "statements/ds/a.jsonl")
        head_b = head_object(target, "n06-lakehouse", "statements/ds/b.jsonl")

    assert sha1 == sha2 == expected_sha
    assert head_a.exists and head_a.content_length == len(payload) and head_a.sha256 == expected_sha
    assert head_b.exists and head_b.content_length == len(payload) and head_b.sha256 == expected_sha

    put_reqs = [req for req in server.requests if req[0] == "PUT"]
    assert len(put_reqs) == 2
    for _, _, headers, body in put_reqs:
        assert body == payload
        assert headers["content-length"] == str(len(payload))
        assert headers["x-amz-content-sha256"] == expected_sha
        assert headers["x-amz-meta-sha256"] == expected_sha


def test_put_immutable_skips_same_sha_and_rejects_mismatch_or_missing_meta(tmp_path: Path) -> None:
    file_a = tmp_path / "a.jsonl"
    file_a.write_bytes(b'{"id":1}\n')
    file_b = tmp_path / "b.jsonl"
    file_b.write_bytes(b'{"id":2}\n')

    with fake_object_s3() as server:
        target = _target(server.server_address[1])
        assert put_object_immutable_file(target, "n06-lakehouse", "k/a.jsonl", file_a) == "uploaded"
        assert put_object_immutable_file(target, "n06-lakehouse", "k/a.jsonl", file_a) == "skipped"

        with pytest.raises(ObjectIntegrityError):
            put_object_immutable_file(target, "n06-lakehouse", "k/a.jsonl", file_b)

        # Object đã tồn tại nhưng thiếu x-amz-meta-sha256 -> từ chối ghi đè (ADR-0012).
        server.objects["/n06-lakehouse/k/no-meta.json"] = (b"{}\n", {})
        with pytest.raises(ObjectIntegrityError):
            put_object_immutable_bytes(target, "n06-lakehouse", "k/no-meta.json", b"{}\n")


def test_transient_503_retries_and_403_does_not_leak_secret() -> None:
    sleeps: list[float] = []
    with fake_object_s3() as server:
        target = _target(server.server_address[1])
        server.fail_next_statuses = [503]
        sha = put_object_bytes(
            target, "n06-lakehouse", "k/retry.json", b"ok\n", sleep=sleeps.append
        )
        assert sha == hashlib.sha256(b"ok\n").hexdigest()
        assert len(sleeps) == 1

        server.fail_next_statuses = [403]
        with pytest.raises(ObjectStoreError) as exc_info:
            head_object(target, "n06-lakehouse", "k/retry.json", sleep=sleeps.append)
        assert "403" in str(exc_info.value)
        assert "s3cr3t-do-not-leak" not in str(exc_info.value)
