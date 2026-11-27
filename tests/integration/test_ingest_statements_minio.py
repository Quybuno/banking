"""Integration test T23 trên MinIO thật: `PUT`/`HEAD` object và `n06 ingest-statements`."""

from __future__ import annotations

import hashlib
import http.client
import io
import json
import os
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path

import pytest

from n06.cli.init_local import parse_dotenv_file
from n06.cli.statements import run_ingest_statements
from n06.config import REPO_ROOT, load_config
from n06.domain import parse_utc_timestamp
from n06.generator import build_dataset
from n06.ingestion.statements import build_statement_keys, discover_statement_deliveries
from n06.storage.object_store import (
    EMPTY_PAYLOAD_SHA256,
    ObjectIntegrityError,
    ObjectStoreTarget,
    authorization_header,
    ensure_bucket,
    head_object,
    put_object_immutable_file,
    target_from_config,
)

ENV_FILE = REPO_ROOT / ".env"
DEMO_FAULTS_CFG = (
    REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1" / "config" / "generator.demo-faults.json"
)
S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def _env_with_bucket(bucket: str) -> dict[str, str]:
    env: dict[str, str] = {}
    if ENV_FILE.is_file():
        env.update(parse_dotenv_file(ENV_FILE))
    env.update(os.environ)
    env["N06__OBJECT_STORE__BUCKET"] = bucket
    return env


def _raw_s3(
    target: ObjectStoreTarget,
    method: str,
    path: str,
    query: list[tuple[str, str]] | None = None,
) -> tuple[int, bytes]:
    query = query or []
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
        query=query,
        headers=headers,
        payload_sha256=EMPTY_PAYLOAD_SHA256,
    )
    qs = "&".join(f"{k}={v}" for k, v in sorted(query))
    full_path = f"{path}?{qs}" if qs else path
    conn = http.client.HTTPConnection(target.host, target.port, timeout=15)
    try:
        conn.request(method, full_path, headers=headers)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def _list_keys(target: ObjectStoreTarget, bucket: str) -> set[str]:
    status, body = _raw_s3(target, "GET", f"/{bucket}", [("list-type", "2")])
    assert status == 200, f"ListObjectsV2 thất bại: HTTP {status}"
    return {node.text for node in ET.fromstring(body).iter(f"{S3_NS}Key") if node.text}


def _cleanup_bucket(target: ObjectStoreTarget, bucket: str) -> None:
    status, body = _raw_s3(target, "GET", f"/{bucket}", [("list-type", "2")])
    if status == 200:
        for node in ET.fromstring(body).iter(f"{S3_NS}Key"):
            if node.text:
                _raw_s3(target, "DELETE", f"/{bucket}/{node.text}")
    _raw_s3(target, "DELETE", f"/{bucket}")


def test_minio_object_put_head_immutability_and_large_file(tmp_path: Path) -> None:
    bucket = f"n06-it-t23-{uuid.uuid4().hex[:10]}"
    env = _env_with_bucket(bucket)
    cfg = load_config(profile="test", env=env)
    target = target_from_config(cfg, env)
    assert ensure_bucket(target, bucket).ready

    try:
        small = tmp_path / "small.jsonl"
        small.write_bytes(b'{"a":1}\n{"a":2}\n')
        small_sha = hashlib.sha256(small.read_bytes()).hexdigest()

        assert put_object_immutable_file(target, bucket, "obj/small.jsonl", small) == "uploaded"
        head = head_object(target, bucket, "obj/small.jsonl")
        assert (
            head.exists and head.sha256 == small_sha and head.content_length == small.stat().st_size
        )
        get_status, get_body = _raw_s3(target, "GET", f"/{bucket}/obj/small.jsonl")
        assert get_status == 200 and hashlib.sha256(get_body).hexdigest() == small_sha

        # Tải lại cùng nội dung -> skipped.
        assert put_object_immutable_file(target, bucket, "obj/small.jsonl", small) == "skipped"

        # Khác nội dung cùng khóa -> từ chối.
        other = tmp_path / "other.jsonl"
        other.write_bytes(b'{"a":999}\n')
        with pytest.raises(ObjectIntegrityError):
            put_object_immutable_file(target, bucket, "obj/small.jsonl", other)

        # File lớn (~5 MiB, vượt quy mô file lớn nhất 4.3 MB ngoại suy ở 1M tx trong ADR-0012).
        large = tmp_path / "large.jsonl"
        chunk = (b'{"record":"' + b"x" * 900 + b'"}\n') * 5500
        large.write_bytes(chunk)
        large_sha = hashlib.sha256(chunk).hexdigest()
        assert put_object_immutable_file(target, bucket, "obj/large.jsonl", large) == "uploaded"
        large_head = head_object(target, bucket, "obj/large.jsonl")
        assert large_head.exists and large_head.sha256 == large_sha
        assert large_head.content_length == len(chunk)
        large_status, large_body = _raw_s3(target, "GET", f"/{bucket}/obj/large.jsonl")
        assert large_status == 200 and hashlib.sha256(large_body).hexdigest() == large_sha
    finally:
        _cleanup_bucket(target, bucket)


def test_ingest_statements_end_to_end_on_minio_respects_simulated_clock(tmp_path: Path) -> None:
    bucket = f"n06-it-stmt-{uuid.uuid4().hex[:10]}"
    env = _env_with_bucket(bucket)
    cfg = load_config(profile="test", env=env)
    target = target_from_config(cfg, env)
    gen_cfg = json.loads(DEMO_FAULTS_CFG.read_text(encoding="utf-8"))
    gen_cfg["transaction_count"] = 60
    gen_cfg["generation_chunk_rows"] = 30

    out_dir = tmp_path / "generated"
    build_dataset(gen_cfg, out_dir)
    source_dir = out_dir / "source"
    batches = discover_statement_deliveries(source_dir)
    assert len(batches) >= 2

    # Chọn mốc ở file_available_at của lô giữa để vừa có lô đến vừa có lô tương lai.
    mid_idx = len(batches) // 2
    cutoff_str = batches[mid_idx - 1].file_available_at
    cutoff_dt = parse_utc_timestamp(cutoff_str)
    expected_early = [b for b in batches if parse_utc_timestamp(b.file_available_at) <= cutoff_dt]
    expected_future = [b for b in batches if parse_utc_timestamp(b.file_available_at) > cutoff_dt]
    assert expected_early and expected_future

    assert ensure_bucket(target, bucket).ready
    try:
        out1 = io.StringIO()
        code1 = run_ingest_statements(
            source_dir=source_dir,
            as_of_time=cutoff_str,
            profile="test",
            env_file=ENV_FILE,
            env=env,
            as_json=True,
            out=out1,
        )
        assert code1 == 0, out1.getvalue()
        p1 = json.loads(out1.getvalue())
        assert p1["eligible_batches"] == len(expected_early)
        assert p1["uploaded_batches"] == len(expected_early)
        assert p1["deferred_batches"] == len(expected_future)

        # Kiểm tra trực tiếp trên MinIO thật: liệt kê toàn bộ bucket phải bằng đúng 3 * early,
        # không rò rỉ lô tương lai hay statement_deliveries.jsonl ở bất kỳ vị trí nào.
        keys_on_minio = _list_keys(target, bucket)
        assert len(keys_on_minio) == 3 * len(expected_early)
        assert all("statement_deliveries" not in k for k in keys_on_minio)

        for batch in expected_early:
            dk, mk, rk = build_statement_keys(cfg, batch)
            assert {dk, mk, rk}.issubset(keys_on_minio)
            assert head_object(target, bucket, dk).sha256 == batch.sha256
            assert head_object(target, bucket, mk).exists
            assert head_object(target, bucket, rk).exists

        for batch in expected_future:
            dk, mk, rk = build_statement_keys(cfg, batch)
            assert not ({dk, mk, rk} & keys_on_minio)

        # Chạy lại cùng mốc -> idempotent.
        out2 = io.StringIO()
        code2 = run_ingest_statements(
            source_dir=source_dir,
            as_of_time=cutoff_str,
            profile="test",
            env_file=ENV_FILE,
            env=env,
            as_json=True,
            out=out2,
        )
        assert code2 == 0
        p2 = json.loads(out2.getvalue())
        assert p2["uploaded_batches"] == 0 and p2["skipped_batches"] == len(expected_early)

        # Tiến đồng hồ tới mốc cuối cùng -> tải nốt các lô còn lại.
        final_cutoff = batches[-1].file_available_at
        out3 = io.StringIO()
        code3 = run_ingest_statements(
            source_dir=source_dir,
            as_of_time=final_cutoff,
            profile="test",
            env_file=ENV_FILE,
            env=env,
            as_json=True,
            out=out3,
        )
        assert code3 == 0
        p3 = json.loads(out3.getvalue())
        assert p3["uploaded_batches"] == len(expected_future)
        assert p3["skipped_batches"] == len(expected_early)
        assert p3["deferred_batches"] == 0
    finally:
        _cleanup_bucket(target, bucket)
