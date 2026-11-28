"""Đối soát chạy bằng Spark thật trong container `spark` (TASK T33; ADR-0006, ADR-0004b).

Một lần chạy container (`recon_spark_worker.py`) xử lý mọi kịch bản; các test bên dưới so kết quả
với kỳ vọng viết tay hoặc tính độc lập (expected_results.json, số học thời gian ở đây, oracle T14),
không gọi `n06.reconciliation` để tạo kỳ vọng. Fixture acceptance được nạp thành dòng Silver theo
ADR-0004b ngay trong tệp này (phần nạp thuộc `tests/`, không thuộc `src/`).

Chạy: `flock /tmp/n06-integration.lock .venv/bin/pytest tests/integration/test_recon_spark.py`.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:  # để import được `tests.harness` (như tests/unit/oracle)
    sys.path.insert(0, str(REPO_ROOT))

from tests.harness.oracle import (  # noqa: E402
    compare_against_oracle,
    evaluate_snapshot,
    load_acceptance_inputs,
)

from n06.cli import parse_dotenv_file  # noqa: E402
from n06.config import load_config  # noqa: E402
from n06.reconciliation import ReconciliationSettings  # noqa: E402

ACCEPTANCE = REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1" / "fixtures" / "acceptance"
COMPOSE = ["compose", "-f", str(REPO_ROOT / "docker-compose.yml"), "--profile", "core"]
DS = "n06-acceptance-v1"
A, B = "SIM_CHANNEL_A", "SIM_CHANNEL_B"


def _load_jsonl(name: str) -> list[dict[str, Any]]:
    with (ACCEPTANCE / name).open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# nạp fixture thành Silver (ADR-0004b)
def silver_transactions_from_acceptance() -> list[dict[str, Any]]:
    """Silver `transactions`: gộp event TRANSACTION_CREATED trùng (replay) theo `transaction_id`."""
    by_id: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    for index, event in enumerate(_load_jsonl("events.jsonl")):
        if event["event_type"] == "TRANSACTION_CREATED":
            by_id.setdefault(event["payload"]["transaction_id"], []).append((index, event))
    rows = []
    for transaction in _load_jsonl("transactions.jsonl"):
        events = by_id[transaction["transaction_id"]]
        event_ids = sorted({event["event_id"] for _, event in events})
        rows.append(
            {
                **{k: transaction[k] for k in transaction if k != "schema_version"},
                "first_event_id": event_ids[0],
                "event_ids": event_ids,
                "replay_count": len(events),
                "source_pointers": sorted(f"events:0:{index}" for index, _ in events),
                "first_kafka_timestamp": events[0][1]["emitted_at"],
                "first_ingested_at": events[0][1]["emitted_at"],
                "run_id": "run-silver-fixture",
            }
        )
    return rows


def silver_statements_from_acceptance() -> list[dict[str, Any]]:
    """Silver `statements`: gắn `file_available_at` và `delivery_order` từ biên nhận (giả lập)."""
    deliveries = {d["statement_record_id"]: d for d in _load_jsonl("statement_deliveries.jsonl")}
    manifest = json.loads((ACCEPTANCE / "source_manifest.json").read_text(encoding="utf-8"))
    sha_by_path = {f["path"]: f["sha256"] for f in manifest["files"]}
    rows = []
    for statement in _load_jsonl("statements.jsonl"):
        delivery = deliveries[statement["statement_record_id"]]
        batch = [
            json.loads(line)["statement_record_id"]
            for line in (REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1" / delivery["file_path"])
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip()
        ]
        rows.append(
            {
                **{k: statement[k] for k in statement if k != "schema_version"},
                "file_available_at": delivery["file_available_at"],
                "delivery_order": delivery["delivery_order"],
                "source_file": delivery["file_path"],
                "source_sha256": sha_by_path[delivery["file_path"]],
                "source_line": batch.index(statement["statement_record_id"]) + 1,
                "run_id": "run-silver-fixture",
            }
        )
    return rows


# dòng Silver tổng hợp cho kịch bản
def tx(ref: str, *, tid: str | None = None, channel: str = A, amount: int = 100,
       currency: str = "VND", status: str = "SUCCESS", event_time: str = "2026-01-01T01:00:00Z",
       business_date: str = "2026-01-01") -> dict[str, Any]:  # fmt: skip
    return {
        "dataset_id": DS, "transaction_id": tid or f"tx-{ref}", "external_reference": ref,
        "channel": channel, "account_from": "acc-a", "account_to": "acc-b",
        "amount_minor": amount, "currency": currency, "status": status,
        "transaction_type": "INTERNAL_TRANSFER", "event_time": event_time,
        "business_date": business_date, "first_event_id": f"ev-{ref}", "event_ids": [f"ev-{ref}"],
        "replay_count": 1, "source_pointers": [f"t:0:{ref}"],
        "first_kafka_timestamp": event_time, "first_ingested_at": event_time, "run_id": "r",
    }  # fmt: skip


def st(ref: str, *, sid: str | None = None, channel: str = A, amount: int = 100,
       currency: str = "VND", status: str = "SUCCESS", event_time: str = "2026-01-01T01:00:00Z",
       business_date: str = "2026-01-01", file_at: str = "2026-01-01T02:00:00Z",
       line: int = 1) -> dict[str, Any]:  # fmt: skip
    return {
        "dataset_id": DS, "statement_record_id": sid or f"st-{ref}", "external_reference": ref,
        "channel": channel, "amount_minor": amount, "currency": currency, "status": status,
        "event_time": event_time, "business_date": business_date, "available_at": file_at,
        "file_available_at": file_at, "delivery_order": 0, "source_file": "f.jsonl",
        "source_sha256": "sha", "source_line": line, "run_id": "r",
    }  # fmt: skip


def kc(ref: str | None, code: str, *, entity: str = "transaction", channel: str | None = A,
       business_date: str = "2026-01-01") -> dict[str, Any]:  # fmt: skip
    return {
        "dataset_id": DS, "business_date": business_date, "side_origin": "INTERNAL",
        "entity": entity, "business_key": "k", "channel": channel, "external_reference": ref,
        "conflict_code": code, "issue_codes": [code], "source_pointers": [f"kc:0:{ref}"],
        "run_id": "r",
    }  # fmt: skip


# chạy container
def _env() -> dict[str, str]:
    env_file = REPO_ROOT / ".env"
    dotenv = parse_dotenv_file(env_file) if env_file.is_file() else {}
    return {**dotenv, **os.environ}


def _base_settings() -> dict[str, Any]:
    cfg = load_config(profile="default", env=_env())
    settings = ReconciliationSettings.from_config(cfg)
    return {
        "allowed_lateness_seconds": settings.allowed_lateness_seconds,
        "business_timezone": settings.business_timezone,
        "statuses": list(settings.statuses),
        "channels": list(settings.channels),
    }


def _scenarios() -> list[dict[str, Any]]:
    base = _base_settings()
    snapshots = json.loads((ACCEPTANCE / "expected_results.json").read_text("utf-8"))["snapshots"]
    cases = json.loads((ACCEPTANCE / "cutoff_cases.json").read_text("utf-8"))
    transactions = silver_transactions_from_acceptance()
    statements = silver_statements_from_acceptance()
    out: list[dict[str, Any]] = []
    for snap in snapshots:
        out.append({
            "name": f"acceptance-{snap['name']}", "settings": base,
            "as_of_time": snap["as_of_time"],
            "transactions": transactions, "statements": statements, "key_conflicts": [],
            "repeat": 2,
        })  # fmt: skip
    for index, case in enumerate(cases["cases"]):
        out.append({
            "name": f"cutoff-{index}", "settings": base, "as_of_time": case["as_of_time"],
            "transactions": [
                t for t in transactions if t["external_reference"] == cases["reference"]
            ],
            "statements": [],
        })  # fmt: skip
    far = "2026-01-03T01:00:00Z"
    out += [
        {  # many-to-many: 2 giao dịch x 3 sao kê độc lập cùng một khóa, và một khóa sạch
            "name": "cardinality",
            "settings": base,
            "as_of_time": far,
            "transactions": [
                tx("R1", tid="t1", amount=10),
                tx("R1", tid="t2", amount=10),
                tx("R2"),
            ],
            "statements": [
                st("R1", sid="s1", amount=10),
                st("R1", sid="s2", amount=10),
                st("R1", sid="s3", amount=10),
                st("R2"),
            ],
        },
        {  # nhiều lỗi một khóa, và ưu tiên giữa các mismatch
            "name": "multi-issue",
            "settings": base,
            "as_of_time": far,
            "transactions": [
                tx("M1", amount=100, currency="VND", status="SUCCESS"),
                tx("M2", amount=100, status="SUCCESS"),
                tx("M3", amount=100, status="SUCCESS"),
            ],
            "statements": [
                st("M1", amount=200, currency="USD", status="FAILED"),
                st("M2", amount=250, status="FAILED"),
                st("M3", amount=100, status="FAILED"),
            ],
        },
        {  # replay sao kê cùng statement_record_id (khác file) không phải duplicate nghiệp vụ
            "name": "statement-replay",
            "settings": base,
            "as_of_time": far,
            "transactions": [tx("P1")],
            "statements": [
                st("P1", sid="same", file_at="2026-01-01T02:00:00Z"),
                st("P1", sid="same", file_at="2026-01-02T02:00:00Z", line=7),
            ],
        },
        {  # đúng thời điểm file có mặt (<=) và sau as_of (loại)
            "name": "visibility",
            "settings": base,
            "as_of_time": "2026-01-01T02:00:00Z",
            "transactions": [tx("V1"), tx("V2")],
            "statements": [
                st("V1", file_at="2026-01-01T02:00:00Z"),
                st("V2", file_at="2026-01-01T02:00:01Z"),
            ],
        },
        {  # key_conflicts: xung đột nguồn, quarantine còn key, khóa chỉ có trong key_conflicts
            "name": "key-conflicts",
            "settings": base,
            "as_of_time": far,
            "transactions": [tx("C1"), tx("C3"), tx("C8")],
            "statements": [st("C1"), st("C2", event_time="2026-01-01T03:00:00Z"), st("C8")],
            "key_conflicts": [
                kc("C1", "BUSINESS_KEY_CONFLICT"),
                kc("C2", "SCHEMA_VALUE", entity="statement"),
                kc("C4", "EVENT_ID_CONFLICT"),
                kc("C5", "SCHEMA_VALUE"),
                kc("C8", "SCHEMA_VALUE"),
                kc(None, "BUSINESS_KEY_CONFLICT"),
                kc("C6", "BUSINESS_KEY_CONFLICT", entity="ledger_entry"),
                kc("C7", "BUSINESS_KEY_CONFLICT", channel="SIM_CHANNEL_X"),
            ],
        },
        {  # thuộc tính NULL ở một phía không được coi là khớp
            "name": "null-attributes",
            "settings": base,
            "as_of_time": far,
            "transactions": [tx("Z1", currency=None), tx("Z2", amount=None)],
            "statements": [st("Z1"), st("Z2")],
        },
        {  # tập channel/status cấu hình
            "name": "config-filter",
            "settings": {**base, "channels": [A], "statuses": ["SUCCESS"]},
            "as_of_time": far,
            "transactions": [tx("F1"), tx("F2", channel=B), tx("F3", status="FAILED")],
            "statements": [st("F1"), st("F2", channel=B)],
        },
        {  # tuổi âm bị kẹp, hai phía khác business_date, anchor của khóa chỉ có sao kê
            "name": "age-and-date",
            "settings": base,
            "as_of_time": "2026-01-01T00:00:00Z",
            "transactions": [
                tx("N1", event_time="2026-01-01T00:00:05Z"),
                tx("N2", business_date="2026-01-01"),
            ],
            "statements": [
                st("N2", business_date="2026-01-02", file_at="2026-01-01T00:00:00Z"),
                st(
                    "N3",
                    sid="n3a",
                    event_time="2026-01-01T00:00:09Z",
                    file_at="2026-01-01T00:00:00Z",
                ),
                st(
                    "N3",
                    sid="n3b",
                    event_time="2025-12-31T23:00:00Z",
                    file_at="2026-01-01T00:00:00Z",
                ),
            ],
        },
    ]
    # cutoff theo cấu hình: lateness 0 và múi giờ khác; so sánh bằng số học độc lập bên dưới
    for as_of in ("2026-01-01T16:59:59Z", "2026-01-01T17:00:00Z"):
        out.append({"name": f"lateness0-hcm-{as_of}",
                    "settings": {**base, "allowed_lateness_seconds": 0},
                    "as_of_time": as_of, "transactions": [tx("L1")], "statements": []})  # fmt: skip
    for as_of in ("2026-01-02T04:59:59Z", "2026-01-02T05:00:00Z"):
        out.append({"name": f"lateness0-ny-{as_of}",
                    "settings": {**base, "allowed_lateness_seconds": 0,
                                 "business_timezone": "America/New_York"},
                    "as_of_time": as_of, "transactions": [tx("L1")], "statements": []})  # fmt: skip
    return out


@pytest.fixture(scope="module")
def worker_output() -> dict[str, Any]:
    docker = shutil.which("docker")
    assert docker is not None, "Test tích hợp cần docker; không bỏ qua."
    cfg = load_config(profile="default", env=_env())
    payload = {
        "app_name": f"{cfg.spark.app_name}-recon-test",
        "master": cfg.spark.master,
        "shuffle_partitions": cfg.spark.shuffle_partitions,
        "scenarios": _scenarios(),
        "error_cases": {
            "settings": _base_settings(), "as_of_time": "2026-01-03T01:00:00Z",
            "transactions": [tx("E1")],
        },
    }  # fmt: skip
    command = [docker, *COMPOSE, "run", "--rm", "--no-deps", "-T"]
    for folder in ("src", "config", "contracts"):
        command += ["-v", f"{REPO_ROOT / folder}:/opt/n06/{folder}:ro"]
    command += ["-v", f"{REPO_ROOT / 'tests' / 'integration'}:/opt/n06/it:ro"]
    command += ["-e", "PYTHONPATH=/opt/n06/src", "-e", "PYTHONDONTWRITEBYTECODE=1"]
    command += ["spark", "python3", "/opt/n06/it/recon_spark_worker.py"]
    proc = subprocess.run(
        command, cwd=REPO_ROOT, env=_env(), input=json.dumps(payload),
        text=True, capture_output=True, check=False,
    )  # fmt: skip
    assert proc.returncode == 0, f"{proc.stderr[-4000:]}\n{proc.stdout[-2000:]}"
    line = next(x for x in proc.stdout.splitlines() if x.startswith("N06_RECON_RESULT="))
    return json.loads(line.removeprefix("N06_RECON_RESULT="))


def _by_ref(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    refs = [row["external_reference"] for row in rows]
    assert len(refs) == len(set(refs)), "Mỗi match key chỉ được có một dòng kết quả"
    return {row["external_reference"]: row for row in rows}


def _scenario(out: dict[str, Any], name: str) -> dict[str, Any]:
    return out["scenarios"][name]


# điều kiện 1, 3, 4: ba snapshot
def test_three_acceptance_snapshots_match_expected_results_and_oracle(
    worker_output: dict[str, Any],
) -> None:
    expected_doc = json.loads((ACCEPTANCE / "expected_results.json").read_text("utf-8"))
    transactions, statements, deliveries = load_acceptance_inputs(ACCEPTANCE)
    tx_rows = {t["external_reference"]: t for t in _load_jsonl("transactions.jsonl")}
    st_rows = _load_jsonl("statements.jsonl")
    delivered = {d["statement_record_id"]: d["file_available_at"] for d in deliveries}
    for snap in expected_doc["snapshots"]:
        result = _scenario(worker_output, f"acceptance-{snap['name']}")
        first, second = result["runs"]
        assert first == second, "Chạy lại cùng snapshot phải cho đúng cùng kết quả"
        rows = _by_ref(first)
        assert {ref: r["recon_status"] for ref, r in rows.items()} == (
            snap["expected_status_by_reference"]
        )
        assert len(rows) == 13
        counts: dict[str, int] = {}
        for row in rows.values():
            counts[row["recon_status"]] = counts.get(row["recon_status"], 0) + 1
        assert counts == snap["expected_counts"]

        as_of = _ts(snap["as_of_time"])
        for ref, row in rows.items():
            assert row["as_of_time"] == snap["as_of_time"] == row["last_evaluated_at"]
            assert row["first_seen_at"] is None and row["resolved_at"] is None
            if row["recon_status"] == "MATCHED":
                assert row["anchor_event_time"] is None
                assert row["discrepancy_age_seconds"] is None
                assert row["issue_codes"] == []
                continue
            # anchor tính độc lập từ tệp tham chiếu: giao dịch, hoặc sao kê sớm nhất đã có mặt
            if ref in tx_rows:
                anchor = tx_rows[ref]["event_time"]
            else:
                anchor = min(
                    s["event_time"]
                    for s in st_rows
                    if s["external_reference"] == ref
                    and _ts(delivered[s["statement_record_id"]]) <= as_of
                )
            assert row["anchor_event_time"] == anchor
            assert row["discrepancy_age_seconds"] == int((as_of - _ts(anchor)).total_seconds())
            assert row["discrepancy_age_seconds"] >= 0
            assert row["issue_codes"], f"{ref} khác MATCHED phải có issue_codes"

        # tiền, currency, status hai phía của từng khóa đủ một bản ghi mỗi phía (spec II.6 dòng 855)
        for ref, row in rows.items():
            if row["recon_status"] == "DUPLICATE_OR_CONFLICT":
                continue
            internal = tx_rows.get(ref)
            seen = [
                s
                for s in st_rows
                if s["external_reference"] == ref
                and _ts(delivered[s["statement_record_id"]]) <= as_of
            ]
            for side, record, prefix in (
                ("internal", internal, "internal"),
                ("statement", seen[0] if seen else None, "statement"),
            ):
                if record is None:
                    assert row[f"{prefix}_amount_minor"] is None, (ref, side)
                    assert row[f"{prefix}_currency"] is None, (ref, side)
                    assert row[f"{prefix}_status"] is None, (ref, side)
                else:
                    assert row[f"{prefix}_amount_minor"] == record["amount_minor"], (ref, side)
                    assert row[f"{prefix}_currency"] == record["currency"], (ref, side)
                    assert row[f"{prefix}_status"] == record["status"], (ref, side)

        oracle = evaluate_snapshot(
            transactions=transactions, statements=statements, deliveries=deliveries,
            as_of_time=snap["as_of_time"],
        )  # fmt: skip
        report = compare_against_oracle(oracle, first)
        assert report.passed, report.mismatches


def test_early_snapshot_reference_age_of_ref_t006_is_97194_seconds(
    worker_output: dict[str, Any],
) -> None:
    """Giá trị ghi trong spec Phần II mục 6 (dòng 857)."""
    row = _by_ref(_scenario(worker_output, "acceptance-early")["runs"][0])["REF-T006"]
    assert row["anchor_event_time"] == "2026-01-01T01:00:06Z"
    assert row["discrepancy_age_seconds"] == 97194


def test_acceptance_transitions_across_snapshots(worker_output: dict[str, Any]) -> None:
    chain = {
        ref: [
            _by_ref(_scenario(worker_output, f"acceptance-{name}")["runs"][0])[ref]["recon_status"]
            for name in ("early", "after_cutoff", "late_corrected")
        ]
        for ref in ("REF-T007", "REF-T008")
    }
    assert chain["REF-T007"] == ["PENDING", "MATCHED", "MATCHED"]
    assert chain["REF-T008"] == ["PENDING", "MISSING_EXTERNAL", "MATCHED"]


def test_acceptance_result_columns_and_types(worker_output: dict[str, Any]) -> None:
    result = _scenario(worker_output, "acceptance-early")
    assert result["columns"] == worker_output["result_columns"]
    # Đúng ADR-0005 mục 2.3, thêm input_snapshot_id.
    assert result["columns"] == [
        "dataset_id", "channel", "external_reference", "business_date", "date_key", "recon_status",
        "issue_codes", "transaction_id", "internal_amount_minor", "internal_currency",
        "internal_status", "statement_amount_minor", "statement_currency", "statement_status",
        "internal_record_count", "statement_record_count", "anchor_event_time",
        "discrepancy_age_seconds", "as_of_time", "last_evaluated_at", "first_seen_at",
        "resolved_at", "recon_rule_version", "source_pointers", "run_id", "input_snapshot_id",
    ]  # fmt: skip
    dtypes = result["dtypes"]
    assert dtypes["internal_amount_minor"] == dtypes["statement_amount_minor"] == "bigint"
    assert dtypes["discrepancy_age_seconds"] == "bigint"
    assert dtypes["issue_codes"] == "array<string>" == dtypes["source_pointers"]
    assert dtypes["date_key"] == dtypes["internal_record_count"] == "int"
    assert not [name for name, dtype in dtypes.items() if dtype in ("double", "float")]
    row = _by_ref(result["runs"][0])["REF-T001"]
    assert (row["date_key"], row["business_date"]) == (20260101, "2026-01-01")
    assert row["recon_rule_version"] == "1.0.0"
    assert (row["run_id"], row["input_snapshot_id"]) == ("run-test", "snap-test")
    assert row["transaction_id"] == "52a6c7e3-a44d-52a1-9ffb-ae06d7bf6522"
    assert (row["internal_amount_minor"], row["statement_amount_minor"]) == (100, 100)
    assert (row["internal_record_count"], row["statement_record_count"]) == (1, 1)
    assert len(row["source_pointers"]) == 2  # một con trỏ giao dịch, một con trỏ sao kê
    assert row["source_pointers"] == sorted(row["source_pointers"])


def test_ref_t009_duplicate_keeps_both_sides_unpicked(worker_output: dict[str, Any]) -> None:
    row = _by_ref(_scenario(worker_output, "acceptance-after_cutoff")["runs"][0])["REF-T009"]
    assert row["recon_status"] == "DUPLICATE_OR_CONFLICT"
    assert row["issue_codes"] == ["DUPLICATE_OR_CONFLICT"]
    assert (row["internal_record_count"], row["statement_record_count"]) == (1, 2)
    assert row["statement_amount_minor"] is None and row["statement_status"] is None
    assert row["transaction_id"] == "3b672e06-0d8c-5db9-ac2d-b37036566bb5"


# điều kiện 3: đúng cutoff là quá hạn
def test_exact_cutoff_is_overdue(worker_output: dict[str, Any]) -> None:
    cases = json.loads((ACCEPTANCE / "cutoff_cases.json").read_text("utf-8"))
    assert cases["cutoff_utc"] == "2026-01-02T17:00:00Z"
    for index, case in enumerate(cases["cases"]):
        result = _scenario(worker_output, f"cutoff-{index}")
        row = _by_ref(result["runs"][0])[cases["reference"]]
        assert row["recon_status"] == case["expected_status"], case["as_of_time"]
        assert set(result["cutoff"].values()) == {cases["cutoff_utc"]}


def test_cutoff_follows_configured_lateness_and_timezone(worker_output: dict[str, Any]) -> None:
    def status(name: str) -> str:
        return _by_ref(_scenario(worker_output, name)["runs"][0])["L1"]["recon_status"]

    # lateness 0, Asia/Ho_Chi_Minh: cutoff ngày 01/01 là 00:00 02/01 giờ VN = 17:00 01/01 UTC
    assert status("lateness0-hcm-2026-01-01T16:59:59Z") == "PENDING"
    assert status("lateness0-hcm-2026-01-01T17:00:00Z") == "MISSING_EXTERNAL"
    # lateness 0, America/New_York (UTC-5 tháng 1): cutoff = 00:00 02/01 giờ NY = 05:00 02/01 UTC
    assert status("lateness0-ny-2026-01-02T04:59:59Z") == "PENDING"
    assert status("lateness0-ny-2026-01-02T05:00:00Z") == "MISSING_EXTERNAL"


# điều kiện 5: cardinality
def test_many_to_many_does_not_multiply_rows(worker_output: dict[str, Any]) -> None:
    result = _scenario(worker_output, "cardinality")
    rows = result["runs"][0]
    assert len(rows) == 2  # naive join sẽ cho 2 x 3 = 6 dòng cho R1
    r1 = _by_ref(rows)["R1"]
    assert r1["recon_status"] == "DUPLICATE_OR_CONFLICT"
    assert (r1["internal_record_count"], r1["statement_record_count"]) == (2, 3)
    assert r1["transaction_id"] is None and r1["internal_amount_minor"] is None
    assert r1["statement_amount_minor"] is None
    assert r1["source_pointers"] == ["f.jsonl:sha:1", "t:0:R1"]
    assert _by_ref(rows)["R2"]["recon_status"] == "MATCHED"


def test_statement_replay_with_same_record_id_is_not_a_business_duplicate(
    worker_output: dict[str, Any],
) -> None:
    row = _by_ref(_scenario(worker_output, "statement-replay")["runs"][0])["P1"]
    assert row["recon_status"] == "MATCHED"
    assert row["statement_record_count"] == 1
    assert row["source_pointers"] == ["f.jsonl:sha:1", "t:0:P1"]  # bản có file sớm nhất


# điều kiện 2: ưu tiên và issue_codes
def test_priority_and_issue_codes_for_multi_issue_keys(worker_output: dict[str, Any]) -> None:
    rows = _by_ref(_scenario(worker_output, "multi-issue")["runs"][0])
    assert rows["M1"]["recon_status"] == "CURRENCY_MISMATCH"
    assert rows["M1"]["issue_codes"] == ["CURRENCY_MISMATCH", "AMOUNT_MISMATCH", "STATUS_MISMATCH"]
    assert rows["M2"]["recon_status"] == "AMOUNT_MISMATCH"
    assert rows["M2"]["issue_codes"] == ["AMOUNT_MISMATCH", "STATUS_MISMATCH"]
    assert rows["M3"]["recon_status"] == "STATUS_MISMATCH"
    assert rows["M3"]["issue_codes"] == ["STATUS_MISMATCH"]
    # tiền hai phía vẫn được ghi (khóa đủ hai phía và không duplicate)
    assert (rows["M1"]["internal_amount_minor"], rows["M1"]["statement_amount_minor"]) == (100, 200)
    assert (rows["M1"]["internal_currency"], rows["M1"]["statement_currency"]) == ("VND", "USD")


def test_missing_and_pending_codes_in_acceptance(worker_output: dict[str, Any]) -> None:
    rows = _by_ref(_scenario(worker_output, "acceptance-early")["runs"][0])
    assert rows["REF-T006"]["issue_codes"] == ["PENDING"]
    assert rows["REF-X012"]["issue_codes"] == ["PENDING"]
    late = _by_ref(_scenario(worker_output, "acceptance-late_corrected")["runs"][0])
    assert late["REF-T006"]["issue_codes"] == ["MISSING_EXTERNAL"]
    assert late["REF-X012"]["issue_codes"] == ["MISSING_INTERNAL"]
    assert late["REF-T003"]["issue_codes"] == ["AMOUNT_MISMATCH"]
    assert late["REF-T004"]["issue_codes"] == ["CURRENCY_MISMATCH"]
    assert late["REF-T005"]["issue_codes"] == ["STATUS_MISMATCH"]


# hiển thị theo as_of
def test_statement_visible_exactly_at_file_available_at(worker_output: dict[str, Any]) -> None:
    rows = _by_ref(_scenario(worker_output, "visibility")["runs"][0])
    assert rows["V1"]["recon_status"] == "MATCHED"  # file_available_at == as_of: có mặt
    assert rows["V2"]["recon_status"] == "PENDING"  # sau as_of 1 giây: chưa có


# key_conflicts
def test_key_conflicts_and_quarantine_markers(worker_output: dict[str, Any]) -> None:
    result = _scenario(worker_output, "key-conflicts")
    rows = _by_ref(result["runs"][0])
    # C1: đủ hai phía khớp nhưng có BUSINESS_KEY_CONFLICT ở nguồn
    assert rows["C1"]["recon_status"] == "DUPLICATE_OR_CONFLICT"
    assert rows["C1"]["issue_codes"] == ["DUPLICATE_OR_CONFLICT", "BUSINESS_KEY_CONFLICT"]
    assert "kc:0:C1" in rows["C1"]["source_pointers"]
    # C2: chỉ có sao kê và một bản ghi sao kê bị quarantine (SCHEMA_VALUE) còn khôi phục được key
    assert rows["C2"]["recon_status"] == "MISSING_INTERNAL"
    assert rows["C2"]["issue_codes"] == ["MISSING_INTERNAL", "COUNTERPART_QUARANTINED"]
    assert rows["C2"]["anchor_event_time"] == "2026-01-01T03:00:00Z"
    # C3: chỉ có giao dịch, không dính key_conflicts
    assert rows["C3"]["issue_codes"] == ["MISSING_EXTERNAL"]
    # C4: chỉ có trong key_conflicts: có dòng, không có event_time để neo
    assert rows["C4"]["recon_status"] == "DUPLICATE_OR_CONFLICT"
    assert rows["C4"]["issue_codes"] == ["DUPLICATE_OR_CONFLICT", "EVENT_ID_CONFLICT"]
    assert rows["C4"]["anchor_event_time"] is None and rows["C4"]["discrepancy_age_seconds"] is None
    assert (rows["C4"]["internal_record_count"], rows["C4"]["statement_record_count"]) == (0, 0)
    # Không bịa khóa: quarantine không còn bản ghi hợp lệ, thiếu key, ledger_entry, kênh ngoài tập
    assert set(rows) == {"C1", "C2", "C3", "C4", "C8"}
    # C8: hai phía khớp, có một bản ghi quarantine cùng khóa: MATCHED, issue_codes rỗng (ADR-0005)
    assert rows["C8"]["recon_status"] == "MATCHED" and rows["C8"]["issue_codes"] == []


# tập channel/status cấu hình
def test_null_attribute_on_one_side_is_a_mismatch_not_a_match(
    worker_output: dict[str, Any],
) -> None:
    rows = _by_ref(_scenario(worker_output, "null-attributes")["runs"][0])
    assert rows["Z1"]["recon_status"] == "CURRENCY_MISMATCH"
    assert rows["Z2"]["recon_status"] == "AMOUNT_MISMATCH"


def test_configured_channel_and_status_sets_filter_both_sides(
    worker_output: dict[str, Any],
) -> None:
    rows = _by_ref(_scenario(worker_output, "config-filter")["runs"][0])
    assert set(rows) == {"F1"}
    assert rows["F1"]["recon_status"] == "MATCHED"


# tuổi, ngày nghiệp vụ, thống kê
def test_negative_age_clamped_business_date_disagreement_and_statement_anchor(
    worker_output: dict[str, Any],
) -> None:
    result = _scenario(worker_output, "age-and-date")
    rows = _by_ref(result["runs"][0])
    # N1: event_time sau as_of 5 giây: tuổi kẹp về 0 và được đếm
    assert rows["N1"]["discrepancy_age_seconds"] == 0
    assert rows["N1"]["anchor_event_time"] == "2026-01-01T00:00:05Z"
    # N2: giao dịch ngày 01/01, sao kê ngày 02/01: lấy ngày giao dịch (Q3) và đếm bất đồng
    assert rows["N2"]["business_date"] == "2026-01-01"
    # N3: chỉ có sao kê (hai ID độc lập): anchor là event_time nhỏ nhất của sao kê
    assert rows["N3"]["recon_status"] == "DUPLICATE_OR_CONFLICT"
    assert rows["N3"]["anchor_event_time"] == "2025-12-31T23:00:00Z"
    assert rows["N3"]["discrepancy_age_seconds"] == 3600
    counters = result["counters"]
    assert counters["age_clamped"] == 1
    assert counters["business_date_disagreements"] == 1
    assert counters["keys"] == 3 and counters["DUPLICATE_OR_CONFLICT"] == 1


# lỗi đầu vào
def test_invalid_inputs_are_rejected_with_clear_messages(worker_output: dict[str, Any]) -> None:
    errors = worker_output["errors"]
    assert "có múi giờ" in errors["naive_as_of"]
    assert "currency" in errors["missing_column"] and "thiếu cột" in errors["missing_column"]
    assert "amount_minor" in errors["float_money"] and "bigint" in errors["float_money"]
    assert "run_id" in errors["empty_run_id"]
    assert "UTC" in errors["non_utc_session"]


def test_midnight_timezone_arithmetic_is_independent_of_the_pipeline() -> None:
    """Kiểm chéo số học cutoff của test: 00:00 giờ VN ngày kế tiếp = 17:00 UTC ngày đó."""
    cutoff = _ts("2026-01-01T17:00:00Z") + timedelta(seconds=86400)
    assert _iso(cutoff) == "2026-01-02T17:00:00Z"
