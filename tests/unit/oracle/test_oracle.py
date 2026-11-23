"""Unit tests cho test harness oracle và lệnh `n06 validate` (TASK T14)."""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path

from tests.harness.oracle import (
    ReconStatus,
    compare_against_oracle,
    compute_cutoff_utc,
    evaluate_snapshot,
    load_acceptance_inputs,
    parse_utc_timestamp,
)

from n06.cli import ExitCode
from n06.cli import main as cli_main

REPO_ROOT = Path(__file__).resolve().parents[3]
ACCEPTANCE_DIR = REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1" / "fixtures" / "acceptance"


def _load_expected() -> dict:
    return json.loads((ACCEPTANCE_DIR / "expected_results.json").read_text(encoding="utf-8"))


def test_oracle_matches_all_three_acceptance_snapshots() -> None:
    """Oracle tính đúng trạng thái từng khóa, histogram và tuổi sai lệch ở cả 3 snapshot."""
    transactions, statements, deliveries = load_acceptance_inputs(ACCEPTANCE_DIR)
    expected_doc = _load_expected()

    for snap in expected_doc["snapshots"]:
        as_of_dt = parse_utc_timestamp(snap["as_of_time"])
        result = evaluate_snapshot(
            transactions=transactions,
            statements=statements,
            deliveries=deliveries,
            as_of_time=as_of_dt,
        )

        actual_status_by_ref = {
            ref: rec.recon_status.value for ref, rec in result.by_reference.items()
        }
        assert actual_status_by_ref == snap["expected_status_by_reference"]
        assert result.counts_by_status == snap["expected_counts"]

        # Kiểm tra discrepancy_age_seconds và anchor_event_time cho mọi khóa
        for rec in result.records:
            if rec.recon_status == ReconStatus.MATCHED:
                assert rec.anchor_event_time is None
                assert rec.discrepancy_age_seconds is None
            else:
                assert rec.anchor_event_time is not None
                assert rec.discrepancy_age_seconds is not None
                assert rec.discrepancy_age_seconds >= 0
                expected_age = int((as_of_dt - rec.anchor_event_time).total_seconds())
                assert rec.discrepancy_age_seconds == expected_age


def test_oracle_transitions_and_discrepancy_age_values() -> None:
    """Kiểm tra chuỗi T007, T008 qua 3 snapshot và tuổi sai lệch tính tay của T006, X012."""
    transactions, statements, deliveries = load_acceptance_inputs(ACCEPTANCE_DIR)
    expected_doc = _load_expected()
    snapshots = {
        snap["name"]: evaluate_snapshot(
            transactions=transactions,
            statements=statements,
            deliveries=deliveries,
            as_of_time=snap["as_of_time"],
        )
        for snap in expected_doc["snapshots"]
    }

    # Chuỗi chuyển trạng thái T007: PENDING -> MATCHED -> MATCHED
    assert snapshots["early"].by_reference["REF-T007"].recon_status == ReconStatus.PENDING
    assert snapshots["after_cutoff"].by_reference["REF-T007"].recon_status == ReconStatus.MATCHED
    assert snapshots["late_corrected"].by_reference["REF-T007"].recon_status == ReconStatus.MATCHED

    # Chuỗi chuyển trạng thái T008: PENDING -> MISSING_EXTERNAL -> MATCHED
    assert snapshots["early"].by_reference["REF-T008"].recon_status == ReconStatus.PENDING
    assert (
        snapshots["after_cutoff"].by_reference["REF-T008"].recon_status
        == ReconStatus.MISSING_EXTERNAL
    )
    assert snapshots["late_corrected"].by_reference["REF-T008"].recon_status == ReconStatus.MATCHED

    # Ví dụ tính tay trong docs/spec.md Phần II mục 6: REF-T006 tại early = 97194 giây
    t006_early = snapshots["early"].by_reference["REF-T006"]
    assert t006_early.anchor_event_time == datetime(2026, 1, 1, 1, 0, 6, tzinfo=UTC)
    assert t006_early.discrepancy_age_seconds == 97_194

    # REF-X012 (MISSING_INTERNAL): anchor là event_time của statement (2026-01-01T01:00:12Z)
    x012_after = snapshots["after_cutoff"].by_reference["REF-X012"]
    assert x012_after.recon_status == ReconStatus.MISSING_INTERNAL
    assert x012_after.anchor_event_time == datetime(2026, 1, 1, 1, 0, 12, tzinfo=UTC)
    # 2026-01-02T18:00:00Z - 2026-01-01T01:00:12Z = 1d (86400) + 16h59m48s (61188) = 147588s
    assert x012_after.discrepancy_age_seconds == 147_588


def test_oracle_cutoff_boundary_cases() -> None:
    """as_of đúng tại cutoff (17:00:00Z) được coi là quá hạn; trước 1 giây vẫn PENDING."""
    cutoff_doc = json.loads((ACCEPTANCE_DIR / "cutoff_cases.json").read_text(encoding="utf-8"))
    computed_cutoff = compute_cutoff_utc(
        cutoff_doc["business_date"],
        allowed_lateness_seconds=cutoff_doc["allowed_lateness_seconds"],
    )
    assert computed_cutoff == parse_utc_timestamp(cutoff_doc["cutoff_utc"])

    transactions, statements, deliveries = load_acceptance_inputs(ACCEPTANCE_DIR)
    ref = cutoff_doc["reference"]

    for case in cutoff_doc["cases"]:
        res = evaluate_snapshot(
            transactions=transactions,
            statements=statements,
            deliveries=deliveries,
            as_of_time=case["as_of_time"],
            allowed_lateness_seconds=cutoff_doc["allowed_lateness_seconds"],
        )
        assert res.by_reference[ref].recon_status.value == case["expected_status"]


def test_compare_against_oracle_pass_and_mismatch() -> None:
    """Kiểm tra hàm đối chiếu kết quả thực tế với oracle cho cả ca PASS và FAIL."""
    transactions, statements, deliveries = load_acceptance_inputs(ACCEPTANCE_DIR)
    expected = evaluate_snapshot(
        transactions=transactions,
        statements=statements,
        deliveries=deliveries,
        as_of_time="2026-01-02T04:00:00Z",
    )

    # Tạo bản sao hoàn hảo từ expected để test ca PASS
    perfect_actual = [
        {
            "dataset_id": rec.dataset_id,
            "channel": rec.channel,
            "external_reference": rec.external_reference,
            "recon_status": rec.recon_status.value,
            "anchor_event_time": (
                rec.anchor_event_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                if rec.anchor_event_time
                else None
            ),
            "discrepancy_age_seconds": rec.discrepancy_age_seconds,
        }
        for rec in expected.records
    ]
    pass_report = compare_against_oracle(expected, perfect_actual)
    assert pass_report.passed is True
    assert pass_report.mismatches == ()

    # Sửa 1 bản ghi sai trạng thái và sai tuổi -> phải báo mismatch
    broken_actual = [dict(row) for row in perfect_actual]
    for row in broken_actual:
        if row["external_reference"] == "REF-T006":
            row["recon_status"] = "MATCHED"
            row["discrepancy_age_seconds"] = None
    fail_report = compare_against_oracle(expected, broken_actual)
    assert fail_report.passed is False
    mismatch_fields = {(m.match_key[2], m.field) for m in fail_report.mismatches}
    assert ("REF-T006", "recon_status") in mismatch_fields
    assert ("REF-T006", "discrepancy_age_seconds") in mismatch_fields


def test_cli_validate_command() -> None:
    """Kiểm tra khung CLI `n06 validate --against-ground-truth`."""
    # --help liệt kê validate
    out_help = io.StringIO()
    err_help = io.StringIO()
    assert cli_main(["--help"], out=out_help, err=err_help) == 0
    assert "validate" in out_help.getvalue()

    # Chạy validate --against-ground-truth với snapshot early (truyền --harness-module rõ ràng)
    out_ok = io.StringIO()
    err_ok = io.StringIO()
    code_ok = cli_main(
        [
            "validate",
            "--dataset-id",
            "n06-acceptance-v1",
            "--against-ground-truth",
            "--as-of-time",
            "2026-01-02T04:00:00Z",
            "--harness-module",
            "tests.harness.oracle",
            "--json",
        ],
        out=out_ok,
        err=err_ok,
    )
    assert code_ok == int(ExitCode.OK)
    payload = json.loads(out_ok.getvalue())
    assert payload["match_key_count"] == 13
    assert payload["counts_by_status"]["PENDING"] == 4
    assert payload["counts_by_status"]["MATCHED"] == 5

    # Khi không cấu hình harness (môi trường cài đặt thật không có tests/) -> CONFIG_ERROR (10)
    out_no_harness = io.StringIO()
    err_no_harness = io.StringIO()
    code_no_harness = cli_main(
        [
            "validate",
            "--dataset-id",
            "n06-acceptance-v1",
            "--against-ground-truth",
            "--as-of-time",
            "2026-01-02T04:00:00Z",
        ],
        out=out_no_harness,
        err=err_no_harness,
    )
    assert code_no_harness == int(ExitCode.CONFIG_ERROR)

    # Thiếu múi giờ UTC ở --as-of-time -> CLI_USAGE_ERROR (2)
    out_bad_ts = io.StringIO()
    err_bad_ts = io.StringIO()
    code_bad_ts = cli_main(
        [
            "validate",
            "--dataset-id",
            "n06-acceptance-v1",
            "--against-ground-truth",
            "--as-of-time",
            "2026-01-02T04:00:00",
        ],
        out=out_bad_ts,
        err=err_bad_ts,
    )
    assert code_bad_ts == int(ExitCode.CLI_USAGE_ERROR)

    # Không truyền --against-ground-truth -> CLI_USAGE_ERROR (2)
    out_no_flag = io.StringIO()
    err_no_flag = io.StringIO()
    code_no_flag = cli_main(
        [
            "validate",
            "--dataset-id",
            "n06-acceptance-v1",
            "--as-of-time",
            "2026-01-02T04:00:00Z",
        ],
        out=out_no_flag,
        err=err_no_flag,
    )
    assert code_no_flag == int(ExitCode.CLI_USAGE_ERROR)
