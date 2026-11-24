"""Test `n06 generate` tối thiểu và bước kiểm dung lượng trước `build_dataset` (CFG-scale-guard).

`disk_usage` được tiêm qua `DiskProbe`; `build_dataset` được giả bằng hàm ghi nhận lời gọi, trừ
một ca chạy thật ở quy mô nhỏ. Expected viết tay theo yêu cầu: bị từ chối thì thoát mã 16 và KHÔNG
tạo thư mục hay file nào.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest

from n06.cli import ExitCode, main, run_generate
from n06.limits import METHOD_WSL, DiskProbe

GB = 10**9
CONTRACT_CONFIG = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "N06_INPUT_CONTRACT_V1"
    / "config"
    / "generator.demo-clean.json"
)
CREDENTIALS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak-test",
    "N06_OBJECT_STORE_SECRET_KEY": "sk-test",
    "N06_POSTGRES_MART_PASSWORD": "pw",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw",
    "N06_POSTGRES_METABASE_PASSWORD": "pw",
}


class _Summary:
    source_dir = Path("source")
    business_checksum = "checksum-test"
    event_record_count = 7


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[dict[str, Any], Path]] = []

    def __call__(self, config: Any, out_dir: Path) -> _Summary:
        self.calls.append((dict(config), out_dir))
        return _Summary()


def _probe(free_gb: float | None) -> DiskProbe:
    if free_gb is None:
        return DiskProbe(None, None, METHOD_WSL, "registry không có distro")
    return DiskProbe(int(free_gb * GB), "/mnt/d", METHOD_WSL)


def _generator_config(tmp_path: Path, **overrides: Any) -> Path:
    config = json.loads(CONTRACT_CONFIG.read_text("utf-8"))
    config.update(overrides)
    path = tmp_path / "generator.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def _run(
    tmp_path: Path,
    *,
    probe: DiskProbe,
    env: dict[str, str] | None = None,
    build: Any = None,
    config_overrides: dict[str, Any] | None = None,
    **kwargs: Any,
) -> tuple[int, str, Path]:
    out = io.StringIO()
    target = tmp_path / "out" / "dataset"
    code = run_generate(
        generator_config=_generator_config(tmp_path, **(config_overrides or {})),
        out_dir=target,
        profile="demo",
        env_file=tmp_path / "none.env",
        out=out,
        env={**CREDENTIALS, **(env or {})},
        disk_probe=probe,
        build=build if build is not None else _Recorder(),
        **kwargs,
    )
    return code, out.getvalue(), target


def _forbid_build(*_: Any) -> None:
    raise AssertionError("build_dataset không được gọi khi bị từ chối")


def test_over_max_transactions_is_refused_with_exit_16_and_nothing_created(
    tmp_path: Path,
) -> None:
    code, text, target = _run(
        tmp_path,
        probe=_probe(900.0),
        env={"N06__LIMITS__MAX_TRANSACTIONS": "1000"},
        build=_forbid_build,
        transactions=5000,
    )
    assert code == int(ExitCode.CAPACITY_ERROR) == 16
    for expected in (
        "5,000",
        "1,000",
        "limits.max_transactions",
        "có 900.00 GB",
        "reserve 8.00 GB",
    ):
        assert expected in text
    assert not target.exists() and not target.parent.exists()


def test_insufficient_disk_is_refused_with_figures_and_nothing_created(tmp_path: Path) -> None:
    # 5.000.000 × 4.000 B = 20 GB + 8 GB reserve = 28 GB cần; có 22 GB: thiếu 6 GB.
    code, text, target = _run(
        tmp_path, probe=_probe(22.0), build=_forbid_build, transactions=5_000_000
    )
    assert code == 16
    for expected in ("cần 28.00 GB", "có 22.00 GB", "reserve 8.00 GB", "thiếu 6.00 GB"):
        assert expected in text
    assert not target.parent.exists()


def test_unknown_disk_is_refused_with_exit_16(tmp_path: Path) -> None:
    code, text, target = _run(tmp_path, probe=_probe(None), build=_forbid_build)
    assert code == 16 and "không xác định được chỗ trống" in text
    assert not target.parent.exists()


def test_unmeasured_pipeline_runs_but_warns_it_is_not_a_full_pass(tmp_path: Path) -> None:
    recorder = _Recorder()
    code, text, target = _run(tmp_path, probe=_probe(46.8), build=recorder, transactions=2000)
    assert code == 0
    assert "phần pipeline chưa đo" in text and "không phải PASS đầy đủ" in text
    ((config, out_dir),) = recorder.calls
    assert config["transaction_count"] == 2000 and out_dir == target  # --transactions ghi đè


def test_measured_pipeline_and_enough_space_runs_without_warning(tmp_path: Path) -> None:
    recorder = _Recorder()
    code, text, _ = _run(
        tmp_path,
        probe=_probe(46.8),
        env={"N06__LIMITS__PIPELINE_BYTES_PER_TX": "1000"},
        build=recorder,
    )
    assert code == 0 and "chưa đo" not in text and len(recorder.calls) == 1


def test_json_output_on_refusal_carries_the_capacity_verdict(tmp_path: Path) -> None:
    code, text, _ = _run(
        tmp_path,
        probe=_probe(1.0),
        build=_forbid_build,
        transactions=1_000_000,
        as_json=True,
    )
    payload = json.loads(text)
    assert code == 16 and payload["ok"] is False
    capacity = payload["capacity"]
    assert capacity["status"] == "REFUSE" and capacity["reason"] == "insufficient_disk"
    assert (capacity["required_bytes"], capacity["free_bytes"]) == (12 * GB, 1 * GB)


def test_invalid_generator_configuration_exits_10(tmp_path: Path) -> None:
    code, text, _ = _run(
        tmp_path,
        probe=_probe(900.0),
        build=_forbid_build,
        config_overrides={"transaction_count": 0},
    )
    assert code == 10 and "transaction_count" in text
    out = io.StringIO()
    missing = run_generate(
        generator_config=tmp_path / "missing.json",
        profile="demo",
        env_file=tmp_path / "none.env",
        out=out,
        env=CREDENTIALS,
        disk_probe=_probe(900.0),
        build=_forbid_build,
    )
    assert missing == 10


def test_build_configuration_error_maps_to_exit_10(tmp_path: Path) -> None:
    def rejecting(config: Any, out_dir: Path) -> None:
        raise ValueError("cấu hình generator vi phạm ràng buộc")

    code, text, _ = _run(tmp_path, probe=_probe(900.0), build=rejecting)
    assert code == 10 and "vi phạm ràng buộc" in text


def test_default_output_directory_comes_from_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    recorder = _Recorder()
    out = io.StringIO()
    code = run_generate(
        generator_config=_generator_config(tmp_path),
        profile="demo",
        env_file=tmp_path / "none.env",
        out=out,
        env=CREDENTIALS,
        disk_probe=_probe(900.0),
        build=recorder,
    )
    assert code == 0 and recorder.calls[0][1] == Path("data/generated")


def test_real_small_run_writes_a_dataset_when_space_is_enough(tmp_path: Path) -> None:
    from n06.generator import build_dataset

    out = io.StringIO()
    target = tmp_path / "real"
    code = run_generate(
        generator_config=_generator_config(tmp_path, account_count=20),
        out_dir=target,
        transactions=200,
        profile="demo",
        env_file=tmp_path / "none.env",
        out=out,
        env=CREDENTIALS,
        disk_probe=_probe(900.0),
        build=build_dataset,
    )
    assert code == 0, out.getvalue()
    assert "đã sinh 200 giao dịch" in out.getvalue()
    assert (target / "source").is_dir() and any((target / "source").iterdir())


def test_main_wires_generate_and_refuses_over_the_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key, value in CREDENTIALS.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("N06__LIMITS__MAX_TRANSACTIONS", "10")
    monkeypatch.setenv("N06__LIMITS__DISK_PROBE_PATH", str(tmp_path))
    target = tmp_path / "never"
    out, err = io.StringIO(), io.StringIO()
    code = main(
        [
            "generate",
            "--profile",
            "demo",
            "--env-file",
            str(tmp_path / "none.env"),
            "--generator-config",
            str(_generator_config(tmp_path)),
            "--out",
            str(target),
            "--transactions",
            "11",
        ],
        out=out,
        err=err,
    )
    assert code == 16 and "limits.max_transactions" in out.getvalue()
    assert not target.exists()
