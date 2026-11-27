"""Lệnh `n06 generate` tối thiểu: kiểm dung lượng rồi gọi `build_dataset` (CFG-scale-guard).

Phạm vi hẹp có chủ đích: chỉ nhận cấu hình generator (`--generator-config`), thư mục đầu ra và
`--transactions`. Các cờ còn lại của lệnh `n06 generate` trong đặc tả (Phần I mục 12) như
`--dataset-id`, `--seed`, `--from-date`, `--to-date` chưa có ở đây và thuộc task sở hữu lệnh đó.

Bước kiểm dung lượng chạy TRƯỚC `build_dataset`: nếu bị từ chối (mã 16) thì không tạo thư mục
hay file nào. Số liệu và ngưỡng lấy từ `config/` mục `[limits]`; xem `n06.limits`.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any, TextIO

from n06.cli.doctor import ExitCode
from n06.cli.init_local import parse_dotenv_file
from n06.config import REPO_ROOT, AppConfig, ConfigValidationError, load_config
from n06.limits import CapacityVerdict, DiskProbe, ensure_capacity

BuildDataset = Callable[[Mapping[str, Any], Path], Any]


def _nearest_existing_parent(path: Path) -> Path:
    """Thư mục đã tồn tại gần nhất của `path` (để đọc chỗ trống mà không tạo gì)."""
    candidate = path.resolve()
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def _load_app_config(
    profile: str | None, env_file: Path | None, merged_env: Mapping[str, str]
) -> AppConfig:
    resolved_env_file = env_file if env_file is not None else (REPO_ROOT / ".env")
    env: dict[str, str] = {}
    if resolved_env_file.is_file():
        env.update(parse_dotenv_file(resolved_env_file))
    env.update(merged_env)
    return load_config(profile=profile, env=env)


def _verdict_dict(verdict: CapacityVerdict) -> dict[str, Any]:
    return asdict(verdict)


def run_generate(
    *,
    generator_config: Path,
    out_dir: Path | None = None,
    transactions: int | None = None,
    profile: str | None = None,
    env_file: Path | None = None,
    as_json: bool = False,
    out: TextIO | None = None,
    env: Mapping[str, str] | None = None,
    disk_probe: DiskProbe | None = None,
    build: BuildDataset | None = None,
) -> int:
    """Thực thi `n06 generate`; mã thoát: 0 xong, 10 cấu hình hoặc cấu hình generator sai,
    12 lỗi ghi đĩa, 16 bị từ chối vì dung lượng hoặc vượt `limits.max_transactions`."""
    stream = out if out is not None else sys.stdout
    merged_env: Mapping[str, str] = env if env is not None else os.environ

    def fail(code: ExitCode, message: str, verdict: CapacityVerdict | None = None) -> int:
        if as_json:
            payload: dict[str, Any] = {"ok": False, "error": message}
            if verdict is not None:
                payload["capacity"] = _verdict_dict(verdict)
            stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
        else:
            stream.write(f"[generate] Lỗi: {message}\n")
        return int(code)

    try:
        cfg = _load_app_config(profile, env_file, merged_env)
    except ConfigValidationError as exc:
        return fail(ExitCode.CONFIG_ERROR, exc.public_message)

    try:
        generator_cfg = json.loads(generator_config.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return fail(
            ExitCode.CONFIG_ERROR,
            f"Không đọc được cấu hình generator '{generator_config}': {type(exc).__name__}",
        )
    if not isinstance(generator_cfg, dict):
        return fail(ExitCode.CONFIG_ERROR, "Cấu hình generator phải là một đối tượng JSON.")
    if transactions is not None:
        generator_cfg["transaction_count"] = transactions
    requested = generator_cfg.get("transaction_count")
    if type(requested) is not int or requested < 1:
        return fail(
            ExitCode.CONFIG_ERROR,
            "transaction_count của generator phải là số nguyên >= 1; đặt bằng --transactions "
            "hoặc trong tệp cấu hình.",
        )

    target_dir = out_dir if out_dir is not None else Path(cfg.paths.generator_output_dir)
    verdict = ensure_capacity(
        requested,
        cfg.limits,
        _nearest_existing_parent(target_dir),
        probe=disk_probe,
        target_path=target_dir,
    )
    if verdict.refused:
        # Từ chối trước mọi thao tác ghi: không tạo thư mục hay file dở.
        return fail(ExitCode.CAPACITY_ERROR, verdict.message, verdict)
    if not as_json and verdict.status != "PASS":
        stream.write(f"[generate] {verdict.message}\n")

    # Nhập muộn: nặng và chỉ cần khi chạy thật. Chỉ các lỗi cấu hình đã biết mới ra mã 10;
    # ValueError do lỗi nội bộ của generator không bị nuốt.
    from n06.generator import FaultConfigError, ScenarioConfigError, build_dataset

    if build is None:
        build = build_dataset
    try:
        summary = build(generator_cfg, target_dir)
    except OSError as exc:
        return fail(ExitCode.RESOURCE_ERROR, f"Lỗi ghi đĩa khi sinh dữ liệu: {exc}")
    except (FaultConfigError, ScenarioConfigError) as exc:
        return fail(ExitCode.CONFIG_ERROR, str(exc))

    result = {
        "ok": True,
        "transactions": requested,
        "out_dir": str(target_dir),
        "source_dir": str(summary.source_dir),
        "business_checksum": summary.business_checksum,
        "event_record_count": summary.event_record_count,
        "capacity": _verdict_dict(verdict),
    }
    if as_json:
        stream.write(json.dumps(result, ensure_ascii=False) + "\n")
    else:
        stream.write(
            f"[generate] đã sinh {requested:,} giao dịch vào {target_dir} "
            f"({summary.event_record_count:,} event, checksum {summary.business_checksum})\n"
        )
    return int(ExitCode.OK)
