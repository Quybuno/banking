"""Lệnh `n06 benchmark`: điều phối chạy thí nghiệm và xuất CSV/JSON/biểu đồ (ADR-0007)."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TextIO

from n06.cli.doctor import ExitCode
from n06.cli.init_local import parse_dotenv_file
from n06.config import REPO_ROOT, ConfigValidationError
from n06.observability.workloads import WorkloadNotRegisteredError, WorkloadRegistry


def run_benchmark_cli(
    *,
    experiment: str,
    levels: Sequence[int] | None = None,
    repetitions: int | None = None,
    warmup: int | None = None,
    seed: int | None = None,
    as_of_time: str | None = None,
    profile: str = "benchmark",
    env_file: Path | None = None,
    output_dir: Path | None = None,
    allow_dirty: bool = False,
    share_baseline: bool = False,
    generator_config: Path | None = None,
    registry: WorkloadRegistry | None = None,
    as_json: bool = False,
    out: TextIO | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    """Thực thi `n06 benchmark` và trả về `ExitCode` có tên."""
    stream = out if out is not None else sys.stdout
    merged_env: dict[str, str] = {}
    resolved_env_file = env_file if env_file is not None else (REPO_ROOT / ".env")
    if resolved_env_file.is_file():
        merged_env.update(parse_dotenv_file(resolved_env_file))
    merged_env.update(env if env is not None else os.environ)

    def fail(code: ExitCode, message: str) -> int:
        if as_json:
            stream.write(
                json.dumps(
                    {
                        "ok": False,
                        "exit_code": int(code),
                        "error_code": code.name,
                        "error": message,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
        else:
            stream.write(f"[benchmark] Lỗi ({code.name}): {message}\n")
        return int(code)

    try:
        from n06.observability.runner import run_benchmark

        summary = run_benchmark(
            experiment=experiment,
            levels=levels,
            repetitions=repetitions,
            warmup=warmup,
            seed=seed,
            as_of_time=as_of_time,
            profile=profile,
            env_file=env_file,
            output_dir=output_dir,
            allow_dirty=allow_dirty,
            share_baseline=share_baseline,
            generator_config=generator_config,
            registry=registry,
            env=merged_env,
        )
    except WorkloadNotRegisteredError as exc:
        return fail(ExitCode.WORKLOAD_UNAVAILABLE, str(exc))
    except ConfigValidationError as exc:
        return fail(ExitCode.CONFIG_ERROR, exc.public_message)
    except RuntimeError as exc:
        return fail(ExitCode.CONFIG_ERROR, str(exc))

    payload = {
        "ok": True,
        "batch_id": summary.batch_id,
        "output_dir": str(summary.output_dir),
        "total_runs": summary.total_runs,
        "counted_runs": summary.counted_runs,
        "files": {k: str(v) for k, v in summary.written_files.items()},
    }
    if as_json:
        stream.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    else:
        stream.write(
            f"[benchmark] batch_id={summary.batch_id} total_runs={summary.total_runs} "
            f"counted_runs={summary.counted_runs} output_dir={summary.output_dir}\n"
        )
    return summary.exit_code
