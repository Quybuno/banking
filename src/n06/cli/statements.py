"""Lệnh `n06 ingest-statements` đưa file sao kê lên MinIO theo lịch giao (TASK T23)."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

from n06.cli.doctor import ExitCode
from n06.cli.init_local import parse_dotenv_file
from n06.cli.validate import parse_as_of_time_utc
from n06.config import DEFAULT_CONFIG_DIR, REPO_ROOT, ConfigValidationError, load_config
from n06.ingestion.statements import (
    DEFAULT_SOURCE_DIRNAME,
    StatementIngestConfigError,
    ingest_statements,
)
from n06.storage.object_store import (
    ObjectStoreConfigError,
    ObjectStoreError,
    target_from_config,
)


def _merge_env(env_file: Path | None, env: Mapping[str, str] | None) -> dict[str, str]:
    merged: dict[str, str] = {}
    dotenv = env_file if env_file is not None else Path(".env")
    if dotenv.is_file():
        merged.update(parse_dotenv_file(dotenv))
    merged.update(os.environ if env is None else env)
    return merged


def _emit(out: TextIO, *, as_json: bool, payload: dict[str, object], text: str) -> None:
    if as_json:
        out.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        out.write(text + "\n")


def run_ingest_statements(
    *,
    source_dir: Path | None = None,
    as_of_time: str | None = None,
    dataset_id: str | None = None,
    profile: str | None = None,
    env_file: Path | None = None,
    env: Mapping[str, str] | None = None,
    config_dir: Path | None = None,
    as_json: bool = False,
    clock: Callable[[], datetime] | None = None,
    out: TextIO = sys.stdout,
) -> int:
    """Thực thi `n06 ingest-statements` từ host tới MinIO."""
    if as_of_time is not None:
        try:
            cutoff = parse_as_of_time_utc(as_of_time)
        except ValueError as exc:
            _emit(
                out,
                as_json=as_json,
                payload={
                    "status": "error",
                    "exit_code": int(ExitCode.CONFIG_ERROR),
                    "detail": str(exc),
                },
                text=f"[ingest-statements] Lỗi --as-of-time: {exc}",
            )
            return int(ExitCode.CONFIG_ERROR)
    else:
        cutoff = (clock() if clock is not None else datetime.now(UTC)).astimezone(UTC)

    merged_env = _merge_env(env_file, env)
    cfg_dir = config_dir if config_dir is not None else DEFAULT_CONFIG_DIR
    try:
        cfg = load_config(profile=profile, config_dir=cfg_dir, env=merged_env)
        target = target_from_config(cfg, merged_env, config_dir=cfg_dir)
    except ConfigValidationError as exc:
        _emit(
            out,
            as_json=as_json,
            payload={
                "status": "error",
                "exit_code": int(ExitCode.CONFIG_ERROR),
                "detail": exc.public_message,
            },
            text=f"[ingest-statements] Lỗi cấu hình: {exc.public_message}",
        )
        return int(ExitCode.CONFIG_ERROR)
    except ObjectStoreConfigError as exc:
        _emit(
            out,
            as_json=as_json,
            payload={
                "status": "error",
                "exit_code": int(ExitCode.CONFIG_ERROR),
                "detail": str(exc),
            },
            text=f"[ingest-statements] Lỗi cấu hình object store: {exc}",
        )
        return int(ExitCode.CONFIG_ERROR)

    if source_dir is None:
        base_dir = Path(cfg.paths.generator_output_dir)
        if not base_dir.is_absolute():
            base_dir = REPO_ROOT / base_dir
        resolved_source = base_dir / DEFAULT_SOURCE_DIRNAME
    else:
        resolved_source = source_dir

    try:
        result = ingest_statements(
            cfg,
            target,
            resolved_source,
            as_of_time=cutoff,
            dataset_id=dataset_id,
        )
    except StatementIngestConfigError as exc:
        _emit(
            out,
            as_json=as_json,
            payload={
                "status": "error",
                "exit_code": int(ExitCode.CONFIG_ERROR),
                "detail": str(exc),
            },
            text=f"[ingest-statements] Lỗi nguồn sao kê: {exc}",
        )
        return int(ExitCode.CONFIG_ERROR)
    except ObjectStoreError as exc:
        _emit(
            out,
            as_json=as_json,
            payload={
                "status": "error",
                "exit_code": int(ExitCode.SERVICE_ERROR),
                "detail": str(exc),
            },
            text=f"[ingest-statements] Lỗi object store: {exc}",
        )
        return int(ExitCode.SERVICE_ERROR)

    payload: dict[str, object] = {
        "status": "ok",
        "exit_code": int(ExitCode.OK),
        "dataset_id": result.dataset_id,
        "as_of_time": result.as_of_time,
        "bucket": result.bucket,
        "total_batches": result.total_batches,
        "eligible_batches": result.eligible_batches,
        "uploaded_batches": result.uploaded_batches,
        "skipped_batches": result.skipped_batches,
        "deferred_batches": result.deferred_batches,
    }
    text = (
        f"[ingest-statements] OK dataset={result.dataset_id} as_of={result.as_of_time} "
        f"bucket={result.bucket} eligible={result.eligible_batches}/{result.total_batches} "
        f"uploaded={result.uploaded_batches} skipped={result.skipped_batches} "
        f"deferred={result.deferred_batches}"
    )
    _emit(out, as_json=as_json, payload=payload, text=text)
    return int(ExitCode.OK)
