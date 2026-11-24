"""Hiện thực lệnh `n06 init-local` (TASK T03, T03b).

Đọc `.env.example`, sinh secret ngẫu nhiên an toàn thay cho các placeholder `CHANGE_ME_*`,
cập nhật `N06_PROFILE` nếu được chỉ định, và ghi ra file `.env` (nằm trong `.gitignore`).
Sau đó (T03b) tạo bucket object store do cấu hình của profile định nghĩa, an toàn khi chạy lại.
Không bao giờ in giá trị secret ra stdout/stderr.
"""

from __future__ import annotations

import json
import os
import secrets
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from n06.cli.object_store import (
    STATUS_ERROR,
    STATUS_UNREACHABLE,
    BucketResult,
    ObjectStoreConfigError,
    ensure_bucket,
    target_from_config,
    validate_bucket_name,
)
from n06.config import DEFAULT_CONFIG_DIR, REPO_ROOT, ConfigValidationError, load_config


@dataclass(frozen=True)
class InitLocalResult:
    """Kết quả thực thi lệnh `n06 init-local` (không chứa giá trị secret)."""

    env_file: Path
    profile: str
    created: bool
    overwritten: bool
    generated_secret_keys: tuple[str, ...]
    message: str


def parse_dotenv_file(path: Path) -> dict[str, str]:
    """Đọc file định dạng `.env` thành dict `key -> value`."""
    result: dict[str, str] = {}
    if not path.is_file():
        return result
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, val = stripped.partition("=")
        result[key.strip()] = val.strip()
    return result


def generate_local_env(
    *,
    profile: str | None = None,
    template_path: Path | None = None,
    target_path: Path | None = None,
    force: bool = False,
) -> InitLocalResult:
    """Sinh file `.env` từ `.env.example` với secret ngẫu nhiên cho các biến `CHANGE_ME_*`."""
    resolved_template = template_path if template_path is not None else (REPO_ROOT / ".env.example")
    resolved_target = target_path if target_path is not None else (REPO_ROOT / ".env")

    if not resolved_template.is_file():
        raise FileNotFoundError(f"Không tìm thấy file mẫu biến môi trường: {resolved_template}")

    existed_before = resolved_target.is_file()
    if existed_before and not force:
        existing_vars = parse_dotenv_file(resolved_target)
        current_profile = existing_vars.get("N06_PROFILE", profile or "demo")
        return InitLocalResult(
            env_file=resolved_target,
            profile=current_profile,
            created=False,
            overwritten=False,
            generated_secret_keys=(),
            message=(
                f"File {resolved_target} đã tồn tại (giữ nguyên để không phá dữ liệu/cấu hình cũ; "
                "dùng --force nếu muốn tạo lại)."
            ),
        )

    output_lines: list[str] = []
    generated_keys: list[str] = []
    effective_profile = profile or "demo"

    for raw_line in resolved_template.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            output_lines.append(raw_line)
            continue

        key, _, val = raw_line.partition("=")
        clean_key = key.strip()
        clean_val = val.strip()

        if clean_key == "N06_PROFILE":
            if profile is not None:
                output_lines.append(f"N06_PROFILE={profile}")
                effective_profile = profile
            else:
                output_lines.append(raw_line)
                effective_profile = clean_val
            continue

        if clean_val.startswith("CHANGE_ME_"):
            random_secret = f"n06_{secrets.token_urlsafe(24)}"
            output_lines.append(f"{clean_key}={random_secret}")
            generated_keys.append(clean_key)
        else:
            output_lines.append(raw_line)

    resolved_target.parent.mkdir(parents=True, exist_ok=True)
    resolved_target.write_text("\n".join(output_lines) + "\n", encoding="utf-8")

    return InitLocalResult(
        env_file=resolved_target,
        profile=effective_profile,
        created=not existed_before,
        overwritten=existed_before and force,
        generated_secret_keys=tuple(generated_keys),
        message=(
            f"Đã khởi tạo file môi trường cục bộ tại {resolved_target} "
            f"(profile={effective_profile}, đã sinh {len(generated_keys)} biến secret)."
        ),
    )


def _ensure_profile_buckets(
    result: InitLocalResult,
    *,
    bucket_profile: str,
    env: Mapping[str, str] | None,
    config_dir: Path | None,
) -> tuple[list[BucketResult], str | None]:
    """Tạo bucket của profile đang dùng; trả `(kết quả, lỗi cấu hình hoặc None)`.

    Bucket duy nhất cần dùng là `object_store.bucket` của profile (ADR-0002 mục 1). Không in giá
    trị cấu hình: lỗi cấu hình chỉ nêu đường dẫn trường.
    """
    merged: dict[str, str] = dict(parse_dotenv_file(result.env_file))
    merged.update(env if env is not None else os.environ)
    resolved_dir = config_dir if config_dir is not None else DEFAULT_CONFIG_DIR
    try:
        cfg = load_config(profile=bucket_profile, config_dir=resolved_dir, env=merged)
        bucket = validate_bucket_name(cfg.object_store.bucket)
        target = target_from_config(cfg, merged, config_dir=resolved_dir)
    except ConfigValidationError as exc:
        return [], f"cấu hình không hợp lệ tại trường '{exc.field_path}'"
    except ObjectStoreConfigError as exc:
        return [], str(exc)
    return [ensure_bucket(target, bucket)], None


def run_init_local(
    *,
    profile: str | None = None,
    template_path: Path | None = None,
    target_path: Path | None = None,
    force: bool = False,
    skip_buckets: bool = False,
    require_buckets: bool = False,
    env: Mapping[str, str] | None = None,
    config_dir: Path | None = None,
    as_json: bool = False,
    out: TextIO | None = None,
) -> int:
    """Thực thi lệnh `n06 init-local` và in báo cáo đã ẩn toàn bộ secret.

    Mã thoát: 0 khi thành công; 0 kèm cảnh báo khi MinIO chưa chạy (bucket `unreachable`) trừ khi
    có `require_buckets` (khi đó 14); 14 khi object store từ chối hoặc lỗi; 10 khi cấu hình hoặc
    tên bucket không hợp lệ.
    """
    # Nhập muộn để tránh vòng nhập: `doctor` nhập `parse_dotenv_file` từ module này.
    from n06.cli.doctor import ExitCode

    stream = out if out is not None else sys.stdout
    result = generate_local_env(
        profile=profile,
        template_path=template_path,
        target_path=target_path,
        force=force,
    )

    # Profile được chỉ định tường minh quyết định bucket cần tạo; nếu không thì theo `.env`.
    bucket_profile = profile if profile is not None else result.profile
    buckets: list[BucketResult] = []
    config_problem: str | None = None
    if not skip_buckets:
        buckets, config_problem = _ensure_profile_buckets(
            result, bucket_profile=bucket_profile, env=env, config_dir=config_dir
        )

    exit_code = int(ExitCode.OK)
    if config_problem is not None:
        exit_code = int(ExitCode.CONFIG_ERROR)
    elif any(b.status == STATUS_ERROR for b in buckets):
        exit_code = int(ExitCode.SERVICE_ERROR)
    elif require_buckets and any(not b.ready for b in buckets):
        exit_code = int(ExitCode.SERVICE_ERROR)
    warning = any(b.status == STATUS_UNREACHABLE for b in buckets)
    status = "ERROR" if exit_code != int(ExitCode.OK) else ("WARN" if warning else "OK")

    if as_json:
        payload = {
            "command": "init-local",
            "status": status,
            "env_file": str(result.env_file),
            "profile": result.profile,
            "bucket_profile": bucket_profile,
            "created": result.created,
            "overwritten": result.overwritten,
            "generated_secret_keys": list(result.generated_secret_keys),
            "secrets_redacted": True,
            "message": result.message,
            "buckets_skipped": skip_buckets,
            "require_buckets": require_buckets,
            "buckets_ready": (not skip_buckets and config_problem is None)
            and all(b.ready for b in buckets),
            "buckets": [
                {"name": b.bucket, "status": b.status, "detail": b.detail} for b in buckets
            ],
            "bucket_error": config_problem,
        }
        stream.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    else:
        stream.write(f"[init-local] {result.message}\n")
        if result.generated_secret_keys:
            for key in result.generated_secret_keys:
                stream.write(f"  - {key}=***REDACTED***\n")
        if skip_buckets:
            stream.write("[init-local] Bỏ qua bước tạo bucket (--skip-buckets).\n")
        if config_problem is not None:
            stream.write(f"[init-local] LỖI bucket: {config_problem}\n")
        for b in buckets:
            stream.write(f"[init-local] bucket '{b.bucket}': {b.status} ({b.detail})\n")
            if b.status == STATUS_UNREACHABLE:
                stream.write(
                    "[init-local] CẢNH BÁO: bucket chưa được tạo. Khởi động hạ tầng bằng "
                    "`docker compose --profile core up -d` rồi chạy lại `n06 init-local`"
                    f" (profile {bucket_profile}); lệnh chạy lại an toàn.\n"
                )
    return exit_code
