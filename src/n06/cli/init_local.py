"""Hiện thực lệnh `n06 init-local` (TASK T03).

Đọc `.env.example`, sinh secret ngẫu nhiên an toàn thay cho các placeholder `CHANGE_ME_*`,
cập nhật `N06_PROFILE` nếu được chỉ định, và ghi ra file `.env` (nằm trong `.gitignore`).
Không bao giờ in giá trị secret ra stdout/stderr.
"""

from __future__ import annotations

import json
import secrets
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from n06.config import REPO_ROOT


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


def run_init_local(
    *,
    profile: str | None = None,
    template_path: Path | None = None,
    target_path: Path | None = None,
    force: bool = False,
    as_json: bool = False,
    out: TextIO | None = None,
) -> int:
    """Thực thi lệnh `n06 init-local` và in báo cáo đã ẩn toàn bộ secret."""
    stream = out if out is not None else sys.stdout
    result = generate_local_env(
        profile=profile,
        template_path=template_path,
        target_path=target_path,
        force=force,
    )

    if as_json:
        payload = {
            "command": "init-local",
            "status": "OK",
            "env_file": str(result.env_file),
            "profile": result.profile,
            "created": result.created,
            "overwritten": result.overwritten,
            "generated_secret_keys": list(result.generated_secret_keys),
            "secrets_redacted": True,
            "message": result.message,
        }
        stream.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    else:
        stream.write(f"[init-local] {result.message}\n")
        if result.generated_secret_keys:
            for key in result.generated_secret_keys:
                stream.write(f"  - {key}=***REDACTED***\n")
    return 0
