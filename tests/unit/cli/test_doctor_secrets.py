"""`n06 doctor` không in mật khẩu ở đường lỗi cấu hình.

Mật khẩu có thể bị đặt nhầm trong địa chỉ (`user:mat-khau@host`), trong biến cổng hoặc trong tệp
TOML; những chuỗi này không nằm trong biến `*PASSWORD*`/`*SECRET*` nên bộ lọc secret không biết.
Mỗi ca đặt một chuỗi đánh dấu vào đúng một chỗ rồi kiểm toàn bộ output text và JSON: không có
chuỗi đó (không phân biệt hoa thường), mã thoát là 10 và thông báo nêu tên trường hoặc biến.
"""

from __future__ import annotations

import io
import shutil
from collections.abc import Mapping
from pathlib import Path
from unittest.mock import patch

import pytest

from n06.cli.doctor import CheckItem, SystemResourceSnapshot, run_doctor
from n06.cli.init_local import parse_dotenv_file, run_init_local
from n06.config import DEFAULT_CONFIG_DIR

MARKER = "SentinelPw9137"
SNAPSHOT = SystemResourceSnapshot(3, 11, 9, 8, 16.0, 100.0)
FAKE_DOCKER = (CheckItem("docker.engine", "docker", True, 0, "giả", "giả"), "docker")


def _doctor_outputs(
    tmp_path: Path,
    overrides: Mapping[str, str],
    *,
    profile: str = "test",
    config_dir: Path | None = None,
) -> list[tuple[int, str]]:
    env_file = tmp_path / ".env"
    run_init_local(target_path=env_file, skip_buckets=True, out=io.StringIO())
    env = {**parse_dotenv_file(env_file), **overrides}
    results = []
    for as_json in (False, True):
        out = io.StringIO()
        with (
            patch("n06.cli.doctor.check_docker_engine", return_value=FAKE_DOCKER),
            patch("n06.cli.doctor._get_compose_service_states", return_value={}),
        ):
            code = run_doctor(
                profile=profile,
                env_file=env_file,
                env=env,
                config_dir=config_dir,
                resource_snapshot=SNAPSHOT,
                as_json=as_json,
                out=out,
            )
        results.append((code, out.getvalue()))
    return results


def _assert_config_failure_without_marker(outputs: list[tuple[int, str]], named: str) -> None:
    for code, text in outputs:
        assert MARKER.lower() not in text.lower(), text
        assert named in text, text
        assert code == 10, text


@pytest.mark.parametrize(
    ("overrides", "named"),
    [
        pytest.param(
            {"N06__OBJECT_STORE__ENDPOINT": f"http://user:{MARKER}@127.0.0.1:1"},
            "object_store.endpoint",
            id="endpoint-userinfo",
        ),
        pytest.param(
            {"N06__OBJECT_STORE__ENDPOINT": f"http://user:{MARKER}/x@127.0.0.1:1"},
            "object_store.endpoint",
            id="endpoint-password-with-slash",
        ),
        pytest.param(
            {
                "N06__OBJECT_STORE__ENDPOINT": f"http://user:{MARKER}#@127.0.0.1:1",
                "N06_MINIO_API_HOST_PORT": "",
            },
            "object_store.endpoint",
            id="endpoint-password-with-hash",
        ),
        pytest.param(
            {"N06__OBJECT_STORE__ENDPOINT": f"http://localhost:{MARKER}"},
            "object_store.endpoint",
            id="endpoint-bad-port-without-at-sign",
        ),
        pytest.param(
            {
                "N06__KAFKA__BOOTSTRAP_SERVERS": f"user:{MARKER}@127.0.0.1:9092",
                "N06_KAFKA_HOST_PORT": "",
            },
            "kafka.bootstrap_servers",
            id="kafka-userinfo-empty-port-variable",
        ),
        pytest.param(
            {"N06__KAFKA__BOOTSTRAP_SERVERS": f"user:{MARKER}@127.0.0.1:9092"},
            "kafka.bootstrap_servers",
            id="kafka-userinfo",
        ),
        pytest.param(
            {"N06__KAFKA__BOOTSTRAP_SERVERS": f"localhost:{MARKER}", "N06_KAFKA_HOST_PORT": ""},
            "N06_KAFKA_HOST_PORT",
            id="kafka-bad-default-port-without-at-sign",
        ),
        pytest.param(
            {"N06__POSTGRES__MART__HOST": f"user:{MARKER}@db"},
            "postgres.mart.host",
            id="postgres-userinfo",
        ),
        pytest.param(
            {"N06_POSTGRES_HOST_PORT": MARKER},
            "N06_POSTGRES_HOST_PORT",
            id="value-pasted-into-port-variable",
        ),
        pytest.param(
            {"N06__KAFKA__NUM_PARTITIONS": MARKER},
            "kafka.num_partitions",
            id="value-pasted-into-integer-field",
        ),
    ],
)
def test_misplaced_password_is_never_printed(
    tmp_path: Path, overrides: dict[str, str], named: str
) -> None:
    _assert_config_failure_without_marker(_doctor_outputs(tmp_path, overrides), named)


@pytest.mark.parametrize("profile", ["test", "demo"])
def test_credential_written_in_a_toml_file_is_never_printed(tmp_path: Path, profile: str) -> None:
    """Profile `demo` qua được kiểm cấu hình, lỗi nổi lên khi suy địa chỉ từ `test.toml`."""
    config_dir = tmp_path / "config"
    shutil.copytree(DEFAULT_CONFIG_DIR, config_dir)
    path = config_dir / "test.toml"
    original = path.read_text(encoding="utf-8")
    changed = original.replace("[object_store]\n", f'[object_store]\nsecret_key = "{MARKER}"\n')
    assert changed != original
    path.write_text(changed, encoding="utf-8")

    outputs = _doctor_outputs(tmp_path, {}, profile=profile, config_dir=config_dir)

    _assert_config_failure_without_marker(outputs, "object_store.secret_key")
