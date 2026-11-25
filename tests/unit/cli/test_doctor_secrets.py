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
from contextlib import nullcontext
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
    profile: str | None = "test",
    config_dir: Path | None = None,
    docker: tuple[CheckItem, str | None] = FAKE_DOCKER,
    services: list[CheckItem] | None = None,
    states: Mapping[str, dict[str, str]] | None = None,
) -> list[tuple[int, str]]:
    env_file = tmp_path / ".env"
    run_init_local(target_path=env_file, skip_buckets=True, out=io.StringIO())
    env = {**parse_dotenv_file(env_file), **overrides}
    results = []
    for as_json in (False, True):
        out = io.StringIO()
        services_patch = (
            patch("n06.cli.doctor.check_services", return_value=services)
            if services is not None
            else nullcontext()
        )
        with (
            patch("n06.cli.doctor.check_docker_engine", return_value=docker),
            patch("n06.cli.doctor._get_compose_service_states", return_value=dict(states or {})),
            patch("n06.cli.doctor._probe_tcp", return_value=(False, "probe giả")),
            patch("n06.cli.doctor._probe_http", return_value=(False, "probe giả")),
            services_patch,
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


RUNNING = {
    name: {"state": "running", "health": "healthy", "name": f"fake-{name}"}
    for name in ("kafka", "minio", "postgres", "spark")
}


@pytest.mark.parametrize(
    ("overrides", "named"),
    [
        pytest.param(
            {"N06__POSTGRES__MART__HOST": f"host=db user=n06 password={MARKER}"},
            "postgres.mart.host",
            id="postgres-key-value-string",
        ),
        pytest.param(
            {"N06__POSTGRES__MART__HOST": f"jdbc:postgresql://db/n06?password={MARKER}"},
            "postgres.mart.host",
            id="postgres-jdbc-url",
        ),
        pytest.param(
            {"N06__POSTGRES__MART__HOST": f"user:{MARKER}\uff20db"},
            "postgres.mart.host",
            id="postgres-fullwidth-at",
        ),
        pytest.param(
            {"N06__KAFKA__BOOTSTRAP_SERVERS": f"password={MARKER};host:9092"},
            "kafka.bootstrap_servers",
            id="kafka-key-value-string",
        ),
    ],
)
def test_address_carrying_credentials_is_rejected_before_it_is_printed(
    tmp_path: Path, overrides: dict[str, str], named: str
) -> None:
    outputs = _doctor_outputs(tmp_path, overrides, states=RUNNING)
    _assert_config_failure_without_marker(outputs, named)


@pytest.mark.parametrize(
    "variable",
    [
        "N06_KAFKA_HOST_PORT",
        "N06_MINIO_API_HOST_PORT",
        "N06_POSTGRES_HOST_PORT",
        "N06_SPARK_UI_HOST_PORT",
    ],
)
def test_out_of_range_port_value_is_not_echoed(tmp_path: Path, variable: str) -> None:
    digits = "913700012345"
    for code, text in _doctor_outputs(tmp_path, {variable: digits}, states=RUNNING):
        assert digits not in text, text
        assert variable in text and code == 10, text


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"N06_PROFILE": MARKER * 30}, id="profile-name-too-long-for-a-file"),
        pytest.param({"N06__KAFKA__NUM_PARTITIONS": "9" * 5000}, id="integer-too-long-to-parse"),
    ],
)
def test_unreadable_configuration_is_a_failure_without_traceback(
    tmp_path: Path, overrides: dict[str, str]
) -> None:
    for code, text in _doctor_outputs(tmp_path, overrides, profile=None):
        assert code == 10 and "config.schema" in text, text
        assert MARKER not in text and "9" * 100 not in text, text


KNOWN_SECRET = "KnownSecret_4471xyz"


def _assert_known_secret_absent(outputs: list[tuple[int, str]]) -> None:
    for _, text in outputs:
        assert KNOWN_SECRET.lower() not in text.lower(), text
        assert "***REDACTED***" in text, text


def test_secret_used_as_bucket_name_is_redacted_in_check_id_too(tmp_path: Path) -> None:
    minio_ok = CheckItem("service.minio", "service", True, 0, "giả", "giả")
    outputs = _doctor_outputs(
        tmp_path,
        {
            "N06_OBJECT_STORE_SECRET_KEY": KNOWN_SECRET,
            "N06__OBJECT_STORE__BUCKET": KNOWN_SECRET,
        },
        services=[minio_ok],
    )
    _assert_known_secret_absent(outputs)


def test_secret_lowercased_by_url_parsing_is_still_redacted(tmp_path: Path) -> None:
    secret = "KnownSecret4471xyz"
    outputs = _doctor_outputs(
        tmp_path,
        {
            "N06_OBJECT_STORE_SECRET_KEY": secret,
            "N06__OBJECT_STORE__ENDPOINT": f"http://{secret}:1",
        },
        states=RUNNING,
    )
    for _, text in outputs:
        assert secret.lower() not in text.lower(), text
        assert "***REDACTED***" in text, text


def test_every_printed_field_of_a_check_is_redacted(tmp_path: Path) -> None:
    leaked = CheckItem(
        f"service.{KNOWN_SECRET}",
        "service",
        False,
        14,
        f"tóm tắt {KNOWN_SECRET}",
        f"chi tiết {KNOWN_SECRET.upper()}",
    )
    outputs = _doctor_outputs(
        tmp_path, {"N06_OBJECT_STORE_SECRET_KEY": KNOWN_SECRET}, services=[leaked]
    )
    _assert_known_secret_absent(outputs)


def test_url_userinfo_in_tool_error_text_is_masked(tmp_path: Path) -> None:
    """`docker info` chép nguyên `DOCKER_HOST` hỏng vào stderr."""
    stderr = f'parse "tcp://user:{MARKER}@127.0.0.1:abc": invalid port'
    failed = CheckItem("docker.engine", "docker", False, 13, "Docker lỗi", stderr)
    for code, text in _doctor_outputs(tmp_path, {}, docker=(failed, None)):
        assert MARKER not in text and "tcp://***@127.0.0.1:abc" in text, text
        assert code == 13, text
