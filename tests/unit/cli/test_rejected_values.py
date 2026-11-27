"""Quy tắc: output không in giá trị của trường cấu hình bị từ chối.

Giá trị bị từ chối có thể là mật khẩu đặt nhầm chỗ. Mỗi ca đặt chuỗi đánh dấu vào đúng một trường
bị từ chối rồi kiểm: chuỗi đó không có trong output, mã thoát đúng, và output nêu tên trường.
"""

from __future__ import annotations

import io
import json
import subprocess
from collections.abc import Mapping
from pathlib import Path
from unittest.mock import patch

import pytest

from n06.cli.doctor import (
    CheckItem,
    SystemResourceSnapshot,
    check_docker_engine,
    run_doctor,
)
from n06.cli.generate import run_generate
from n06.cli.init_local import parse_dotenv_file, run_init_local
from n06.cli.publish import run_publish
from n06.limits import METHOD_CONFIG, DiskProbe, resolve_disk_probe
from n06.producer.errors import PublishConfigError
from n06.producer.kafka_client import build_producer_config, make_producer

MARKER = "SentinelPw5521"
REPO_ROOT = Path(__file__).resolve().parents[3]
GENERATOR_CONFIG = (
    REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1" / "config" / "generator.demo-clean.json"
)
SNAPSHOT = SystemResourceSnapshot(3, 11, 9, 8, 16.0, 100.0)
CREDENTIALS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak-test",
    "N06_OBJECT_STORE_SECRET_KEY": "sk-test",
    "N06_POSTGRES_MART_PASSWORD": "pw",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw",
    "N06_POSTGRES_METABASE_PASSWORD": "pw",
}
REJECTED = [
    pytest.param({"N06__KAFKA__NUM_PARTITIONS": MARKER}, "kafka.num_partitions", id="wrong-type"),
    pytest.param(
        {"N06__PRODUCER__KEY_STRATEGY": MARKER}, "producer.key_strategy", id="outside-enum"
    ),
    pytest.param({"N06__DATASET__DATASET_ID": MARKER}, "dataset.dataset_id", id="bad-pattern"),
    pytest.param({"N06_PROFILE": MARKER}, "profile", id="unknown-profile"),
    pytest.param({"N06_PROFILE": MARKER * 40}, "profile", id="profile-too-long-for-a-file"),
]


def _assert_rejected(code: int, text: str, named: str) -> None:
    assert MARKER.lower() not in text.lower(), text
    assert named in text, text
    assert code == 10, text


@pytest.mark.parametrize(("overrides", "named"), REJECTED)
@pytest.mark.parametrize("as_json", [False, True])
def test_publish_names_the_field_without_its_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    overrides: Mapping[str, str],
    named: str,
    as_json: bool,
) -> None:
    for key, value in {**CREDENTIALS, **overrides}.items():
        monkeypatch.setenv(key, value)
    out = io.StringIO()
    code = run_publish(
        profile=None if "N06_PROFILE" in overrides else "test",
        env_file=tmp_path / "none.env",
        source_dir=tmp_path,
        as_json=as_json,
        out=out,
    )
    _assert_rejected(code, out.getvalue(), named)


@pytest.mark.parametrize(("overrides", "named"), REJECTED)
def test_generate_names_the_field_without_its_value(
    tmp_path: Path, overrides: Mapping[str, str], named: str
) -> None:
    out = io.StringIO()
    code = run_generate(
        generator_config=GENERATOR_CONFIG,
        out_dir=tmp_path / "out",
        profile=None if "N06_PROFILE" in overrides else "demo",
        env_file=tmp_path / "none.env",
        out=out,
        env={**CREDENTIALS, **overrides},
    )
    _assert_rejected(code, out.getvalue(), named)


def test_generate_does_not_echo_a_rejected_transaction_count(tmp_path: Path) -> None:
    config = json.loads(GENERATOR_CONFIG.read_text("utf-8"))
    config["transaction_count"] = MARKER
    path = tmp_path / "generator.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    out = io.StringIO()
    code = run_generate(
        generator_config=path,
        out_dir=tmp_path / "out",
        profile="demo",
        env_file=tmp_path / "none.env",
        out=out,
        env=dict(CREDENTIALS),
    )
    _assert_rejected(code, out.getvalue(), "transaction_count")


def test_producer_settings_are_rejected_without_their_value() -> None:
    with pytest.raises(PublishConfigError) as acks:
        build_producer_config(bootstrap_servers="h:1", acks=MARKER, retries=5, batch_size=1)
    assert MARKER not in str(acks.value) and "producer.acks" in str(acks.value)

    with pytest.raises(PublishConfigError) as retries:
        build_producer_config(bootstrap_servers="h:1", acks="all", retries=-7421, batch_size=1)
    assert "-7421" not in str(retries.value) and "producer.retries" in str(retries.value)


def test_value_refused_by_librdkafka_is_not_repeated() -> None:
    with pytest.raises(PublishConfigError) as refused:
        make_producer({"bootstrap.servers": "h:1", "batch.size": -7421})
    assert "-7421" not in str(refused.value), str(refused.value)
    assert "batch.size" in str(refused.value)


def test_unreadable_configured_probe_path_is_not_printed(tmp_path: Path) -> None:
    def boom(path: Path) -> int:
        raise FileNotFoundError(2, "No such file or directory", str(path))

    limits = _limits(f"/khong-co/{MARKER}")
    probe = resolve_disk_probe(limits, tmp_path, env={}, disk_free=boom, osrelease="")

    assert probe.free_bytes is None and probe.method == METHOD_CONFIG
    assert MARKER not in repr(probe), repr(probe)
    assert "limits.disk_probe_path" in (probe.error or "")


def _limits(disk_probe_path: str):  # type: ignore[no-untyped-def]
    from n06.config import load_config

    return load_config(
        profile="demo",
        env={**CREDENTIALS, "N06__LIMITS__DISK_PROBE_PATH": disk_probe_path},
    ).limits


def _doctor(tmp_path: Path, overrides: Mapping[str, str], **patches: object) -> tuple[int, str]:
    env_file = tmp_path / ".env"
    run_init_local(target_path=env_file, skip_buckets=True, out=io.StringIO())
    env = {**parse_dotenv_file(env_file), **overrides}
    docker = (CheckItem("docker.engine", "docker", True, 0, "giả", "giả"), "docker")
    services = [CheckItem("service.minio", "service", True, 0, "giả", "giả")]
    out = io.StringIO()
    with (
        patch("n06.cli.doctor.check_docker_engine", return_value=docker),
        patch("n06.cli.doctor.check_services", return_value=services),
    ):
        code = run_doctor(
            profile="test",
            env_file=env_file,
            env=env,
            resource_snapshot=patches.get("snapshot", SNAPSHOT),  # type: ignore[arg-type]
            disk_probe=patches.get("disk_probe"),  # type: ignore[arg-type]
            as_json=True,
            out=out,
        )
    return code, out.getvalue()


def test_doctor_does_not_print_a_rejected_bucket_name(tmp_path: Path) -> None:
    code, text = _doctor(tmp_path, {"N06__OBJECT_STORE__BUCKET": f"Bad_{MARKER}"})
    _assert_rejected(code, text, "object_store.bucket")


def test_masking_a_secret_never_changes_which_failure_wins(tmp_path: Path) -> None:
    """Secret trùng chữ trong `check_id` không được đổi mã thoát hay đổi tên hạng mục."""
    broken_docker = (CheckItem("docker.engine", "docker", False, 13, "hỏng", "hỏng"), None)
    unknown_disk = DiskProbe(None, None, METHOD_CONFIG, "không đọc được")
    env_file = tmp_path / ".env"
    run_init_local(target_path=env_file, skip_buckets=True, out=io.StringIO())
    env = {**parse_dotenv_file(env_file), "N06_EXTRA_TOKEN": "capacity"}
    out = io.StringIO()
    with patch("n06.cli.doctor.check_docker_engine", return_value=broken_docker):
        code = run_doctor(
            profile="test",
            env_file=env_file,
            env=env,
            resource_snapshot=SNAPSHOT,
            disk_probe=unknown_disk,
            as_json=True,
            out=out,
        )
    check_ids = [item["check_id"] for item in json.loads(out.getvalue())["checks"]]
    assert code == 13, out.getvalue()
    assert "capacity.limits" in check_ids and "docker.engine" in check_ids


@pytest.mark.parametrize("failing_call", ["version", "info"])
def test_docker_error_text_is_withheld_when_docker_host_is_set(
    monkeypatch: pytest.MonkeyPatch, failing_call: str
) -> None:
    """Docker trích lại `DOCKER_HOST` bị từ chối, kể cả một phần mật khẩu."""
    host = f"tcp://user:ab/{MARKER}@127.0.0.1:abc"
    monkeypatch.setenv("DOCKER_HOST", host)

    def fake_run(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        failed = failing_call in argv
        stderr = f'parse "{host}": invalid port ":{MARKER}" after host' if failed else ""
        return subprocess.CompletedProcess(argv, 1 if failed else 0, "27.0", stderr)

    with (
        patch("n06.cli.doctor.shutil.which", return_value="docker"),
        patch("n06.cli.doctor.subprocess.run", side_effect=fake_run),
    ):
        item, docker_bin = check_docker_engine()

    assert docker_bin is None and item.exit_code == 13
    assert MARKER not in item.summary + item.detail, item.detail
    assert "DOCKER_HOST" in item.detail


def test_docker_error_text_is_kept_when_docker_host_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DOCKER_HOST", raising=False)
    failed = subprocess.CompletedProcess([], 1, "", "permission denied on docker.sock")
    with (
        patch("n06.cli.doctor.shutil.which", return_value="docker"),
        patch("n06.cli.doctor.subprocess.run", return_value=failed),
    ):
        item, _ = check_docker_engine()
    assert "permission denied" in item.detail
