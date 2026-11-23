"""Unit tests for deterministic generator primitives (T11)."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from n06.generator.determinism import (
    canonical_json,
    deterministic_external_reference,
    deterministic_uuid,
    draw,
    external_reference_from_uuid,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
GOLDEN_VECTORS_PATH = (
    REPO_ROOT
    / "contracts"
    / "N06_INPUT_CONTRACT_V1"
    / "fixtures"
    / "golden"
    / "deterministic_vectors.json"
)


def _load_golden() -> dict[str, Any]:
    return json.loads(GOLDEN_VECTORS_PATH.read_text(encoding="utf-8"))


_GOLDEN = _load_golden()
_DRAW_CASES = [
    pytest.param(
        case,
        id=f"{case['label']}-ord{case['ordinal']}-[{case['low']},{case['high']}]",
    )
    for case in _GOLDEN["draw"]
]
_UUID_CASES = [
    pytest.param(
        case,
        id=f"{case['entity']}-ord{case['ordinal']}-{case['discriminator']}",
    )
    for case in _GOLDEN["uuid"]
]
_EXT_REF_CASES = [
    pytest.param(
        case,
        id=f"tx-ord{case['transaction_ordinal']}",
    )
    for case in _GOLDEN["external_reference"]
]


@pytest.mark.parametrize("case", _DRAW_CASES)
def test_golden_draw_vector(case: dict[str, Any]) -> None:
    actual = draw(
        contract_version=_GOLDEN["contract_version"],
        seed=_GOLDEN["seed"],
        label=case["label"],
        ordinal=case["ordinal"],
        low=case["low"],
        high=case["high"],
    )
    assert actual == case["value"]


@pytest.mark.parametrize("case", _UUID_CASES)
def test_golden_uuid_vector(case: dict[str, Any]) -> None:
    actual = deterministic_uuid(
        seed_namespace=_GOLDEN["seed_namespace"],
        contract_version=_GOLDEN["contract_version"],
        dataset_id=_GOLDEN["dataset_id"],
        seed=_GOLDEN["seed"],
        entity=case["entity"],
        ordinal=case["ordinal"],
        discriminator=case["discriminator"],
    )
    assert actual == case["value"]


@pytest.mark.parametrize("case", _EXT_REF_CASES)
def test_golden_external_reference_vector(case: dict[str, Any]) -> None:
    actual = deterministic_external_reference(
        seed_namespace=_GOLDEN["seed_namespace"],
        contract_version=_GOLDEN["contract_version"],
        dataset_id=_GOLDEN["dataset_id"],
        seed=_GOLDEN["seed"],
        transaction_ordinal=case["transaction_ordinal"],
    )
    assert actual == case["value"]


def test_external_reference_from_uuid_formatting() -> None:
    assert (
        external_reference_from_uuid("137d7b44-1903-5184-96da-3cfa3b667980")
        == "REF-137D7B441903518496DA3CFA3B667980"
    )


def test_canonical_json_rules() -> None:
    payload = {"z": 1, "a": {"d": "Tiếng Việt", "b": [2, 1]}}
    assert canonical_json(payload) == '{"a":{"b":[2,1],"d":"Tiếng Việt"},"z":1}'

    with pytest.raises(ValueError):
        canonical_json({"bad": float("nan")})
    with pytest.raises(ValueError):
        canonical_json({"bad": float("inf")})


def test_draw_rejection_sampling_retry_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify retry_counter advances when SHA-256 output >= limit."""
    calls: list[bytes] = []

    class _FakeDigest:
        def __init__(self, digest_bytes: bytes) -> None:
            self._digest_bytes = digest_bytes

        def digest(self) -> bytes:
            return self._digest_bytes

    def _fake_sha256(data: bytes) -> _FakeDigest:
        calls.append(data)
        if len(calls) == 1:
            # All 0xFF bits -> 2^256 - 1, guaranteed >= limit whenever 2^256 % width != 0.
            return _FakeDigest(b"\xff" * 32)
        # Second attempt -> value 5 in big-endian.
        return _FakeDigest((5).to_bytes(32, "big"))

    monkeypatch.setattr(hashlib, "sha256", _fake_sha256)

    # width = 3 (not a power of 2, so 2^256 % 3 == 1 and limit = 2^256 - 1)
    result = draw("1.0.0", 42, "retry-test", 0, 10, 12)
    assert result == 12  # 10 + (5 % 3) = 12
    assert len(calls) == 2
    assert json.loads(calls[0].decode("utf-8"))[-1] == 0
    assert json.loads(calls[1].decode("utf-8"))[-1] == 1


def test_draw_and_uuid_input_validation() -> None:
    with pytest.raises(ValueError):
        draw("1.0.0", 42, "amount", 0, 100, 10)
    with pytest.raises(ValueError):
        draw("1.0.0", 42, "amount", -1, 0, 10)
    with pytest.raises(TypeError):
        draw("1.0.0", True, "amount", 0, 0, 10)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        deterministic_uuid(_GOLDEN["seed_namespace"], "1.0.0", "ds", 42, "account", -1, "original")


def test_determinism_across_repeated_and_subprocess_runs() -> None:
    first = [draw("1.0.0", 42, "amount", i, 100, 100000) for i in range(5)]
    second = [draw("1.0.0", 42, "amount", i, 100, 100000) for i in range(5)]
    assert first == second

    script = (
        "import json; "
        "from n06.generator.determinism import draw; "
        "print(json.dumps([draw('1.0.0', 42, 'amount', i, 100, 100000) for i in range(5)]))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(proc.stdout) == first
