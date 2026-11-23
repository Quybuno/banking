"""Deterministic primitives for N06 data generation (spec Part II, section 5.2)."""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

_UINT256_SPACE = 1 << 256


def _require_strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {type(value).__name__}")
    return value


def _require_non_empty_str(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty str")
    return value


def canonical_json(value: Any) -> str:
    """Serialize ``value`` to deterministic UTF-8 canonical JSON.

    Rules (spec Part II, section 5.2):
    - Sorted keys
    - Compact separators ``(',', ':')``
    - ``ensure_ascii=False``
    - No ``NaN`` / ``Infinity`` values (``allow_nan=False``)
    """
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def draw(
    contract_version: str,
    seed: int,
    label: str,
    ordinal: int,
    low: int,
    high: int,
) -> int:
    """Deterministically draw an integer in ``[low, high]`` via SHA-256 rejection sampling."""
    _require_non_empty_str(contract_version, "contract_version")
    _require_strict_int(seed, "seed")
    _require_non_empty_str(label, "label")
    _require_strict_int(ordinal, "ordinal")
    _require_strict_int(low, "low")
    _require_strict_int(high, "high")

    if ordinal < 0:
        raise ValueError(f"ordinal must be >= 0, got {ordinal}")
    if low > high:
        raise ValueError(f"low ({low}) must be <= high ({high})")

    width = high - low + 1
    limit = _UINT256_SPACE - (_UINT256_SPACE % width)

    retry_counter = 0
    while True:
        payload = canonical_json([contract_version, seed, label, ordinal, retry_counter])
        digest = hashlib.sha256(payload.encode("utf-8")).digest()
        number = int.from_bytes(digest, "big")
        if number < limit:
            return low + (number % width)
        retry_counter += 1


def deterministic_uuid(
    seed_namespace: str | uuid.UUID,
    contract_version: str,
    dataset_id: str,
    seed: int,
    entity: str,
    ordinal: int,
    discriminator: str,
) -> str:
    """Generate a deterministic UUIDv5 string from contract identity components."""
    namespace = (
        seed_namespace if isinstance(seed_namespace, uuid.UUID) else uuid.UUID(seed_namespace)
    )
    _require_non_empty_str(contract_version, "contract_version")
    _require_non_empty_str(dataset_id, "dataset_id")
    _require_strict_int(seed, "seed")
    _require_non_empty_str(entity, "entity")
    _require_strict_int(ordinal, "ordinal")
    _require_non_empty_str(discriminator, "discriminator")

    if ordinal < 0:
        raise ValueError(f"ordinal must be >= 0, got {ordinal}")

    name = canonical_json([contract_version, dataset_id, seed, entity, ordinal, discriminator])
    return str(uuid.uuid5(namespace, name))


def external_reference_from_uuid(transaction_id: str | uuid.UUID) -> str:
    """Format ``REF-<HEX>`` from a transaction UUID without hyphens in uppercase."""
    parsed = (
        transaction_id if isinstance(transaction_id, uuid.UUID) else uuid.UUID(str(transaction_id))
    )
    return f"REF-{parsed.hex.upper()}"


def deterministic_external_reference(
    seed_namespace: str | uuid.UUID,
    contract_version: str,
    dataset_id: str,
    seed: int,
    transaction_ordinal: int,
    discriminator: str = "original",
) -> str:
    """Compute the deterministic ``external_reference`` for a transaction ordinal."""
    tx_uuid = deterministic_uuid(
        seed_namespace=seed_namespace,
        contract_version=contract_version,
        dataset_id=dataset_id,
        seed=seed,
        entity="transaction",
        ordinal=transaction_ordinal,
        discriminator=discriminator,
    )
    return external_reference_from_uuid(tx_uuid)
