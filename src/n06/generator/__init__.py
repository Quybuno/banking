"""Synthetic banking dataset and fault-injection generator for N06."""

from n06.generator.determinism import (
    canonical_json,
    deterministic_external_reference,
    deterministic_uuid,
    draw,
    external_reference_from_uuid,
)
from n06.generator.external_sort import event_time_ordinal_key, external_sort_jsonl
from n06.generator.scenario import (
    ScenarioConfigError,
    ScenarioSummary,
    ScenarioValidationError,
    compute_event_timestamp,
    generate_base_scenario,
    select_account_pair,
    select_channel,
)

__all__ = [
    "ScenarioConfigError",
    "ScenarioSummary",
    "ScenarioValidationError",
    "canonical_json",
    "compute_event_timestamp",
    "deterministic_external_reference",
    "deterministic_uuid",
    "draw",
    "event_time_ordinal_key",
    "external_reference_from_uuid",
    "external_sort_jsonl",
    "generate_base_scenario",
    "select_account_pair",
    "select_channel",
]
