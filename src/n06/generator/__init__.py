"""Synthetic banking dataset and fault-injection generator for N06."""

from n06.generator.control_report import (
    ControlTotals,
    build_control_report_from_envelopes,
    write_control_report,
)
from n06.generator.dataset import DatasetSummary, OutputValidationError, build_dataset
from n06.generator.determinism import (
    canonical_json,
    deterministic_external_reference,
    deterministic_uuid,
    draw,
    external_reference_from_uuid,
)
from n06.generator.events import build_event_envelopes
from n06.generator.external_sort import event_time_ordinal_key, external_sort_jsonl
from n06.generator.fault import (
    FAULT_ORDER,
    FaultPlan,
    compute_fault_counts,
    fault_rank_key,
)
from n06.generator.params import FaultConfigError
from n06.generator.scenario import (
    ScenarioConfigError,
    ScenarioSummary,
    ScenarioValidationError,
    compute_event_timestamp,
    generate_base_scenario,
    select_account_pair,
    select_channel,
)
from n06.generator.statements import (
    build_statement_rows,
    deadline_d,
    format_utc,
    statement_window,
)

__all__ = [
    "FAULT_ORDER",
    "ControlTotals",
    "DatasetSummary",
    "FaultConfigError",
    "FaultPlan",
    "OutputValidationError",
    "build_control_report_from_envelopes",
    "build_dataset",
    "build_event_envelopes",
    "build_statement_rows",
    "compute_fault_counts",
    "deadline_d",
    "fault_rank_key",
    "format_utc",
    "statement_window",
    "write_control_report",
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
