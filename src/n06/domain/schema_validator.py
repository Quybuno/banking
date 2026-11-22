"""Bộ nạp và kiểm tra JSON Schema Draft 2020-12 có bật format checks (TASK T10).

Tham chiếu: docs/spec.md Phần II mục 2, 3, 8.
Quy tắc:
- Đọc trực tiếp file schema từ `contracts/N06_INPUT_CONTRACT_V1/schemas/*.schema.json`;
  không sao chép nội dung schema vào `src/`.
- Hỗ trợ đầy đủ các keyword Draft 2020-12 dùng trong hợp đồng (`$schema`, `$id`, `$ref`,
  `type`, `additionalProperties`, `properties`, `required`, `const`, `enum`, `format`,
  `pattern`, `minimum`, `maximum`, `minLength`, `minItems`, `items`, `oneOf`, `allOf`,
  `if`, `then`) và fail-closed nếu gặp keyword lạ.
- Bật format checks nghiêm ngặt cho `uuid`, `date` (`YYYY-MM-DD`) và `date-time`
  (`YYYY-MM-DDTHH:mm:ssZ` chuẩn UTC không fractional seconds).
- Phân loại mọi lỗi schema sang đúng `ErrorCode` theo Phần II mục 8.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

from n06.config import REPO_ROOT
from n06.domain.errors import ErrorCode, ValidationIssue, ValidationResult

CONTRACT_ROOT = REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1"
DEFAULT_SCHEMAS_DIR = CONTRACT_ROOT / "schemas"
DRAFT_2020_12_META_URI = "https://json-schema.org/draft/2020-12/schema"

SUPPORTED_KEYWORDS = frozenset(
    {
        "$schema",
        "$id",
        "$ref",
        "type",
        "additionalProperties",
        "properties",
        "required",
        "const",
        "enum",
        "format",
        "pattern",
        "minimum",
        "maximum",
        "minLength",
        "minItems",
        "items",
        "oneOf",
        "allOf",
        "if",
        "then",
    }
)

ENTITY_TO_SCHEMA_FILE: dict[str, str] = {
    "account": "account.schema.json",
    "transaction": "transaction.schema.json",
    "ledger_entry": "ledger_entry.schema.json",
    "statement": "statement.schema.json",
    "external_statement": "statement.schema.json",
    "event_envelope": "event_envelope.schema.json",
    "statement_delivery": "statement_delivery.schema.json",
    "source_manifest": "source_manifest.schema.json",
    "control_report": "control_report.schema.json",
    "generator_config": "generator_config.schema.json",
}

AMOUNT_FIELD_NAMES = frozenset(
    {
        "amount_minor",
        "opening_balance_minor",
        "opening_balance_minor_per_account",
        "amount_min_minor",
        "amount_max_minor",
        "amount_minor_total",
    }
)

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_UTC_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _leaf_field_name(path: str) -> str:
    """Lấy tên trường lá cuối cùng từ đường dẫn dạng `$.payload.amount_minor`."""
    cleaned = path.rsplit(".", 1)[-1]
    if "[" in cleaned:
        cleaned = cleaned.split("[", 1)[0]
    return cleaned


def _check_type(value: Any, expected_type: str) -> bool:
    if expected_type == "object":
        return isinstance(value, dict)
    if expected_type == "array":
        return isinstance(value, list)
    if expected_type == "string":
        return isinstance(value, str)
    if expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected_type == "boolean":
        return isinstance(value, bool)
    if expected_type == "number":
        return (isinstance(value, int | float)) and not isinstance(value, bool)
    if expected_type == "null":
        return value is None
    raise RuntimeError(f"Kiểu JSON Schema không được hỗ trợ: {expected_type!r}")


def check_format(value: str, fmt: str) -> bool:
    """Kiểm tra format Draft 2020-12 cho `uuid`, `date`, `date-time` (chuẩn UTC `...Z`)."""
    if fmt == "uuid":
        try:
            parsed = uuid.UUID(value)
        except (ValueError, AttributeError):
            return False
        return str(parsed) == value.lower()

    if fmt == "date":
        if not _ISO_DATE_RE.fullmatch(value):
            return False
        try:
            date.fromisoformat(value)
            return True
        except ValueError:
            return False

    if fmt == "date-time":
        if not _UTC_DATETIME_RE.fullmatch(value):
            return False
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False

    raise RuntimeError(f"Format JSON Schema chưa được hỗ trợ: {fmt!r}")


class ContractSchemaRegistry:
    """Nạp và lưu bộ JSON Schema Draft 2020-12 trực tiếp từ thư mục `contracts/`."""

    def __init__(self, schemas_dir: Path | None = None) -> None:
        self.schemas_dir = schemas_dir if schemas_dir is not None else DEFAULT_SCHEMAS_DIR
        if not self.schemas_dir.is_dir():
            raise FileNotFoundError(
                f"Không tìm thấy thư mục schema hợp đồng tại: {self.schemas_dir}"
            )
        self._schemas: dict[str, dict[str, Any]] = {}
        for path in sorted(self.schemas_dir.glob("*.schema.json")):
            loaded = json.loads(path.read_text(encoding="utf-8"))
            meta = loaded.get("$schema")
            if meta != DRAFT_2020_12_META_URI:
                raise RuntimeError(
                    f"Schema {path.name} không khai báo Draft 2020-12 ($schema={meta!r})"
                )
            self._schemas[path.name] = loaded
            schema_id = loaded.get("$id")
            if isinstance(schema_id, str) and schema_id:
                self._schemas[schema_id] = loaded

    def resolve_schema_name(self, entity_or_schema: str) -> str:
        """Chuẩn hóa tên entity (ví dụ `transaction`) hoặc tên file (`transaction.schema.json`)."""
        if entity_or_schema in self._schemas:
            return entity_or_schema
        mapped = ENTITY_TO_SCHEMA_FILE.get(entity_or_schema)
        if mapped and mapped in self._schemas:
            return mapped
        with_suffix = f"{entity_or_schema}.schema.json"
        if with_suffix in self._schemas:
            return with_suffix
        raise KeyError(f"Không tìm thấy schema cho '{entity_or_schema}'")

    def get_schema(self, entity_or_schema: str) -> dict[str, Any]:
        """Lấy dict JSON Schema theo tên entity hoặc tên file."""
        name = self.resolve_schema_name(entity_or_schema)
        return self._schemas[name]

    def validate_instance(
        self,
        value: Any,
        entity_or_schema: str | Mapping[str, Any],
        *,
        path: str = "$",
    ) -> list[ValidationIssue]:
        """Kiểm tra `value` theo JSON Schema Draft 2020-12 kèm format checks."""
        if isinstance(entity_or_schema, str):
            schema: Mapping[str, Any] = self.get_schema(entity_or_schema)
        else:
            schema = entity_or_schema

        unknown = set(schema.keys()) - SUPPORTED_KEYWORDS
        if unknown:
            raise RuntimeError(f"Từ khóa JSON Schema chưa hỗ trợ tại {path}: {sorted(unknown)}")

        if "$ref" in schema:
            ref_target = str(schema["$ref"])
            return self.validate_instance(value, self.get_schema(ref_target), path=path)

        issues: list[ValidationIssue] = []
        leaf = _leaf_field_name(path)

        # 1. Nếu là object ở cấp record/payload, ưu tiên kiểm tra phiên bản schema trước
        if isinstance(value, dict):
            props = schema.get("properties", {})
            for ver_key in ("schema_version", "contract_version"):
                if ver_key in props and "const" in props[ver_key]:
                    expected_ver = props[ver_key]["const"]
                    if ver_key in value and value[ver_key] != expected_ver:
                        issues.append(
                            ValidationIssue(
                                code=ErrorCode.UNSUPPORTED_SCHEMA_VERSION,
                                field_path=f"{path}.{ver_key}",
                                message=(
                                    f"Phiên bản '{ver_key}'={value[ver_key]!r} không được hỗ trợ "
                                    f"(yêu cầu {expected_ver!r})."
                                ),
                            )
                        )

        # 2. Kiểm tra kiểu dữ liệu (`type`)
        expected_type = schema.get("type")
        if expected_type is not None and not _check_type(value, expected_type):
            issues.append(
                ValidationIssue(
                    code=ErrorCode.SCHEMA_TYPE,
                    field_path=path,
                    message=(
                        f"Sai kiểu dữ liệu tại '{path}': yêu cầu {expected_type}, nhận {value!r}."
                    ),
                )
            )
            return issues

        # 3. Kiểm tra hằng số (`const`)
        if "const" in schema and value != schema["const"]:
            if leaf in ("schema_version", "contract_version"):
                code = ErrorCode.UNSUPPORTED_SCHEMA_VERSION
            else:
                code = ErrorCode.SCHEMA_VALUE
            issues.append(
                ValidationIssue(
                    code=code,
                    field_path=path,
                    message=f"Giá trị tại '{path}' phải là {schema['const']!r}, nhận {value!r}.",
                )
            )

        # 4. Kiểm tra tập giá trị (`enum`)
        if "enum" in schema and value not in schema["enum"]:
            if leaf == "currency":
                code = ErrorCode.UNSUPPORTED_CURRENCY
            else:
                code = ErrorCode.SCHEMA_VALUE
            issues.append(
                ValidationIssue(
                    code=code,
                    field_path=path,
                    message=(
                        f"Giá trị tại '{path}' ({value!r}) không thuộc danh sách hợp lệ "
                        f"{schema['enum']}."
                    ),
                )
            )

        # 5. Kiểm tra các thuộc tính của `object`
        if isinstance(value, dict):
            props = schema.get("properties", {})
            for req_key in schema.get("required", []):
                if req_key not in value:
                    issues.append(
                        ValidationIssue(
                            code=ErrorCode.MISSING_REQUIRED_FIELD,
                            field_path=f"{path}.{req_key}",
                            message=f"Thiếu trường bắt buộc '{req_key}' tại '{path}'.",
                        )
                    )
            if schema.get("additionalProperties") is False:
                extra_keys = sorted(set(value.keys()) - set(props.keys()))
                for extra_k in extra_keys:
                    issues.append(
                        ValidationIssue(
                            code=ErrorCode.SCHEMA_VALUE,
                            field_path=f"{path}.{extra_k}",
                            message=(
                                f"Trường ngoài hợp đồng '{extra_k}' không được phép tại '{path}'."
                            ),
                        )
                    )
            for key in sorted(set(value.keys()) & set(props.keys())):
                child_path = f"{path}.{key}"
                issues.extend(self.validate_instance(value[key], props[key], path=child_path))

        # 6. Kiểm tra các phần tử của `array`
        if isinstance(value, list):
            min_items = schema.get("minItems")
            if min_items is not None and len(value) < min_items:
                issues.append(
                    ValidationIssue(
                        code=ErrorCode.SCHEMA_VALUE,
                        field_path=path,
                        message=(
                            f"Mảng tại '{path}' có {len(value)} phần tử, yêu cầu >= {min_items}."
                        ),
                    )
                )
            if "items" in schema:
                item_schema = schema["items"]
                for idx, item in enumerate(value):
                    issues.extend(self.validate_instance(item, item_schema, path=f"{path}[{idx}]"))

        # 7. Kiểm tra ràng buộc trên `string` (`minLength`, `pattern`, `format`)
        if isinstance(value, str):
            min_len = schema.get("minLength")
            if min_len is not None and len(value) < min_len:
                issues.append(
                    ValidationIssue(
                        code=ErrorCode.SCHEMA_VALUE,
                        field_path=path,
                        message=f"Chuỗi tại '{path}' ngắn hơn độ dài tối thiểu {min_len}.",
                    )
                )
            pattern = schema.get("pattern")
            if pattern is not None and not re.search(pattern, value):
                issues.append(
                    ValidationIssue(
                        code=ErrorCode.SCHEMA_VALUE,
                        field_path=path,
                        message=(
                            f"Giá trị tại '{path}' ({value!r}) không khớp mẫu regex {pattern!r}."
                        ),
                    )
                )
            fmt = schema.get("format")
            if fmt is not None and not check_format(value, fmt):
                issues.append(
                    ValidationIssue(
                        code=ErrorCode.SCHEMA_VALUE,
                        field_path=path,
                        message=f"Giá trị tại '{path}' ({value!r}) sai định dạng format={fmt!r}.",
                    )
                )

        # 8. Kiểm tra miền giá trị số nguyên (`minimum`, `maximum`)
        if isinstance(value, int) and not isinstance(value, bool):
            min_val = schema.get("minimum")
            max_val = schema.get("maximum")
            if min_val is not None and value < min_val:
                code = (
                    ErrorCode.AMOUNT_OUT_OF_RANGE
                    if leaf in AMOUNT_FIELD_NAMES
                    else ErrorCode.SCHEMA_VALUE
                )
                issues.append(
                    ValidationIssue(
                        code=code,
                        field_path=path,
                        message=f"Giá trị tại '{path}' ({value}) nhỏ hơn giới hạn dưới {min_val}.",
                    )
                )
            if max_val is not None and value > max_val:
                code = (
                    ErrorCode.AMOUNT_OUT_OF_RANGE
                    if leaf in AMOUNT_FIELD_NAMES
                    else ErrorCode.SCHEMA_VALUE
                )
                issues.append(
                    ValidationIssue(
                        code=code,
                        field_path=path,
                        message=f"Giá trị tại '{path}' ({value}) vượt quá giới hạn trên {max_val}.",
                    )
                )

        # 9. Kiểm tra tổ hợp điều kiện `allOf` và `if`/`then`
        has_conditional = False
        for sub_schema in schema.get("allOf", []):
            if "if" in sub_schema:
                has_conditional = True
            issues.extend(self.validate_instance(value, sub_schema, path=path))

        if "if" in schema:
            cond_issues = self.validate_instance(value, schema["if"], path=path)
            if not cond_issues and "then" in schema:
                issues.extend(self.validate_instance(value, schema["then"], path=path))

        # 10. Kiểm tra `oneOf` (nếu không bị chi phối bởi nhánh `allOf`/`if-then` đã bắt lỗi cụ thể)
        if "oneOf" in schema and not has_conditional:
            branch_results = [
                self.validate_instance(value, branch, path=path) for branch in schema["oneOf"]
            ]
            valid_branches = sum(1 for r in branch_results if not r)
            if valid_branches != 1:
                if valid_branches == 0 and branch_results:
                    # Gộp các lỗi từ nhánh gần nhất để giữ đúng phân loại mã lỗi chi tiết
                    best_branch = min(branch_results, key=len)
                    issues.extend(best_branch)
                else:
                    issues.append(
                        ValidationIssue(
                            code=ErrorCode.SCHEMA_VALUE,
                            field_path=path,
                            message=(
                                f"Giá trị tại '{path}' khớp {valid_branches} nhánh "
                                "trong oneOf (yêu cầu đúng 1)."
                            ),
                        )
                    )

        return issues


_DEFAULT_REGISTRY: ContractSchemaRegistry | None = None


def get_default_schema_registry() -> ContractSchemaRegistry:
    """Trả về singleton `ContractSchemaRegistry` mặc định đọc từ `contracts/`."""
    global _DEFAULT_REGISTRY
    if _DEFAULT_REGISTRY is None:
        _DEFAULT_REGISTRY = ContractSchemaRegistry()
    return _DEFAULT_REGISTRY


def parse_and_validate_json_schema(
    raw_or_dict: str | bytes | Mapping[str, Any],
    entity_or_schema: str,
    *,
    registry: ContractSchemaRegistry | None = None,
) -> ValidationResult:
    """Parse chuỗi JSON (hoặc nhận dict) và validate theo JSON Schema Draft 2020-12."""
    reg = registry if registry is not None else get_default_schema_registry()

    if isinstance(raw_or_dict, str | bytes):
        text = (
            raw_or_dict.decode("utf-8", errors="replace")
            if isinstance(raw_or_dict, bytes)
            else raw_or_dict
        )
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            return ValidationResult.from_issues(
                [
                    ValidationIssue(
                        code=ErrorCode.INVALID_JSON,
                        field_path="$",
                        message=f"Không parse được JSON: {exc}",
                    )
                ]
            )
        if not isinstance(parsed, dict):
            return ValidationResult.from_issues(
                [
                    ValidationIssue(
                        code=ErrorCode.SCHEMA_TYPE,
                        field_path="$",
                        message="Bản ghi JSON gốc phải là một JSON object.",
                    )
                ]
            )
        record_dict: dict[str, Any] = parsed
    elif isinstance(raw_or_dict, Mapping):
        record_dict = dict(raw_or_dict)
    else:
        return ValidationResult.from_issues(
            [
                ValidationIssue(
                    code=ErrorCode.SCHEMA_TYPE,
                    field_path="$",
                    message=(
                        "Đầu vào phải là JSON string hoặc Mapping, "
                        f"nhận {type(raw_or_dict).__name__}."
                    ),
                )
            ]
        )

    issues = reg.validate_instance(record_dict, entity_or_schema, path="$")
    return ValidationResult.from_issues(issues, record=record_dict)
