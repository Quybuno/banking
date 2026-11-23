"""External merge sort trên đĩa theo khóa `(event_time, ordinal)` (TASK T12)."""

from __future__ import annotations

import heapq
import json
import tempfile
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any

from n06.generator.determinism import canonical_json


def event_time_ordinal_key(record: dict[str, Any]) -> tuple[str, int]:
    """Khóa sắp xếp chuẩn theo `(event_time, ordinal)` (mục 5.2)."""
    return (str(record["event_time"]), int(record["ordinal"]))


def _write_sorted_run(
    path: Path,
    buffer: list[tuple[Any, dict[str, Any]]],
) -> None:
    buffer.sort(key=lambda item: item[0])
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        for _, record in buffer:
            fh.write(canonical_json(record))
            fh.write("\n")


def _iter_run_file(
    path: Path,
    key_fn: Callable[[dict[str, Any]], Any],
) -> Iterator[tuple[Any, dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            record = json.loads(stripped)
            yield (key_fn(record), record)


def external_sort_jsonl(
    records: Iterable[dict[str, Any]],
    *,
    key_fn: Callable[[dict[str, Any]], Any] = event_time_ordinal_key,
    chunk_rows: int = 10_000,
    max_open_files: int = 64,
) -> Iterator[dict[str, Any]]:
    """Sắp xếp luồng dict mà không nạp toàn bộ dữ liệu vào RAM.

    Chia thành các run file tạm kích thước tối đa ``chunk_rows``, sau đó thực hiện
    k-way merge nhiều lượt (tối đa ``max_open_files`` file mở cùng lúc).
    """
    if chunk_rows < 1:
        raise ValueError(f"chunk_rows phải >= 1, nhận {chunk_rows}")
    if max_open_files < 2:
        raise ValueError(f"max_open_files phải >= 2, nhận {max_open_files}")

    with tempfile.TemporaryDirectory(prefix="n06_extsort_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        run_files: list[Path] = []
        buffer: list[tuple[Any, dict[str, Any]]] = []
        run_index = 0

        for record in records:
            buffer.append((key_fn(record), record))
            if len(buffer) >= chunk_rows:
                run_path = tmp_dir / f"run_{run_index:06d}.jsonl"
                _write_sorted_run(run_path, buffer)
                run_files.append(run_path)
                buffer.clear()
                run_index += 1

        if buffer:
            if not run_files:
                # Chỉ có 1 chunk: trả trực tiếp không cần ghi/đọc thêm
                buffer.sort(key=lambda item: item[0])
                for _, rec in buffer:
                    yield rec
                return
            run_path = tmp_dir / f"run_{run_index:06d}.jsonl"
            _write_sorted_run(run_path, buffer)
            run_files.append(run_path)
            buffer.clear()

        if not run_files:
            return

        pass_index = 0
        while len(run_files) > max_open_files:
            next_runs: list[Path] = []
            for batch_start in range(0, len(run_files), max_open_files):
                batch_paths = run_files[batch_start : batch_start + max_open_files]
                merged_path = tmp_dir / f"merge_p{pass_index:03d}_{batch_start:06d}.jsonl"
                iterators = [_iter_run_file(p, key_fn) for p in batch_paths]
                with merged_path.open("w", encoding="utf-8", newline="\n") as out_fh:
                    for _, rec in heapq.merge(*iterators, key=lambda item: item[0]):
                        out_fh.write(canonical_json(rec))
                        out_fh.write("\n")
                for old_path in batch_paths:
                    old_path.unlink(missing_ok=True)
                next_runs.append(merged_path)
            run_files = next_runs
            pass_index += 1

        final_iterators = [_iter_run_file(p, key_fn) for p in run_files]
        for _, rec in heapq.merge(*final_iterators, key=lambda item: item[0]):
            yield rec
