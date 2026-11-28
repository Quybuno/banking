"""Lấy mẫu CPU, RAM, swap và container theo chuỗi thời gian (ADR-0007 B2, B3, Q8, Q9)."""

from __future__ import annotations

import csv
import json
import os
import platform
import re
import shutil
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

from n06.config import AppConfig

_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]+)?\s*$")
_SIZE_FACTORS_MB: dict[str, float] = {
    "b": 1.0 / (1024.0 * 1024.0),
    "kib": 1.0 / 1024.0,
    "kb": 1000.0 / (1024.0 * 1024.0),
    "k": 1.0 / 1024.0,
    "mib": 1.0,
    "mb": 1000.0 * 1000.0 / (1024.0 * 1024.0),
    "m": 1.0,
    "gib": 1024.0,
    "gb": 1000.0 * 1000.0 * 1000.0 / (1024.0 * 1024.0),
    "g": 1024.0,
    "tib": 1024.0 * 1024.0,
    "tb": 1000.0 * 1000.0 * 1000.0 * 1000.0 / (1024.0 * 1024.0),
    "t": 1024.0 * 1024.0,
}


@dataclass(frozen=True)
class CpuTimes:
    """Bộ đếm thời gian CPU tổng và idle từ `/proc/stat`."""

    total: int
    idle: int


@dataclass(frozen=True)
class MemInfo:
    """Thông tin RAM và swap tính bằng MiB."""

    total_mb: float
    available_mb: float
    used_mb: float
    swap_total_mb: float
    swap_used_mb: float


@dataclass(frozen=True)
class ContainerStat:
    """Chỉ số một container từ `docker stats --no-stream --format '{{json .}}'`."""

    name: str
    cpu_pct: float
    mem_used_mb: float
    mem_limit_mb: float | None


@dataclass(frozen=True)
class ResourceSample:
    """Một điểm mẫu theo thời gian."""

    timestamp_utc: str
    elapsed_seconds: float
    host_cpu_pct: float
    host_ram_used_mb: float
    host_ram_available_mb: float
    host_swap_used_mb: float
    process_tree_rss_mb: float
    containers: dict[str, ContainerStat] = field(default_factory=dict)


@dataclass(frozen=True)
class SamplerSummary:
    """Tổng hợp peak/average từ chuỗi thời gian."""

    cpu_peak_pct: float
    cpu_avg_pct: float
    ram_peak_mb: float
    ram_avg_mb: float
    ram_scope: str
    swap_peak_mb: float
    ram_available_start_mb: float
    sample_count: int
    container_peaks: dict[str, dict[str, float]]


def parse_mem_size_to_mb(raw: str) -> float:
    """Chuyển chuỗi dung lượng (vd `309.1MiB`, `8.726GiB`, `4g`) sang MiB."""
    match = _SIZE_RE.match(raw)
    if not match:
        return 0.0
    value = float(match.group(1))
    unit = (match.group(2) or "b").lower()
    factor = _SIZE_FACTORS_MB.get(unit, 1.0)
    return value * factor


def parse_driver_memory_mb(raw: str) -> float:
    """Chuyển cấu hình `spark.driver_memory` (vd `4g`, `2048m`) sang MiB."""
    return parse_mem_size_to_mb(raw)


def parse_proc_stat(text: str) -> CpuTimes:
    """Đọc dòng `cpu ` đầu tiên của `/proc/stat`."""
    for line in text.splitlines():
        if line.startswith("cpu "):
            parts = line.split()[1:]
            values = [int(x) for x in parts]
            idle = values[3] + (values[4] if len(values) > 4 else 0)
            return CpuTimes(total=sum(values), idle=idle)
    raise ValueError("Không tìm thấy dòng cpu trong /proc/stat")


def compute_cpu_pct(prev: CpuTimes | None, curr: CpuTimes) -> float:
    """Tính phần trăm CPU sử dụng giữa hai lần đọc `/proc/stat`."""
    if prev is None:
        return 0.0
    total_delta = curr.total - prev.total
    idle_delta = curr.idle - prev.idle
    if total_delta <= 0:
        return 0.0
    busy = max(0, total_delta - idle_delta)
    return round(min(100.0, (busy / total_delta) * 100.0), 2)


def parse_proc_meminfo(text: str) -> MemInfo:
    """Đọc `/proc/meminfo` và quy đổi sang MiB."""
    kb: dict[str, int] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, rest = line.split(":", 1)
        tokens = rest.strip().split()
        if tokens and tokens[0].isdigit():
            kb[key] = int(tokens[0])
    total_mb = kb.get("MemTotal", 0) / 1024.0
    available_mb = kb.get("MemAvailable", kb.get("MemFree", 0)) / 1024.0
    used_mb = max(0.0, total_mb - available_mb)
    swap_total_mb = kb.get("SwapTotal", 0) / 1024.0
    swap_free_mb = kb.get("SwapFree", 0) / 1024.0
    swap_used_mb = max(0.0, swap_total_mb - swap_free_mb)
    return MemInfo(
        total_mb=round(total_mb, 2),
        available_mb=round(available_mb, 2),
        used_mb=round(used_mb, 2),
        swap_total_mb=round(swap_total_mb, 2),
        swap_used_mb=round(swap_used_mb, 2),
    )


def parse_docker_stats_lines(text: str) -> dict[str, ContainerStat]:
    """Parse đầu ra JSON lines của `docker stats --no-stream --format '{{json .}}'`."""
    result: dict[str, ContainerStat] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        name = str(obj.get("Name") or obj.get("Container") or "").strip()
        if not name:
            continue
        cpu_raw = str(obj.get("CPUPerc") or "0").rstrip("%").strip()
        try:
            cpu_pct = float(cpu_raw)
        except ValueError:
            cpu_pct = 0.0
        mem_usage = str(obj.get("MemUsage") or "0MiB / 0MiB")
        used_part, _, limit_part = mem_usage.partition("/")
        used_mb = parse_mem_size_to_mb(used_part)
        limit_mb = parse_mem_size_to_mb(limit_part) if limit_part.strip() else None
        result[name] = ContainerStat(
            name=name,
            cpu_pct=round(cpu_pct, 2),
            mem_used_mb=round(used_mb, 2),
            mem_limit_mb=round(limit_mb, 2) if limit_mb is not None and limit_mb > 0 else None,
        )
    return result


def default_host_stat_reader() -> str:
    return Path("/proc/stat").read_text(encoding="utf-8")


def default_host_mem_reader() -> str:
    return Path("/proc/meminfo").read_text(encoding="utf-8")


def default_container_stats_reader(docker_bin: str | None = None) -> dict[str, ContainerStat]:
    binary = docker_bin or shutil.which("docker")
    if binary is None:
        return {}
    proc = subprocess.run(
        [binary, "stats", "--no-stream", "--format", "{{json .}}"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if proc.returncode != 0:
        return {}
    return parse_docker_stats_lines(proc.stdout)


def default_process_tree_rss_reader(root_pid: int | None = None) -> float:
    """Tính tổng VmRSS (MiB) của tiến trình `root_pid` và mọi tiến trình con trong `/proc`."""
    target = root_pid if root_pid is not None else os.getpid()
    proc_dir = Path("/proc")
    if not proc_dir.is_dir():
        return 0.0
    children: dict[int, list[int]] = {}
    rss_kb: dict[int, int] = {}
    for entry in proc_dir.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            stat_text = (entry / "stat").read_text(encoding="utf-8")
            rparen = stat_text.rfind(")")
            if rparen == -1:
                continue
            fields = stat_text[rparen + 2 :].split()
            ppid = int(fields[1])
            children.setdefault(ppid, []).append(pid)
            status_text = (entry / "status").read_text(encoding="utf-8")
            for sline in status_text.splitlines():
                if sline.startswith("VmRSS:"):
                    parts = sline.split()
                    if len(parts) >= 2 and parts[1].isdigit():
                        rss_kb[pid] = int(parts[1])
                    break
        except OSError:
            continue
    stack = [target]
    visited: set[int] = set()
    total_kb = 0
    while stack:
        pid = stack.pop()
        if pid in visited:
            continue
        visited.add(pid)
        total_kb += rss_kb.get(pid, 0)
        stack.extend(children.get(pid, ()))
    return round(total_kb / 1024.0, 2)


class ResourceSampler:
    """Bộ lấy mẫu tài nguyên theo chuỗi thời gian (có thể tiêm đồng hồ và nguồn số giả)."""

    def __init__(
        self,
        *,
        interval_seconds: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        utc_clock: Callable[[], datetime] | None = None,
        host_stat_reader: Callable[[], str] = default_host_stat_reader,
        host_mem_reader: Callable[[], str] = default_host_mem_reader,
        container_stats_reader: Callable[[], dict[str, ContainerStat]] = (
            default_container_stats_reader
        ),
        process_rss_reader: Callable[[], float] = default_process_tree_rss_reader,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds phải > 0")
        self._interval = interval_seconds
        self._clock = clock
        self._utc_clock = utc_clock or (lambda: datetime.now(UTC))
        self._host_stat_reader = host_stat_reader
        self._host_mem_reader = host_mem_reader
        self._container_stats_reader = container_stats_reader
        self._process_rss_reader = process_rss_reader
        self._samples: list[ResourceSample] = []
        self._prev_cpu: CpuTimes | None = None
        self._start_time: float | None = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def take_sample(self) -> ResourceSample:
        """Lấy một mẫu tức thời và lưu vào chuỗi."""
        now_mono = self._clock()
        if self._start_time is None:
            self._start_time = now_mono
        elapsed = round(max(0.0, now_mono - self._start_time), 3)
        now_utc = self._utc_clock().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        cpu_times = parse_proc_stat(self._host_stat_reader())
        cpu_pct = compute_cpu_pct(self._prev_cpu, cpu_times)
        self._prev_cpu = cpu_times

        mem = parse_proc_meminfo(self._host_mem_reader())
        containers = self._container_stats_reader()
        proc_rss = self._process_rss_reader()

        sample = ResourceSample(
            timestamp_utc=now_utc,
            elapsed_seconds=elapsed,
            host_cpu_pct=cpu_pct,
            host_ram_used_mb=mem.used_mb,
            host_ram_available_mb=mem.available_mb,
            host_swap_used_mb=mem.swap_used_mb,
            process_tree_rss_mb=proc_rss,
            containers=containers,
        )
        with self._lock:
            self._samples.append(sample)
        return sample

    def start(self) -> None:
        """Bắt đầu lấy mẫu định kỳ trên luồng nền."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self.take_sample()

        def _loop() -> None:
            while not self._stop_event.wait(self._interval):
                try:
                    self.take_sample()
                except Exception:
                    continue

        self._thread = threading.Thread(target=_loop, name="n06-resource-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Dừng luồng nền và lấy thêm một mẫu cuối."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, self._interval * 2))
            self._thread = None
        try:
            self.take_sample()
        except Exception:
            pass

    def samples(self) -> list[ResourceSample]:
        with self._lock:
            return list(self._samples)

    def summarize(self, *, ram_scope: str = "host") -> SamplerSummary:
        """Tính peak/avg từ chuỗi mẫu đã thu."""
        items = self.samples()
        if not items:
            return SamplerSummary(
                cpu_peak_pct=0.0,
                cpu_avg_pct=0.0,
                ram_peak_mb=0.0,
                ram_avg_mb=0.0,
                ram_scope=ram_scope,
                swap_peak_mb=0.0,
                ram_available_start_mb=0.0,
                sample_count=0,
                container_peaks={},
            )

        cpus = [s.host_cpu_pct for s in items]
        if ram_scope == "process_tree":
            rams = [s.process_tree_rss_mb for s in items]
        else:
            rams = [s.host_ram_used_mb for s in items]
        swaps = [s.host_swap_used_mb for s in items]

        container_series: dict[str, list[ContainerStat]] = {}
        for s in items:
            for name, stat in s.containers.items():
                container_series.setdefault(name, []).append(stat)
        container_peaks: dict[str, dict[str, float]] = {}
        for name, stats in sorted(container_series.items()):
            container_peaks[name] = {
                "cpu_peak_pct": round(max(st.cpu_pct for st in stats), 2),
                "cpu_avg_pct": round(sum(st.cpu_pct for st in stats) / len(stats), 2),
                "mem_peak_mb": round(max(st.mem_used_mb for st in stats), 2),
                "mem_avg_mb": round(sum(st.mem_used_mb for st in stats) / len(stats), 2),
            }

        return SamplerSummary(
            cpu_peak_pct=round(max(cpus), 2),
            cpu_avg_pct=round(sum(cpus) / len(cpus), 2),
            ram_peak_mb=round(max(rams), 2),
            ram_avg_mb=round(sum(rams) / len(rams), 2),
            ram_scope=ram_scope,
            swap_peak_mb=round(max(swaps), 2),
            ram_available_start_mb=items[0].host_ram_available_mb,
            sample_count=len(items),
            container_peaks=container_peaks,
        )


def write_timeseries_csv(path: Path, samples: Sequence[ResourceSample]) -> None:
    """Ghi chuỗi thời gian ra `timeseries/<run_id>.csv`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    container_names = sorted({name for s in samples for name in s.containers})
    header = [
        "timestamp_utc",
        "elapsed_seconds",
        "host_cpu_pct",
        "host_ram_used_mb",
        "host_ram_available_mb",
        "host_swap_used_mb",
        "process_tree_rss_mb",
    ]
    for name in container_names:
        header.extend([f"{name}_cpu_pct", f"{name}_mem_used_mb"])

    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        for s in samples:
            row: list[Any] = [
                s.timestamp_utc,
                s.elapsed_seconds,
                s.host_cpu_pct,
                s.host_ram_used_mb,
                s.host_ram_available_mb,
                s.host_swap_used_mb,
                s.process_tree_rss_mb,
            ]
            for name in container_names:
                c = s.containers.get(name)
                if c is None:
                    row.extend(["", ""])
                else:
                    row.extend([c.cpu_pct, c.mem_used_mb])
            writer.writerow(row)


def _read_cpu_model() -> str:
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.is_file():
        for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    return platform.processor() or platform.machine()


def collect_git_state(repo_root: Path) -> tuple[str, bool]:
    """Trả về `(commit_sha_40_hex, git_dirty)` từ repo thật."""
    sha_proc = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    commit_sha = sha_proc.stdout.strip() if sha_proc.returncode == 0 else "unknown"
    status_proc = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    dirty = bool(status_proc.stdout.strip()) if status_proc.returncode == 0 else False
    return commit_sha, dirty


def collect_container_limits(
    docker_bin: str | None = None,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> tuple[dict[str, dict[str, Any]], dict[str, str], list[str]]:
    """Đọc giới hạn CPU/RAM và image của các container đang chạy qua `docker inspect`."""
    binary = docker_bin or shutil.which("docker")
    if binary is None:
        return {}, {}, []
    ps_proc = runner(
        [binary, "ps", "--format", "{{.Names}}"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if ps_proc.returncode != 0:
        return {}, {}, []
    names = [line.strip() for line in ps_proc.stdout.splitlines() if line.strip()]
    if not names:
        return {}, {}, []

    inspect_proc = runner(
        [binary, "inspect", *names],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if inspect_proc.returncode != 0:
        return {}, {}, names
    try:
        items = json.loads(inspect_proc.stdout)
    except json.JSONDecodeError:
        return {}, {}, names

    limits: dict[str, dict[str, Any]] = {}
    images: dict[str, str] = {}
    for item in items:
        cname = str(item.get("Name", "")).lstrip("/")
        host_cfg = item.get("HostConfig") or {}
        mem = int(host_cfg.get("Memory") or 0)
        nano_cpus = int(host_cfg.get("NanoCpus") or 0)
        cpu_quota = int(host_cfg.get("CpuQuota") or 0)
        cpu_period = int(host_cfg.get("CpuPeriod") or 0)
        cpus: str | float = "none"
        if nano_cpus > 0:
            cpus = round(nano_cpus / 1e9, 3)
        elif cpu_quota > 0 and cpu_period > 0:
            cpus = round(cpu_quota / cpu_period, 3)
        limits[cname] = {
            "cpus": cpus,
            "memory": round(mem / (1024 * 1024), 2) if mem > 0 else "none",
        }
        images[cname] = str((item.get("Config") or {}).get("Image") or "")
    return limits, images, names


def collect_environment_info(
    cfg: AppConfig,
    *,
    repo_root: Path,
    disk_free_bytes: int,
    docker_bin: str | None = None,
    host_mem_reader: Callable[[], str] = default_host_mem_reader,
) -> dict[str, Any]:
    """Thu thập toàn bộ thông số máy và phiên bản cho `environment.json` và `runs.csv`."""
    mem = parse_proc_meminfo(host_mem_reader())
    commit_sha, git_dirty = collect_git_state(repo_root)
    limits, images, running_containers = collect_container_limits(docker_bin=docker_bin)

    airflow_running = any("airflow" in name for name in running_containers)
    metabase_running = any("metabase" in name for name in running_containers)
    services_stopped = (
        not cfg.services.airflow_enabled
        and not cfg.services.metabase_enabled
        and not airflow_running
        and not metabase_running
    )

    try:
        n06_ver = metadata.version("n06")
    except metadata.PackageNotFoundError:
        n06_ver = "0.1.0"

    kafka_image = next((img for name, img in images.items() if "kafka" in name), "")
    minio_image = next((img for name, img in images.items() if "minio" in name), "")
    postgres_image = next((img for name, img in images.items() if "postgres" in name), "")

    return {
        "host_os": f"{platform.system()} {platform.release()}",
        "kernel": platform.version(),
        "cpu_model": _read_cpu_model(),
        "cpu_logical": os.cpu_count() or 1,
        "ram_total_mb": mem.total_mb,
        "ram_available_mb": mem.available_mb,
        "swap_total_mb": mem.swap_total_mb,
        "disk_free_bytes_start": disk_free_bytes,
        "container_limits": limits,
        "running_containers": running_containers,
        "services_stopped": services_stopped,
        "os_cache_dropped": False,
        "clock_source": "host_shared",
        "versions": {
            "n06_version": n06_ver,
            "python": platform.python_version(),
            "spark": "3.5.8",
            "hadoop": "3.3.4",
            "scala": "2.12.18",
            "java": "17",
            "kafka": kafka_image or "apache/kafka:3.7.2",
            "minio_image": minio_image or "minio/minio",
            "postgres_image": postgres_image or "postgres:16.6-bookworm",
            "duckdb": "",
        },
        "commit_sha": commit_sha,
        "git_dirty": git_dirty,
    }
