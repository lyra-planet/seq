#!/usr/bin/env python3
"""Watch the four reverse H3 workers and power off on the requested conditions.

The monitor is deliberately separate from the workers so a worker restart or
an exception cannot bypass the shutdown policy.  It writes a durable log and
only requests shutdown once per machine boot.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class Monitor:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.failures = {port: 0 for port in args.ports}
        self.shutdown_requested = False
        self.log = args.log.open("a", encoding="utf-8", buffering=1)

    def write(self, message: str) -> None:
        line = f"{now()} {message}"
        print(line, flush=True)
        self.log.write(line + "\n")
        self.log.flush()
        os.fsync(self.log.fileno())

    def request_shutdown(self, reason: str) -> None:
        if self.shutdown_requested:
            return
        self.shutdown_requested = True
        self.write(f"shutdown condition met: {reason}")
        self.write("executing /usr/bin/shutdown -h now")
        try:
            result = subprocess.run(["/usr/bin/shutdown", "-h", "now"], check=False, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            self.write(f"shutdown command exited with code {result.returncode}: {result.stdout.strip()}")
        except Exception as error:
            self.write(f"shutdown command failed: {error!r}")
        raise SystemExit(0)

    def queue_state(self) -> dict:
        try:
            return json.loads(self.args.queue.read_text(encoding="utf-8"))
        except Exception as error:
            self.request_shutdown(f"queue unreadable: {error!r}")
            raise AssertionError("unreachable")

    def check_queue(self, state: dict) -> None:
        tasks = state.get("tasks", {})
        completed = sum(1 for record in tasks.values() if record.get("status") == "completed")
        running = sum(1 for record in tasks.values() if record.get("status") == "running")
        pending = sum(1 for record in tasks.values() if record.get("status", "pending") == "pending")
        self.write(f"queue completed={completed} running={running} pending={pending}")
        stop_record = tasks.get(str(self.args.stop_task), {})
        stop_worker = str(stop_record.get("worker_id", ""))
        if (stop_record.get("status") == "completed"
                and any(stop_worker.startswith(prefix)
                        for prefix in self.args.reverse_worker_prefix)):
            self.request_shutdown(
                f"local reverse queue completed stop task {self.args.stop_task} ({stop_worker})"
            )
        if tasks and all(record.get("status") == "completed" for record in tasks.values()):
            self.request_shutdown("all queue tasks completed")
        for task_id, record in tasks.items():
            if record.get("status") != "running":
                continue
            pid = record.get("pid")
            if not pid or not Path(f"/proc/{pid}").exists():
                self.request_shutdown(f"running task {task_id} lost worker pid {pid}")

    def check_services(self) -> None:
        for port in self.args.ports:
            healthy = False
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/system_stats", timeout=self.args.http_timeout) as response:
                    healthy = 200 <= response.status < 300
            except (OSError, urllib.error.URLError, TimeoutError):
                healthy = False
            if healthy:
                if self.failures[port]:
                    self.write(f"ComfyUI {port} recovered")
                self.failures[port] = 0
            else:
                self.failures[port] += 1
                self.write(f"ComfyUI {port} health check failed ({self.failures[port]}/{self.args.max_failures})")
                if self.failures[port] >= self.args.max_failures:
                    self.request_shutdown(f"ComfyUI service {port} unavailable")

    def check_worker_logs(self) -> None:
        for path in self.args.worker_log:
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError as error:
                self.request_shutdown(f"worker log unreadable {path}: {error!r}")
            # Any worker-level failure is treated as an inference failure.  A
            # retry can otherwise leave the machine running indefinitely.
            if "failed and returned to pending:" in text:
                self.request_shutdown(f"worker reported inference failure: {path}")

    def run(self) -> None:
        self.write(f"monitor started; stop_task={self.args.stop_task}, ports={self.args.ports}")
        while True:
            state = self.queue_state()
            self.check_queue(state)
            self.check_services()
            self.check_worker_logs()
            time.sleep(self.args.poll_seconds)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=Path, required=True)
    parser.add_argument("--stop-task", type=int, default=180)
    parser.add_argument("--ports", type=int, nargs="+", default=[8191, 8192, 8193, 8194])
    parser.add_argument("--worker-log", type=Path, nargs="+", default=[Path(f"/root/seq_reverse_gpu{i}.log") for i in range(4)])
    parser.add_argument("--reverse-worker-prefix", action="append", default=["seq_reverse_gpu"],
                        help="worker-id prefix eligible to trigger the stop-task shutdown")
    parser.add_argument("--log", type=Path, default=Path("/root/seq_auto_shutdown.log"))
    parser.add_argument("--poll-seconds", type=float, default=15)
    parser.add_argument("--http-timeout", type=float, default=5)
    parser.add_argument("--max-failures", type=int, default=3)
    args = parser.parse_args()
    args.log.parent.mkdir(parents=True, exist_ok=True)
    if args.poll_seconds <= 0 or args.http_timeout <= 0 or args.max_failures <= 0:
        parser.error("poll and timeout values must be positive")
    Monitor(args).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
