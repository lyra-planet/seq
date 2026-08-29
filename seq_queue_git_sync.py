#!/usr/bin/env python3
"""Commit and push shared_task_queue.json after queue task completions."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def git(repo: Path, *args: str, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.setdefault("GIT_AUTHOR_NAME", "seq-queue-sync")
    env.setdefault("GIT_AUTHOR_EMAIL", "seq-queue-sync@localhost")
    env.setdefault("GIT_COMMITTER_NAME", "seq-queue-sync")
    env.setdefault("GIT_COMMITTER_EMAIL", "seq-queue-sync@localhost")
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=check,
        timeout=timeout,
    )


def sync(repo: Path, queue: Path, remote: str, branch: str, message: str) -> bool:
    lock_path = repo / ".seq_git_sync.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        git(repo, "add", "--", queue.name)
        staged = git(repo, "diff", "--cached", "--quiet", check=False).returncode != 0
        if staged:
            result = git(repo, "commit", "-m", message, check=False)
            if result.returncode != 0:
                print(result.stdout.rstrip(), file=sys.stderr, flush=True)
                return False
            print(result.stdout.rstrip(), flush=True)
        pushed = git(repo, "push", remote, f"HEAD:{branch}", check=False)
        if pushed.returncode != 0:
            print(pushed.stdout.rstrip(), file=sys.stderr, flush=True)
            return False
        print(f"synced {queue.name} to {remote}/{branch}", flush=True)
        return True


def read_events(path: Path, offset: int) -> tuple[int, list[dict[str, object]]]:
    if not path.exists():
        return offset, []
    size = path.stat().st_size
    if size < offset:
        offset = 0
    events: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        handle.seek(offset)
        for line in handle:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                events.append(value)
        offset = handle.tell()
    return offset, events


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--queue", type=Path, default=Path("shared_task_queue.json"))
    parser.add_argument("--events", type=Path)
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--watch", action="store_true", help="keep watching until interrupted")
    args = parser.parse_args()
    repo = args.repo.resolve()
    queue = args.queue.resolve()
    events = (args.events or queue.with_name(f"{queue.name}.events.jsonl")).resolve()
    if not (repo / ".git").is_dir():
        parser.error(f"not a git repository: {repo}")
    if not queue.is_file():
        parser.error(f"queue does not exist: {queue}")
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")

    sync(repo, queue, args.remote, args.branch, "queue: initial snapshot")
    if not args.watch:
        return 0

    # Start at the beginning so a completion written during the initial sync
    # cannot be skipped by an offset initialized after that sync.
    offset = 0
    try:
        while True:
            offset, new_events = read_events(events, offset)
            for event in new_events:
                event_name = event.get("event")
                if event_name == "task_completed":
                    task_id = event.get("task_id", "unknown")
                    sync(repo, queue, args.remote, args.branch, f"queue: complete task_{task_id}")
                elif event_name == "task_failed" and event.get("terminal"):
                    task_id = event.get("task_id", "unknown")
                    sync(repo, queue, args.remote, args.branch, f"queue: terminal failure task_{task_id}")
            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        print("queue git sync stopped", flush=True)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
