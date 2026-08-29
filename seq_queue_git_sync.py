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
from datetime import datetime, timezone
from typing import Any
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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def queue_lock(queue: Path):
    lock = queue.with_name(f"{queue.name}.lock").open("a+")
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
    return lock


def merge_queue_states(local: dict[str, Any], remote: dict[str, Any]) -> dict[str, Any]:
    """Merge task states from two queue snapshots without losing completions."""
    if local.get("kind") != remote.get("kind"):
        raise ValueError("queue kind mismatch while merging remote state")
    if local.get("task_order") != remote.get("task_order"):
        raise ValueError("task order mismatch while merging remote state")
    merged = dict(local)
    local_tasks = local.get("tasks", {})
    remote_tasks = remote.get("tasks", {})
    merged_tasks: dict[str, Any] = {}
    rank = {"failed": 1, "pending": 2, "running": 3, "completed": 4}
    for task_id in local["task_order"]:
        left = dict(local_tasks[task_id])
        right = dict(remote_tasks[task_id])
        left_status = str(left.get("status", "pending"))
        right_status = str(right.get("status", "pending"))
        if left_status == right_status == "completed":
            left_time = str(left.get("completed_at", ""))
            right_time = str(right.get("completed_at", ""))
            chosen = left if left_time >= right_time else right
        elif rank.get(right_status, 0) > rank.get(left_status, 0):
            chosen = right
        else:
            chosen = left
        # Attempts are cumulative even when the snapshots were made by
        # different workers.  This prevents a retry count from going backwards.
        chosen["attempts"] = max(int(left.get("attempts", 0) or 0), int(right.get("attempts", 0) or 0))
        merged_tasks[task_id] = chosen
    merged["tasks"] = merged_tasks
    merged["updated_at"] = utc_now()
    return merged


def merge_remote_queue(repo: Path, queue: Path, remote: str, branch: str) -> bool:
    fetched = git(repo, "fetch", remote, branch, check=False)
    if fetched.returncode != 0:
        print(fetched.stdout.rstrip(), file=sys.stderr, flush=True)
        return False
    shown = git(repo, "show", f"{remote}/{branch}:{queue.name}", check=False)
    if shown.returncode != 0:
        print(shown.stdout.rstrip(), file=sys.stderr, flush=True)
        return False
    lock = queue_lock(queue)
    try:
        remote_state = json.loads(shown.stdout)
        # Keep the local snapshot before aligning Git history.  A fast-forward
        # changes the worktree to the remote tree, but the local snapshot may
        # contain a completion that must still be preserved in the merge.
        local_state = json.loads(queue.read_text(encoding="utf-8"))
        remote_ref = f"{remote}/{branch}"
        local_ancestor = git(repo, "merge-base", "--is-ancestor", "HEAD", remote_ref, check=False).returncode == 0
        remote_ancestor = git(repo, "merge-base", "--is-ancestor", remote_ref, "HEAD", check=False).returncode == 0
        if local_ancestor and not remote_ancestor:
            aligned = git(repo, "merge", "--ff-only", remote_ref, check=False)
        elif not local_ancestor and not remote_ancestor:
            aligned = git(repo, "merge", "--no-ff", "-s", "ours", "-m", "queue: merge remote history", remote_ref, check=False)
        else:
            aligned = subprocess.CompletedProcess([], 0, "", "")
        if aligned.returncode != 0:
            print(aligned.stdout.rstrip(), file=sys.stderr, flush=True)
            return False
        current_state = json.loads(queue.read_text(encoding="utf-8"))
        merged = merge_queue_states(local_state, remote_state)
        if merged == current_state:
            return True
        merged = merge_queue_states(local_state, remote_state)
        temporary = queue.with_name(f".{queue.name}.merge.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(merged, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(queue)
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        print(f"cannot merge queue state: {error}", file=sys.stderr, flush=True)
        return False
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()
    return True


def stage_commit_snapshot(repo: Path, queue: Path, message: str) -> bool:
    """Commit a queue snapshot while excluding concurrent worker writes."""
    lock = queue_lock(queue)
    try:
        git(repo, "add", "--", queue.name)
        staged = git(repo, "diff", "--cached", "--quiet", check=False).returncode != 0
        if not staged:
            return True
        result = git(repo, "commit", "-m", message, check=False)
        if result.returncode != 0:
            print(result.stdout.rstrip(), file=sys.stderr, flush=True)
            return False
        print(result.stdout.rstrip(), flush=True)
        return True
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def sync(repo: Path, queue: Path, remote: str, branch: str, message: str) -> bool:
    lock_path = repo / ".seq_git_sync.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        for attempt in range(1, 4):
            if not stage_commit_snapshot(repo, queue, message):
                return False
            pushed = git(repo, "push", remote, f"HEAD:{branch}", check=False)
            if pushed.returncode == 0:
                print(f"synced {queue.name} to {remote}/{branch}", flush=True)
                return True
            print(pushed.stdout.rstrip(), file=sys.stderr, flush=True)
            if attempt == 3 or not merge_remote_queue(repo, queue, remote, branch):
                return False
            message = "queue: merge concurrent worker state"
        return False


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

    retry_push = not sync(repo, queue, args.remote, args.branch, "queue: initial snapshot")
    if not args.watch:
        return 0

    # Start at the beginning so a completion written during the initial sync
    # cannot be skipped by an offset initialized after that sync.
    offset = 0
    try:
        while True:
            offset, new_events = read_events(events, offset)
            completed_event = False
            for event in new_events:
                event_name = event.get("event")
                if event_name == "task_completed":
                    task_id = event.get("task_id", "unknown")
                    retry_push = not sync(repo, queue, args.remote, args.branch, f"queue: complete task_{task_id}")
                    completed_event = True
                elif event_name == "task_failed" and event.get("terminal"):
                    task_id = event.get("task_id", "unknown")
                    retry_push = not sync(repo, queue, args.remote, args.branch, f"queue: terminal failure task_{task_id}")
            if retry_push and not completed_event:
                retry_push = not sync(repo, queue, args.remote, args.branch, "queue: retry pending push")
            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        print("queue git sync stopped", flush=True)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
