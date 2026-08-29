#!/usr/bin/env python3
"""Reverse MiniMax-H3 Ref2VA worker for the shared seq queue.

The worker intentionally keeps the queue protocol small and compatible with
``seq_queue_git_sync.py``.  It prepares each source video as a 1344x768,
107-frame, 24-fps letterboxed input, submits the known-good ComfyUI Ref2VA
graph, and records the cropped result plus its geometry sidecar.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


WIDTH = 1344
HEIGHT = 768
FRAMES = 107
FPS = 24
CRF = 18


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def run(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT).strip()


def ffprobe(source: Path) -> tuple[int, int, float]:
    value = run([
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height:format=duration",
        "-of", "json", str(source),
    ])
    data = json.loads(value)
    stream = (data.get("streams") or [{}])[0]
    width, height = int(stream["width"]), int(stream["height"])
    duration = float((data.get("format") or {}).get("duration") or 0)
    if width <= 0 or height <= 0 or duration <= 0:
        raise ValueError(f"invalid video metadata for {source}: {value}")
    return width, height, duration


def floor_even(value: float) -> int:
    return max(2, int(value) // 2 * 2)


def geometry(source: Path) -> dict[str, Any]:
    source_width, source_height, duration = ffprobe(source)
    scale = min(WIDTH / source_width, HEIGHT / source_height)
    content_width = floor_even(source_width * scale)
    content_height = floor_even(source_height * scale)
    offset_x = (WIDTH - content_width) // 2
    offset_y = (HEIGHT - content_height) // 2
    return {
        "source": str(source),
        "source_width": source_width,
        "source_height": source_height,
        "source_duration_seconds": duration,
        "scale": scale,
        "content_width": content_width,
        "content_height": content_height,
        "offset_x": offset_x,
        "offset_y": offset_y,
        "canvas_width": WIDTH,
        "canvas_height": HEIGHT,
        "frames": FRAMES,
        "fps": FPS,
        "sampling": "uniform over the complete decoded source duration",
        "resize_filter": "lanczos",
        "pad_color": "black",
    }


def preprocess(source: Path, target: Path, sidecar: Path) -> dict[str, Any]:
    meta = geometry(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        vf = (
            f"scale={meta['content_width']}:{meta['content_height']}:flags=lanczos,"
            f"pad={WIDTH}:{HEIGHT}:{meta['offset_x']}:{meta['offset_y']}:color=black,"
            f"fps={FRAMES}/{meta['source_duration_seconds']:.9f},"
            f"trim=end_frame={FRAMES},tpad=stop_mode=clone:stop={FRAMES},"
            "setpts=N/24/TB"
        )
        tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp.mp4")
        try:
            subprocess.run([
                "ffmpeg", "-nostdin", "-y", "-v", "error", "-i", str(source),
                "-map", "0:v:0", "-vf", vf, "-frames:v", str(FRAMES), "-r", str(FPS),
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", str(CRF),
                "-pix_fmt", "yuv420p", "-an", str(tmp),
            ], check=True)
            tmp.replace(target)
        finally:
            tmp.unlink(missing_ok=True)
    if not sidecar.exists():
        sidecar.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return meta


def lock_queue(queue: Path):
    lock = queue.with_name(f"{queue.name}.lock").open("a+")
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
    return lock


def write_queue(queue: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = now()
    tmp = queue.with_name(f".{queue.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(queue)


def append_event(queue: Path, event: dict[str, Any]) -> None:
    path = queue.with_name(f"{queue.name}.events.jsonl")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"at": now(), **event}, ensure_ascii=False) + "\n")


def claim(queue: Path, worker_id: str, order: str) -> tuple[str, dict[str, Any]] | None:
    lock = lock_queue(queue)
    try:
        state = json.loads(queue.read_text(encoding="utf-8"))
        task_ids = list(state["task_order"])
        if order == "reverse":
            task_ids.reverse()
        for task_id in task_ids:
            record = state["tasks"][task_id]
            if record.get("status", "pending") != "pending":
                continue
            record.update({
                "status": "running",
                "worker_id": worker_id,
                "pid": os.getpid(),
                "claimed_at": now(),
                "claimed_unix": time.time(),
                "attempts": int(record.get("attempts", 0) or 0) + 1,
            })
            write_queue(queue, state)
            append_event(queue, {"event": "task_claimed", "task_id": task_id, "worker_id": worker_id})
            return task_id, dict(record)
        return None
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def finish(queue: Path, task_id: str, worker_id: str, output: Path) -> None:
    lock = lock_queue(queue)
    try:
        state = json.loads(queue.read_text(encoding="utf-8"))
        record = state["tasks"][task_id]
        if record.get("status") != "running" or record.get("worker_id") != worker_id:
            raise RuntimeError(f"task {task_id} ownership changed while running")
        record.update({"status": "completed", "completed_at": now(), "output": str(output), "pid": os.getpid()})
        write_queue(queue, state)
        append_event(queue, {"event": "task_completed", "task_id": task_id, "worker_id": worker_id})
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def retry_task(queue: Path, task_id: str, worker_id: str, error: str) -> None:
    lock = lock_queue(queue)
    try:
        state = json.loads(queue.read_text(encoding="utf-8"))
        record = state["tasks"][task_id]
        if record.get("status") == "running" and record.get("worker_id") == worker_id:
            record.update({"status": "pending", "last_error": error[-4000:], "last_failed_at": now(), "pid": os.getpid()})
            write_queue(queue, state)
            append_event(queue, {"event": "task_retry", "task_id": task_id, "worker_id": worker_id, "error": error[-1000:]})
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def post_prompt(server: str, prompt: dict[str, Any]) -> str:
    payload = json.dumps({"prompt": prompt, "client_id": str(uuid.uuid4())}).encode()
    request = urllib.request.Request(
        server.rstrip("/") + "/prompt", data=payload,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        data = json.loads(response.read())
    if data.get("node_errors"):
        raise RuntimeError(json.dumps(data["node_errors"], ensure_ascii=False))
    if not data.get("prompt_id"):
        raise RuntimeError(f"ComfyUI did not return prompt_id: {data}")
    return str(data["prompt_id"])


def wait_prompt(server: str, prompt_id: str, timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    url = server.rstrip("/") + "/history/" + prompt_id
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                data = json.loads(response.read())
            item = data.get(prompt_id)
            if item:
                status = item.get("status", {})
                if status.get("completed") and status.get("status_str") == "success":
                    return item
                if status.get("status_str") == "error" or any(m[0] == "execution_error" for m in status.get("messages", []) if isinstance(m, list) and m):
                    raise RuntimeError(json.dumps(status, ensure_ascii=False))
        except urllib.error.HTTPError:
            pass
        time.sleep(5)
    raise TimeoutError(f"ComfyUI prompt {prompt_id} did not finish within {timeout}s")


def build_prompt(video_name: str, text: str, prefix: str) -> dict[str, Any]:
    return {
        "100": {"class_type": "VHS_LoadVideo", "inputs": {"video": video_name, "force_rate": 0.0, "custom_width": 0, "custom_height": 0, "frame_load_cap": FRAMES, "skip_first_frames": 0, "select_every_nth": 1, "format": "None"}},
        "119": {"class_type": "VAELoader", "inputs": {"vae_name": "minimax_h3_video_vae_fp16.safetensors"}},
        "120": {"class_type": "VAELoader", "inputs": {"vae_name": "minimax_h3_audio_vae_fp32.safetensors"}},
        "127": {"class_type": "UNETLoader", "inputs": {"unet_name": "minimax_h3_ref2va_pruned_int8_convrot.safetensors", "weight_dtype": "default"}},
        "128": {"class_type": "CLIPLoader", "inputs": {"clip_name": "qwen3vl_32b_minimax_h3_nvfp4_awq.safetensors", "type": "minimax", "device": "default"}},
        "136": {"class_type": "MiniMaxH3ReferenceToVideo", "inputs": {"clip": ["128", 0], "vae": ["119", 0], "audio_vae": ["120", 0], "prompt": text, "width": WIDTH, "height": HEIGHT, "length": FRAMES, "ref_image_size": "match", "ref_videos.ref_video_0": ["100", 0]}},
        "131": {"class_type": "MiniMaxH3SigmaShift", "inputs": {"model": ["127", 0], "shift_video": 12.0, "shift_audio": 3.0}},
        "129": {"class_type": "RandomNoise", "inputs": {"noise_seed": 42}},
        "123": {"class_type": "KSamplerSelect", "inputs": {"sampler_name": "res_multistep"}},
        "124": {"class_type": "BasicScheduler", "inputs": {"model": ["131", 0], "scheduler": "simple", "steps": 20, "denoise": 1.0}},
        "126": {"class_type": "BasicGuider", "inputs": {"model": ["131", 0], "conditioning": ["136", 0]}},
        "125": {"class_type": "SamplerCustomAdvanced", "inputs": {"noise": ["129", 0], "guider": ["126", 0], "sampler": ["123", 0], "sigmas": ["124", 0], "latent_image": ["136", 1]}},
        "122": {"class_type": "VAEDecode", "inputs": {"samples": ["125", 0], "vae": ["119", 0]}},
        "130": {"class_type": "CreateVideo", "inputs": {"images": ["122", 0], "fps": float(FPS), "bit_depth": 8}},
        # ComfyUI 0.31's dynamic combo is flattened in API prompts.
        "92": {"class_type": "SaveVideo", "inputs": {"video": ["130", 0], "filename_prefix": prefix, "format": "mp4", "codec": "h264", "codec.encoding": "re-encode", "codec.encoding.crf": float(CRF)}},
    }


def result_path(history: dict[str, Any], server_output: Path, prefix: str) -> Path:
    outputs = history.get("outputs", {}).get("92", {}).get("images", [])
    if not outputs:
        raise RuntimeError(f"ComfyUI succeeded without SaveVideo output: {history}")
    item = outputs[0]
    filename = item.get("filename")
    subfolder = item.get("subfolder", "")
    if not filename:
        raise RuntimeError(f"invalid SaveVideo output: {item}")
    path = server_output / subfolder / filename
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


def crop_output(canvas: Path, target: Path, meta: dict[str, Any]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp.mp4")
    try:
        subprocess.run([
            "ffmpeg", "-nostdin", "-y", "-v", "error", "-i", str(canvas),
            "-vf", f"crop={meta['content_width']}:{meta['content_height']}:{meta['offset_x']}:{meta['offset_y']}",
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", str(CRF),
            "-pix_fmt", "yuv420p", "-an", str(tmp),
        ], check=True)
        tmp.replace(target)
    finally:
        tmp.unlink(missing_ok=True)


def process_one(args: argparse.Namespace, task_id: str, record: dict[str, Any]) -> Path:
    rows = [json.loads(line) for line in args.metadata.open(encoding="utf-8")]
    index = int(task_id) - 1
    if index < 0 or index >= len(rows):
        raise IndexError(f"task {task_id} has no metadata row")
    row = rows[index]
    source = args.data_dir / str(row["file_name"])
    if not source.is_file():
        raise FileNotFoundError(source)
    preprocessed = args.preprocessed_dir / f"task_{task_id}.mp4"
    sidecar = args.preprocessed_dir / f"task_{task_id}.geometry.json"
    meta = preprocess(source, preprocessed, sidecar)
    input_name = f"seq_reverse_task_{task_id}.mp4"
    comfy_input = args.comfy_input / input_name
    if not comfy_input.exists() or comfy_input.stat().st_size != preprocessed.stat().st_size:
        comfy_input.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(preprocessed, comfy_input)
    prefix = f"seq_reverse/task_{task_id}"
    prompt_id = post_prompt(args.server, build_prompt(input_name, str(row["editing_instruction"]), prefix))
    history = wait_prompt(args.server, prompt_id, args.timeout)
    canvas = result_path(history, args.comfy_output, prefix)
    task_dir = args.out_dir / f"task_{task_id}"
    task_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(canvas, task_dir / "h3_canvas.mp4")
    output = task_dir / "output.mp4"
    crop_output(canvas, output, meta)
    (task_dir / "geometry.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, default=Path("/root/autodl-tmp/CoVEBench/data/covebench_hf/data/metadata.jsonl"))
    parser.add_argument("--data-dir", type=Path, default=Path("/root/autodl-tmp/CoVEBench/data/covebench_hf/data"))
    parser.add_argument("--preprocessed-dir", type=Path, default=Path("/root/seq/h3_preprocessed_626"))
    parser.add_argument("--out-dir", type=Path, default=Path("/root/seq_outputs/reverse_gpu0"))
    parser.add_argument("--comfy-input", type=Path, default=Path("/root/ComfyUI/input"))
    parser.add_argument("--comfy-output", type=Path, default=Path("/root/ComfyUI/output"))
    parser.add_argument("--server", default="http://127.0.0.1:8191")
    parser.add_argument("--worker-id", default="seq_reverse_gpu0")
    parser.add_argument("--order", choices=["forward", "reverse"], default="reverse")
    parser.add_argument("--timeout", type=float, default=3600)
    parser.add_argument("--poll-seconds", type=float, default=5)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    args.queue = args.queue.resolve()
    args.metadata = args.metadata.resolve()
    args.data_dir = args.data_dir.resolve()
    args.preprocessed_dir = args.preprocessed_dir.resolve()
    args.out_dir = args.out_dir.resolve()
    args.comfy_input = args.comfy_input.resolve()
    args.comfy_output = args.comfy_output.resolve()
    while True:
        claimed = claim(args.queue, args.worker_id, args.order)
        if claimed is None:
            print("no pending task; worker is idle", flush=True)
            return 0
        task_id, record = claimed
        print(f"claimed task {task_id} ({record.get('attempts')} attempt)", flush=True)
        try:
            output = process_one(args, task_id, record)
            finish(args.queue, task_id, args.worker_id, output)
            print(f"completed task {task_id}: {output}", flush=True)
        except Exception as error:
            retry_task(args.queue, task_id, args.worker_id, repr(error))
            print(f"task {task_id} failed and returned to pending: {error}", flush=True)
        if args.once:
            return 0
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
