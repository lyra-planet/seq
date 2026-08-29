# seq

这是 MiniMax-H3 本地批量推理的共享任务队列仓库。仓库只保存任务状态和同步脚本，不保存模型权重、输入视频、输出视频、ComfyUI 数据库或日志。

## 文件

- `shared_task_queue.json`：626 条任务的持久化状态。`task_order` 是 planning 文件的原始顺序，任务状态为 `pending`、`running`、`completed` 或 `failed`。
- `seq_queue_git_sync.py`：监听队列事件；每完成一条任务，自动提交并推送队列状态。遇到两个服务器同时推送时，会合并队列状态并重试。
- `.gitignore`：避免把锁、事件日志和本地推理产物加入仓库。

队列中的 `completed` 任务已经在现有机器生成并通过视频几何校验。另一台服务器必须直接复用这个队列，不能重新初始化或删除已完成状态。

## 当前领取策略

本机两路 worker 使用正向 FIFO：从 `task_order` 的开头向后领取第一个未完成任务。两张卡共享同一个队列，先完成的 worker 继续领取下一个任务，不做固定的一半一半分片。

另一台服务器可以使用 `--order reverse`，从 `task_order` 的末尾向前领取第一个未完成任务。这样在两台机器同时运行时，通常会从队列两端推进，减少重复领取。队列状态仍由 `shared_task_queue.json` 中的完成标记决定。

## 在另一台服务器继续推理

先确保另一台服务器已经有与当前一致的 MiniMax-H3 INT8 ConvRot、ComfyUI、planning 文件和预处理视频目录，然后执行：

```bash
git clone https://github.com/lyra-planet/seq.git
cd seq
git pull --rebase origin main
```

如果服务器需要通过本机 Clash 代理访问 GitHub，先设置 7890 端口：

```bash
git config --local http.proxy http://127.0.0.1:7890
git config --local https.proxy http://127.0.0.1:7890
```

当前源服务器的仓库已经使用上述代理；代理只影响 Git 同步，不会改变 H3 推理请求。

不要运行会重新创建队列的初始化命令。使用本机已有的 worker 脚本，并把 `--queue` 指向克隆出来的 `shared_task_queue.json`。下面是单个 GPU worker 的示例，路径请按另一台服务器实际位置修改：

```bash
python /path/to/Aurora_Original_CoVEBench/aurora_msr_control/scripts/run_h3_ref2va_int8_shared_queue.py \
  --planning-results /path/to/planning_results.jsonl \
  --queue "$PWD/shared_task_queue.json" \
  --out-dir /path/to/seq_outputs/gpu0 \
  --server http://127.0.0.1:8192 \
  --worker-id seq_remote_gpu0 \
  --completion-root /path/to/seq_outputs/gpu0 \
  --preprocessed-dir /path/to/h3_preprocessed_626 \
  --order reverse
```

两张 GPU 时启动两个进程，使用不同的 `--worker-id`、`--server` 和 `--out-dir`，但必须指向同一个克隆目录下的 `shared_task_queue.json`。不要手工按 task ID 切半，也不要让两个 worker 使用不同的队列副本。

## 每条任务自动同步 Git

在运行 worker 的同一台服务器上另开一个终端或 screen，启动同步器：

```bash
python seq_queue_git_sync.py \
  --repo "$PWD" \
  --queue "$PWD/shared_task_queue.json" \
  --watch
```

同步器会：

1. 读取队列事件文件中的 `task_completed` 事件；
2. 将最新的 `shared_task_queue.json` 提交为 `queue: complete task_<id>`；
3. 推送到 `origin/main`；
4. 网络暂时失败时保留本地提交，并在下一轮继续尝试推送；
5. 如果远端已经有另一台服务器的提交，按任务状态合并队列，`completed` 优先保留，再重试推送。

如果只需要手工同步当前状态：

```bash
git add shared_task_queue.json
git commit -m "queue: update task state"
git push origin main
```

另一台服务器开始新一轮任务前，先同步远端状态：

```bash
git pull --rebase origin main
```

如果同一时间有两台服务器都向同一个远端推送，两边各运行一个同步器即可；同步器会处理非快进推送并合并不同任务的状态。启动新 worker 前仍建议先执行 `git pull --rebase origin main`，确认 planning 文件与队列版本一致。

## 查看进度

```bash
python - <<'PY'
import json
from pathlib import Path

p = Path('shared_task_queue.json')
s = json.loads(p.read_text())
counts = {}
for record in s['tasks'].values():
    status = record.get('status', 'pending')
    counts[status] = counts.get(status, 0) + 1
print(counts)
for task_id in s['task_order']:
    record = s['tasks'][task_id]
    if record.get('status') == 'running':
        print('running', task_id, record.get('worker_id'), 'attempts', record.get('attempts'))
PY
```

队列锁和事件日志是运行时文件，不需要提交：

```text
shared_task_queue.json.lock
shared_task_queue.json.events.jsonl
```
