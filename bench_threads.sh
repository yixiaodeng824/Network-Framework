#!/usr/bin/env bash
#
# bench_threads.sh — 压测 + 线程占用采样 (multi-reactor 版)
#
# 启动 server, 运行 benchmarks.echo (100/500/1000 并发各 10s),
# 同时按固定间隔采样 /proc/<pid>/task 下每个线程的状态与 CPU 占用,
# 并按角色归类:
#   主线程 acceptLoop / sub-epoll 线程 / 线程池 worker / logger 等其他线程
#
# 用法:
#   ./bench_threads.sh                      # 默认: Release 构建 + 自动线程数 + 2 个 sub-epoll
#   NF_THREADS=4 ./bench_threads.sh         # 指定 worker 线程数
#   NF_THREADS=16 NF_INTERVAL=0.2 ./bench_threads.sh
#   NF_SUBS=3 ./bench_threads.sh            # 若 main.cpp 的 sub_count 改了, 按实际值传
#   NF_BENCH_CMD="python3 -m benchmarks.echo_mp --procs 8 --clients 100,500,1000" ./bench_threads.sh
#
# 输出:
#   thread_usage.csv    每个采样点的汇总 (总线程数/状态数量/各角色 CPU%)
#   thread_detail.csv   每个线程每个采样点的明细 (角色/状态/CPU%)
#   bench_output.txt    压测客户端原始输出
#
set -euo pipefail

PORT="${NF_PORT:-8888}"
THREADS="${NF_THREADS:-0}"        # 0 = 自动 = CPU 逻辑核数
SUBS="${NF_SUBS:-2}"              # MainReactor 里写死的 sub_count
INTERVAL="${NF_INTERVAL:-0.5}"    # 采样间隔(秒)
SUMMARY="${NF_SUMMARY:-thread_usage.csv}"
DETAIL="${NF_DETAIL:-thread_detail.csv}"
BENCH_OUT="${NF_BENCH_OUT:-bench_output.txt}"
BENCH_CMD="${NF_BENCH_CMD:-python3 -m benchmarks.echo}"
SERVER_BIN="${NF_SERVER_BIN:-./build/server}"

cd "$(dirname "$0")"

# ---------- 1) 构建 ----------
echo "==> building server (Release)"
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build build --target server --parallel >/dev/null

# ---------- 2) 启动 server ----------
echo "==> starting server on port $PORT (threads=$THREADS, subs=$SUBS)"
"$SERVER_BIN" -p "$PORT" -t "$THREADS" &
SERVER_PID=$!
cleanup() {
    kill -INT "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# 等监听就绪
ready=0
for _ in $(seq 1 100); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
        exec 3>&- 3<&- 2>/dev/null || true
        ready=1
        break
    fi
    sleep 0.05
done
if [[ "$ready" != "1" ]]; then
    echo "ERROR: server did not become ready on port $PORT" >&2
    exit 1
fi

# 线程池用 0(自动)时, 实际 worker 数 = 逻辑核数, 与 server 内 hardware_concurrency() 一致
if [[ "$THREADS" -eq 0 ]]; then
    WORKERS=$(nproc)
else
    WORKERS="$THREADS"
fi
sleep 1   # 先留一小段空闲采样

# ---------- 3) 采样器(后台) ----------
# 角色判定依赖 tid 分配顺序(与线程创建顺序一致, 见 main.cpp / main_reactor.cpp / Logger.cpp):
#   位置 0:              主线程 acceptLoop (tid == pid);
#   位置 1..WORKERS:     线程池 worker (ThreadPool 构造时创建);
#   位置 WORKERS+1:      异步 logger (start() 里第一条 LOG_INFO 触发, 在 sub 线程创建之前);
#   位置 WORKERS+2 .. WORKERS+1+SUBS: sub-epoll 线程 (MainReactor::start 最后创建)。
python3 - "$SERVER_PID" "$INTERVAL" "$SUMMARY" "$DETAIL" "$WORKERS" "$SUBS" <<'PY' &
import csv
import os
import sys
import time

pid = int(sys.argv[1])
interval = float(sys.argv[2])
summary_path, detail_path = sys.argv[3], sys.argv[4]
workers = int(sys.argv[5])
subs = int(sys.argv[6])
clk = os.sysconf("SC_CLK_TCK")
prev = {}          # tid -> (cpu_ticks, sample_time)
start = time.monotonic()


def role_for(pos: int, tid: int) -> str:
    if tid == pid:
        return "main_accept"
    if 1 <= pos <= workers:
        return "pool_worker"
    if pos == workers + 1:
        return "logger"
    if workers + 1 < pos <= workers + 1 + subs:
        return "sub_epoll"
    return "other"


with open(summary_path, "w", newline="") as sf, open(detail_path, "w", newline="") as df:
    sw = csv.writer(sf)
    dw = csv.writer(df)
    sw.writerow(["t", "total_threads", "threads_R", "threads_S", "threads_D",
                 "cpu_main", "cpu_sub_avg", "cpu_sub_max",
                 "cpu_pool_avg", "cpu_pool_max", "cpu_logger_max", "active_others"])
    dw.writerow(["t", "tid", "role", "state", "cpu_pct", "comm"])

    while True:
        task_dir = f"/proc/{pid}/task"
        try:
            tids = sorted(int(t) for t in os.listdir(task_dir) if t.isdigit())
        except FileNotFoundError:
            break

        now = time.monotonic()
        t = now - start
        rows = []
        for tid in tids:
            try:
                with open(f"{task_dir}/{tid}/stat") as f:
                    parts = f.read().split()
            except (FileNotFoundError, ProcessLookupError):
                continue
            if len(parts) < 15:
                continue
            state = parts[2]
            cpu_ticks = int(parts[13]) + int(parts[14])  # utime + stime
            comm = parts[1].strip("()")
            cpu_pct = 0.0
            if tid in prev:
                pt, pts = prev[tid]
                dt = now - pts
                if dt > 0:
                    cpu_pct = (cpu_ticks - pt) / dt / clk * 100.0
            prev[tid] = (cpu_ticks, now)
            pos = tids.index(tid)
            role = role_for(pos, tid)
            rows.append((tid, role, state, cpu_pct, comm))
            dw.writerow([f"{t:.2f}", tid, role, state, f"{cpu_pct:.2f}", comm])

        total = len(rows)
        n_r = sum(1 for r in rows if r[2] == "R")
        n_s = sum(1 for r in rows if r[2] == "S")
        n_d = sum(1 for r in rows if r[2] == "D")
        main_cpu = next((r[3] for r in rows if r[1] == "main_accept"), 0.0)
        subs_cpu = [r[3] for r in rows if r[1] == "sub_epoll"]
        pools_cpu = [r[3] for r in rows if r[1] == "pool_worker"]
        logger_cpu = [r[3] for r in rows if r[1] == "logger"]
        sub_avg = sum(subs_cpu) / len(subs_cpu) if subs_cpu else 0.0
        sub_max = max(subs_cpu) if subs_cpu else 0.0
        pool_avg = sum(pools_cpu) / len(pools_cpu) if pools_cpu else 0.0
        pool_max = max(pools_cpu) if pools_cpu else 0.0
        logger_max = max(logger_cpu) if logger_cpu else 0.0
        active = sum(1 for r in rows if r[1] != "main_accept" and r[3] >= 50.0)
        sw.writerow([f"{t:.2f}", total, n_r, n_s, n_d,
                     f"{main_cpu:.2f}", f"{sub_avg:.2f}", f"{sub_max:.2f}",
                     f"{pool_avg:.2f}", f"{pool_max:.2f}", f"{logger_max:.2f}", active])
        sf.flush()
        df.flush()
        time.sleep(interval)
PY
SAMPLER_PID=$!

# ---------- 4) 压测 ---------- (NF_BENCH_CMD 按 shell 求值, 默认跑 benchmarks.echo)
echo "==> running benchmark: $BENCH_CMD"
eval "$BENCH_CMD" | tee "$BENCH_OUT"

# ---------- 5) 收尾: 关 server, 等采样器退出 ----------
kill -INT "$SERVER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true
trap - EXIT

# ---------- 6) 汇总 ----------
python3 - "$SUMMARY" <<'PY'
import csv
import sys

rows = list(csv.DictReader(open(sys.argv[1])))
if not rows:
    print("no samples captured")
    sys.exit(0)

totals = [int(r["total_threads"]) for r in rows]
cpu_main = [float(r["cpu_main"]) for r in rows]
cpu_sub_avg = [float(r["cpu_sub_avg"]) for r in rows]
cpu_sub_max = [float(r["cpu_sub_max"]) for r in rows]
cpu_pool_avg = [float(r["cpu_pool_avg"]) for r in rows]
cpu_pool_max = [float(r["cpu_pool_max"]) for r in rows]
cpu_logger_max = [float(r["cpu_logger_max"]) for r in rows]
active = [int(r["active_others"]) for r in rows]


def pct(v):
    return f"{v:.1f}%"


def mean(v):
    return sum(v) / len(v)


print()
print("===== 线程占用汇总 (multi-reactor) =====")
print(f"采样点数           : {len(rows)}")
print(f"总线程数           : 最小 {min(totals)} / 最大 {max(totals)} / 中值 {sorted(totals)[len(totals)//2]}")
print(f"主线程 acceptLoop  : 平均 CPU {pct(mean(cpu_main))}  峰值 {pct(max(cpu_main))}")
print(f"sub-epoll 线程     : 平均 CPU {pct(mean(cpu_sub_avg))}  单线程峰值 {pct(max(cpu_sub_max))}")
print(f"worker 池线程      : 平均 CPU {pct(mean(cpu_pool_avg))}  单线程峰值 {pct(max(cpu_pool_max))}")
print(f"logger 线程        : 峰值 CPU {pct(max(cpu_logger_max))}")
print(f"活跃线程(CPU>=50%): 平均 {mean(active):.1f} 个 / 峰值 {max(active)} 个")
print("明细见 thread_detail.csv, 逐采样点汇总见 thread_usage.csv")
PY
