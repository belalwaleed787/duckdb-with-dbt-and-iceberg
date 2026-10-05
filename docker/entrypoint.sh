#!/bin/sh
# Runs dbt inside the project. With no arguments it runs the whole pipeline: `dbt build`.
#   docker run IMAGE                                  -> dbt build
#   docker run IMAGE build --select silver+           -> dbt build --select silver+
#   docker run IMAGE dbt build --full-refresh         -> dbt build --full-refresh
#   docker run IMAGE sh                               -> shell, for debugging
# When dbt finishes, one "run_summary" JSON line reports time, CPU, peak memory and network use.
set -eu

mkdir -p "$(dirname "$DUCKDB_PATH")" "$DBT_TARGET_PATH" "$DBT_LOG_PATH"

# Memory available to this container: the cgroup limit, or the machine's RAM if there is none.
mem_total=$(( $(awk '/^MemTotal:/ {print $2}' /proc/meminfo) * 1024 ))
mem_limit=$(cat /sys/fs/cgroup/memory.max 2>/dev/null \
    || cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null \
    || echo max)
case "$mem_limit" in
    '' | max | *[!0-9]*) mem_limit=$mem_total ;;
esac
if [ "$mem_limit" -gt "$mem_total" ]; then
    mem_limit=$mem_total
fi
# On ECS Fargate the task's memory limit is not visible in the container's cgroup; read it
# from the task metadata endpoint instead.
if [ -n "${ECS_CONTAINER_METADATA_URI_V4:-}" ]; then
    task_mib=$(python -c "import json, os, urllib.request; print(int(json.load(urllib.request.urlopen(os.environ['ECS_CONTAINER_METADATA_URI_V4'] + '/task', timeout=3))['Limits']['Memory']))" 2>/dev/null || true)
    if [ -n "$task_mib" ] && [ $((task_mib * 1048576)) -lt "$mem_limit" ]; then
        mem_limit=$((task_mib * 1048576))
    fi
fi

# DuckDB's own default (80% of RAM) leaves too little room for dbt/Python and, on Docker
# Desktop, for the Docker VM itself. Default to 60% of the container's memory.
if [ -z "${DUCKDB_MEMORY_LIMIT:-}" ]; then
    DUCKDB_MEMORY_LIMIT="$((mem_limit * 6 / 10 / 1048576))MiB"
    export DUCKDB_MEMORY_LIMIT
fi

# dbt finds the project through DBT_PROJECT_DIR / DBT_PROFILES_DIR. Run from a writable
# scratch directory because DuckDB's Iceberg writer creates an empty ./data folder.
cd "$(dirname "$DUCKDB_PATH")"

if [ "$#" -eq 0 ]; then
    set -- build
fi

case "$1" in
    sh | bash) exec "$@" ;;
    dbt) shift ;;
esac

# ---- resource metrics (cgroup v2 in Docker Desktop, cgroup v1 or v2 on Fargate) ----
anon_memory_bytes() {  # memory used by processes, without the page cache
    for f in /sys/fs/cgroup/memory.stat /sys/fs/cgroup/memory/memory.stat; do
        if [ -r "$f" ]; then
            awk '$1 == "anon" || $1 == "total_rss" {printf "%.0f\n", $2; exit}' "$f"
            return
        fi
    done
    echo 0
}
cpu_usec() {
    if [ -r /sys/fs/cgroup/cpu.stat ]; then
        awk '$1 == "usage_usec" {printf "%.0f\n", $2}' /sys/fs/cgroup/cpu.stat
    elif [ -r /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
        awk '{printf "%.0f\n", $1 / 1000}' /sys/fs/cgroup/cpuacct/cpuacct.usage
    else
        echo 0
    fi
}
network_bytes() {  # "received sent", all interfaces except loopback
    sed 's/:/ /' /proc/net/dev | awk 'NR > 2 && $1 != "lo" {rx += $2; tx += $10} END {printf "%.0f %.0f\n", rx, tx}'
}

peak_file="$(pwd)/.peak_memory_bytes"
echo 0 > "$peak_file"
(
    peak=0
    while :; do
        current=$(anon_memory_bytes)
        if [ "${current:-0}" -gt "$peak" ]; then
            peak=$current
            echo "$peak" > "$peak_file"
        fi
        sleep 5
    done
) &
sampler_pid=$!

started=$(date +%s)
cpu_before=$(cpu_usec)
network_before=$(network_bytes)

# Run dbt as a child (not exec) so the summary can be printed; forward stop signals to it.
dbt "$@" &
dbt_pid=$!
trap 'kill -TERM "$dbt_pid" 2>/dev/null || true' TERM INT
status=0
wait "$dbt_pid" || status=$?
if kill -0 "$dbt_pid" 2>/dev/null; then
    wait "$dbt_pid" || status=$?
fi
kill "$sampler_pid" 2>/dev/null || true

network_after=$(network_bytes)
elapsed=$(( $(date +%s) - started ))
cpu_seconds=$(( ($(cpu_usec) - cpu_before) / 1000000 ))
received=$(( ${network_after% *} - ${network_before% *} ))
sent=$(( ${network_after#* } - ${network_before#* } ))
peak=$(cat "$peak_file")

echo "run_summary {\"exit_code\": $status, \"elapsed_s\": $elapsed, \"cpu_s\": $cpu_seconds, \"cpus\": $(nproc), \"container_memory_mib\": $((mem_limit / 1048576)), \"duckdb_memory_limit\": \"$DUCKDB_MEMORY_LIMIT\", \"peak_memory_mib\": $((peak / 1048576)), \"network_in_mib\": $((received / 1048576)), \"network_out_mib\": $((sent / 1048576)), \"dbt_threads\": \"${DBT_THREADS:-}\"}"
exit "$status"
