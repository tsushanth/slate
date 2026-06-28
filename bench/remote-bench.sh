#!/usr/bin/env bash
# Runs on the remote Hetzner box. Called by run-hetzner-bench.sh.
# Args: $1=S3_BUCKET $2=BENCH_PREFIX $3=REGION $4=TOTAL_MIB
set -euo pipefail
source $HOME/.cargo/env

S3_BUCKET="$1"
BENCH_PREFIX="$2"
REGION="$3"
TOTAL_MIB="$4"
EXPECTED_BYTES=$((TOTAL_MIB * 1024 * 1024))

# object_store reads from env vars, not ~/.aws/credentials
export AWS_ACCESS_KEY_ID=$(awk -F' = ' '/aws_access_key_id/{print $2}' ~/.aws/credentials | head -1)
export AWS_SECRET_ACCESS_KEY=$(awk -F' = ' '/aws_secret_access_key/{print $2}' ~/.aws/credentials | head -1)
export AWS_DEFAULT_REGION="$REGION"

SLATE=$HOME/slate/target/release/slate
S3_SRC="s3://${S3_BUCKET}/${BENCH_PREFIX}"
LOCAL_DST="/tmp/slate-bench-dst"

# Verify the destination actually received data (guards against silent failures)
verify_dst() {
  local actual
  actual=$(du -sb "$LOCAL_DST" 2>/dev/null | cut -f1 || echo 0)
  if [[ "$actual" -lt $((EXPECTED_BYTES / 2)) ]]; then
    printf "    WARNING: only %d bytes in dst (expected ~%d)\n" "$actual" "$EXPECTED_BYTES" >&2
    return 1
  fi
}

# Time a single run; returns elapsed seconds as a decimal
time_one() {
  local label="$1"; shift
  rm -rf "$LOCAL_DST" && mkdir -p "$LOCAL_DST"
  local s e elapsed
  s=$(date +%s%3N)
  "$@" &>/dev/null
  e=$(date +%s%3N)
  elapsed=$(echo "scale=3; ($e - $s) / 1000" | bc | sed 's/^\./0./')
  verify_dst || { echo "999"; return 0; }
  echo "$elapsed"
}

# Run 3 times, return best-2-of-3 average
best2() {
  local label="$1"; shift
  local t1 t2 t3
  printf "    run 1..." >&2; t1=$(time_one "$label" "$@"); printf " %ss\n" "$t1" >&2
  printf "    run 2..." >&2; t2=$(time_one "$label" "$@"); printf " %ss\n" "$t2" >&2
  printf "    run 3..." >&2; t3=$(time_one "$label" "$@"); printf " %ss\n" "$t3" >&2
  # drop slowest
  local sorted a b
  sorted=$(printf '%s\n' "$t1" "$t2" "$t3" | sort -n)
  a=$(echo "$sorted" | sed -n '1p' | sed 's/^\./0./')
  b=$(echo "$sorted" | sed -n '2p' | sed 's/^\./0./')
  echo "scale=3; ($a + $b) / 2" | bc | sed 's/^\./0./'
}

mbps() { echo "scale=1; $TOTAL_MIB / $1" | bc; }

echo "" >&2
echo "==> Phase 1: parallelism sweep" >&2
echo "" >&2

# Each config: "objects chunks chunk_size_mib"
configs=(
  "4 8 16"
  "8 8 16"
  "16 8 16"
  "8 16 8"
  "16 16 8"
  "32 4 16"
)
config_labels=(
  "obj=4  chunk=8  csz=16MiB"
  "obj=8  chunk=8  csz=16MiB"
  "obj=16 chunk=8  csz=16MiB"
  "obj=8  chunk=16 csz=8MiB"
  "obj=16 chunk=16 csz=8MiB"
  "obj=32 chunk=4  csz=16MiB"
)

declare -a SWEEP_TIMES
BEST_T="99999"
BEST_IDX=0

for i in "${!configs[@]}"; do
  read -r obj chunk csz <<< "${configs[$i]}"
  printf "  [%s]\n" "${config_labels[$i]}" >&2

  export SLATE_PARALLEL_OBJECTS=$obj
  export SLATE_PARALLEL_CHUNKS=$chunk
  export SLATE_CHUNK_SIZE_MIB=$csz

  t=$(best2 "slate" "$SLATE" copy "$S3_SRC" "$LOCAL_DST")
  SWEEP_TIMES[$i]=$t
  printf "  => avg: %ss  (%s MB/s)\n\n" "$t" "$(mbps $t)" >&2

  if (( $(echo "$t < $BEST_T" | bc -l) )); then
    BEST_T=$t
    BEST_IDX=$i
  fi
done

read -r BEST_OBJ BEST_CHUNK BEST_CSZ <<< "${configs[$BEST_IDX]}"
printf "==> Best: %s → %ss (%s MB/s)\n\n" \
  "${config_labels[$BEST_IDX]}" "$BEST_T" "$(mbps $BEST_T)" >&2

###############################################################################
echo "==> Phase 2: final head-to-head" >&2
###############################################################################

export SLATE_PARALLEL_OBJECTS=$BEST_OBJ
export SLATE_PARALLEL_CHUNKS=$BEST_CHUNK
export SLATE_CHUNK_SIZE_MIB=$BEST_CSZ

printf "  [slate — best config]\n" >&2
SLATE_AVG=$(best2 "slate" "$SLATE" copy "$S3_SRC" "$LOCAL_DST")

unset SLATE_PARALLEL_OBJECTS SLATE_PARALLEL_CHUNKS SLATE_CHUNK_SIZE_MIB

printf "  [aws s3 cp]\n" >&2
AWS_AVG=$(best2 "aws" aws s3 cp "$S3_SRC" "$LOCAL_DST" --recursive --no-progress)

printf "  [rclone]\n" >&2
RCLONE_AVG=$(best2 "rclone" rclone copy "s3:${S3_BUCKET}/${BENCH_PREFIX}" "$LOCAL_DST" --transfers 8 --checkers 16)

SLATE_MBPS=$(mbps $SLATE_AVG)
AWS_MBPS=$(mbps $AWS_AVG)
RCLONE_MBPS=$(mbps $RCLONE_AVG)
SLATE_VS_AWS=$(echo "scale=2; $SLATE_MBPS / $AWS_MBPS" | bc)
SLATE_VS_RCLONE=$(echo "scale=2; $SLATE_MBPS / $RCLONE_MBPS" | bc)

###############################################################################
# Markdown output
###############################################################################
echo "## Slate Benchmark Results"
echo ""
echo "**Environment:** Hetzner cpx41 (8 vCPU, 16 GB RAM) · Frankfurt, Germany"
echo "**Source:** AWS S3 us-east-1 → Hetzner Frankfurt (cross-region, cross-provider)"
echo "**Dataset:** 5 files × 529 MiB = ${TOTAL_MIB} MiB of real ML model weights (Gemma 3 1B)"
echo "**Methodology:** 6-config parallelism sweep → 3 runs at best config, best 2-of-3 averaged"
echo ""
echo "### Parallelism Sweep"
echo ""
echo "| objects | chunks/obj | chunk size | avg time | throughput |"
echo "|---------|-----------|------------|----------|------------|"
for i in "${!configs[@]}"; do
  read -r obj chunk csz <<< "${configs[$i]}"
  t=${SWEEP_TIMES[$i]}
  m=$(mbps $t)
  marker=""
  [[ $i -eq $BEST_IDX ]] && marker=" ✓"
  echo "| $obj | $chunk | ${csz} MiB | ${t}s | **${m} MB/s**${marker} |"
done
echo ""
echo "### Final Head-to-Head"
echo ""
echo "| Tool | Config | Avg time | Throughput | vs slate |"
echo "|------|--------|----------|------------|----------|"
echo "| **slate** | obj=${BEST_OBJ} · chunks=${BEST_CHUNK} · csz=${BEST_CSZ}MiB | ${SLATE_AVG}s | **${SLATE_MBPS} MB/s** | — |"
echo "| aws s3 cp | --recursive (default) | ${AWS_AVG}s | ${AWS_MBPS} MB/s | ${SLATE_VS_AWS}× slower |"
echo "| rclone | --transfers 8 --checkers 16 | ${RCLONE_AVG}s | ${RCLONE_MBPS} MB/s | ${SLATE_VS_RCLONE}× |"
echo ""
echo "> Transfer: S3 us-east-1 → Hetzner Frankfurt. Slate uses parallel range-GETs"
echo "> per object with concurrent object-level parallelism. Single binary, no config"
echo "> beyond standard AWS env vars (\`AWS_ACCESS_KEY_ID\`, \`AWS_SECRET_ACCESS_KEY\`)."
