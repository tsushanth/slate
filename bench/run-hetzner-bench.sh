#!/usr/bin/env bash
# Slate Hetzner Benchmark
# Uploads Slate source to a Hetzner cpx41, builds it, then runs a head-to-head
# benchmark against aws s3 cp and rclone. Saves results to bench/results-latest.md.
#
# Usage:
#   ./bench/run-hetzner-bench.sh

set -euo pipefail

HETZNER_HOST="178.156.192.31"
SSH_KEY="$HOME/.ssh/google_compute_engine"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o BatchMode=yes"
SSH="ssh $SSH_OPTS root@$HETZNER_HOST"

S3_BUCKET="tidymail-models-451115460668"
S3_SEED_KEY="gemma3-1b-it-int4.task"
BENCH_PREFIX="slate-bench"
REGION="us-east-1"
TOTAL_MIB=2645   # 5 × 529 MiB

WORK_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS_FILE="$WORK_DIR/bench/results-latest.md"

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)

###############################################################################
echo "==> [1/6] Preparing S3 benchmark dataset (5 × 529 MiB)..."
###############################################################################
for i in 1 2 3 4 5; do
  KEY="${BENCH_PREFIX}/file_${i}.bin"
  if aws s3api head-object --bucket "$S3_BUCKET" --key "$KEY" --region "$REGION" &>/dev/null; then
    echo "    Exists: $KEY"
  else
    aws s3 cp "s3://${S3_BUCKET}/${S3_SEED_KEY}" "s3://${S3_BUCKET}/${KEY}" \
      --region "$REGION" --no-progress
    echo "    Copied: $KEY"
  fi
done

###############################################################################
echo "==> [2/6] Uploading Slate source to Hetzner..."
###############################################################################
$SSH "mkdir -p ~/slate"
(cd "$WORK_DIR" && tar czf - \
  --exclude target \
  --exclude .git \
  --exclude 'bench/results-*.md' \
  . 2>/dev/null) | $SSH "tar xzf - -C ~/slate 2>/dev/null; echo '    done'"

###############################################################################
echo "==> [3/6] Installing tools (Rust / rclone / AWS CLI v2)..."
###############################################################################
$SSH bash /dev/stdin << BOOTSTRAP
set -euo pipefail

# AWS creds
mkdir -p ~/.aws
printf '[default]\naws_access_key_id = ${AWS_KEY}\naws_secret_access_key = ${AWS_SECRET}\n' > ~/.aws/credentials
printf '[default]\nregion = ${REGION}\noutput = json\n' > ~/.aws/config

# Rust
if ! command -v cargo &>/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal -q 2>&1 | tail -2
fi
source \$HOME/.cargo/env

# rclone
if ! command -v rclone &>/dev/null; then
  curl -fsSL https://rclone.org/install.sh | bash 2>&1 | tail -1
fi

# AWS CLI v2
if ! command -v aws &>/dev/null; then
  cd /tmp
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
  unzip -q awscliv2.zip
  ./aws/install -i /usr/local/aws-cli -b /usr/local/bin 2>&1 | tail -1
  rm -rf awscliv2.zip aws
fi

# rclone S3 config
mkdir -p ~/.config/rclone
printf '[s3]\ntype = s3\nprovider = AWS\nenv_auth = true\nregion = ${REGION}\n' > ~/.config/rclone/rclone.conf

echo "  rustc:  \$(rustc --version)"
echo "  rclone: \$(rclone --version | head -1)"
echo "  aws:    \$(aws --version)"
BOOTSTRAP

###############################################################################
echo "==> [4/6] Building Slate release binary on Hetzner..."
###############################################################################
$SSH bash /dev/stdin << 'BUILD'
set -euo pipefail
source $HOME/.cargo/env
cd ~/slate
cargo build --release --bin slate 2>&1 | grep -E "Compiling slate|Finished|^error" || true
echo "  binary: $(ls -lh target/release/slate | awk '{print $5, $9}')"
BUILD

###############################################################################
echo "==> [5/6] Running benchmark (3 tools × 3 runs)..."
###############################################################################
# Upload the remote benchmark script and run it
scp $SSH_OPTS "$WORK_DIR/bench/remote-bench.sh" "root@$HETZNER_HOST:~/slate/bench/remote-bench.sh"
BENCHMARK_OUTPUT=$($SSH bash ~/slate/bench/remote-bench.sh \
  "$S3_BUCKET" "$BENCH_PREFIX" "$REGION" "$TOTAL_MIB" 2>&1)

# Split stderr progress from stdout markdown
PROGRESS=$(echo "$BENCHMARK_OUTPUT" | grep '^\s*\[' || true)
MARKDOWN=$(echo "$BENCHMARK_OUTPUT" | grep -v '^\s*\[' || true)

echo "$PROGRESS"
echo ""
echo "$MARKDOWN"

###############################################################################
echo "==> [6/6] Saving results and cleaning up S3..."
###############################################################################
mkdir -p "$WORK_DIR/bench"
printf "<!-- Generated %s -->\n\n%s\n" \
  "$(date -u '+%Y-%m-%d %H:%M UTC')" \
  "$MARKDOWN" > "$RESULTS_FILE"

aws s3 rm "s3://${S3_BUCKET}/${BENCH_PREFIX}/" --recursive --region "$REGION" 2>/dev/null || true

echo "Results saved to bench/results-latest.md"
echo "Done."
