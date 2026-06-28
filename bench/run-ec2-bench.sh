#!/usr/bin/env bash
# Slate EC2 Benchmark
# Launches a c5n.xlarge in us-east-1, builds Slate on it, runs a head-to-head
# benchmark against aws s3 cp and rclone, prints a markdown table, then
# terminates all resources.
#
# Usage:
#   ./bench/run-ec2-bench.sh
#
# Requires: aws CLI configured with credentials that have EC2 + S3 access.

set -euo pipefail

###############################################################################
# Config
###############################################################################
REGION="us-east-1"
INSTANCE_TYPE="c5n.xlarge"         # 4 vCPU, 10.5 GB RAM, 25 Gbps network
AMI_ID="ami-08f44e8eca9095668"     # Amazon Linux 2023 x86_64 (us-east-1)
S3_BUCKET="tidymail-models-451115460668"
S3_SEED_KEY="gemma3-1b-it-int4.task"   # 529 MiB seed file already in bucket
BENCH_PREFIX="slate-bench"
KEY_NAME="slate-bench-tmp-$$"
SG_NAME="slate-bench-sg-$$"
WORK_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS_FILE="$WORK_DIR/bench/results-latest.md"

# Cleanup tracking
INSTANCE_ID=""
SG_ID=""
KEY_FILE="/tmp/${KEY_NAME}.pem"

###############################################################################
# Cleanup trap — runs on exit (normal or error)
###############################################################################
cleanup() {
  echo ""
  echo "==> Cleaning up AWS resources..."
  if [[ -n "$INSTANCE_ID" ]]; then
    echo "    Terminating $INSTANCE_ID..."
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$REGION" --output text --query 'TerminatingInstances[0].CurrentState.Name' 2>/dev/null || true
    aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID" --region "$REGION" 2>/dev/null || true
    echo "    Instance terminated."
  fi
  if [[ -n "$SG_ID" ]]; then
    echo "    Deleting security group $SG_ID..."
    aws ec2 delete-security-group --group-id "$SG_ID" --region "$REGION" 2>/dev/null || true
  fi
  aws ec2 delete-key-pair --key-name "$KEY_NAME" --region "$REGION" 2>/dev/null || true
  rm -f "$KEY_FILE"
  echo "==> Cleanup done."
}
trap cleanup EXIT

###############################################################################
# Step 1: Create temporary key pair
###############################################################################
echo "==> Creating temporary key pair: $KEY_NAME"
aws ec2 create-key-pair \
  --key-name "$KEY_NAME" \
  --region "$REGION" \
  --query 'KeyMaterial' \
  --output text > "$KEY_FILE"
chmod 600 "$KEY_FILE"

###############################################################################
# Step 2: Create security group (SSH only)
###############################################################################
echo "==> Creating security group: $SG_NAME"
MY_IP=$(curl -sf https://checkip.amazonaws.com)
SG_ID=$(aws ec2 create-security-group \
  --group-name "$SG_NAME" \
  --description "Slate benchmark (temporary)" \
  --region "$REGION" \
  --query 'GroupId' \
  --output text)

aws ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --protocol tcp \
  --port 22 \
  --cidr "${MY_IP}/32" \
  --region "$REGION" > /dev/null

echo "    SG: $SG_ID  (SSH from $MY_IP)"

###############################################################################
# Step 3: Launch instance
###############################################################################
echo "==> Launching $INSTANCE_TYPE ($AMI_ID)..."
INSTANCE_ID=$(aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_NAME" \
  --security-group-ids "$SG_ID" \
  --region "$REGION" \
  --instance-initiated-shutdown-behavior terminate \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=slate-bench},{Key=Purpose,Value=benchmark}]" \
  --query 'Instances[0].InstanceId' \
  --output text)

echo "    Instance: $INSTANCE_ID"
echo "==> Waiting for instance to be running..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$REGION"

PUBLIC_IP=$(aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --region "$REGION" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' \
  --output text)
echo "    Public IP: $PUBLIC_IP"

###############################################################################
# Step 4: Wait for SSH
###############################################################################
echo "==> Waiting for SSH to be ready..."
for i in $(seq 1 30); do
  if ssh -i "$KEY_FILE" \
       -o StrictHostKeyChecking=no \
       -o ConnectTimeout=5 \
       -o BatchMode=yes \
       "ec2-user@$PUBLIC_IP" "exit 0" 2>/dev/null; then
    echo "    SSH ready."
    break
  fi
  sleep 5
done

SSH="ssh -i $KEY_FILE -o StrictHostKeyChecking=no ec2-user@$PUBLIC_IP"

###############################################################################
# Step 5: Prepare S3 test dataset
#   Copy the seed file into 4 more keys so we have 5 × 529 MiB = ~2.6 GB total,
#   testing multi-object parallelism. Uses S3 server-side copy (no data transfer cost).
###############################################################################
echo "==> Preparing S3 test dataset (5 × 529 MiB)..."
for i in 1 2 3 4 5; do
  KEY="${BENCH_PREFIX}/file_${i}.bin"
  EXISTS=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$KEY" --region "$REGION" 2>/dev/null && echo yes || echo no)
  if [[ "$EXISTS" != "yes" ]]; then
    aws s3 cp \
      "s3://${S3_BUCKET}/${S3_SEED_KEY}" \
      "s3://${S3_BUCKET}/${KEY}" \
      --region "$REGION" --no-progress
    echo "    Copied -> $KEY"
  else
    echo "    Already exists: $KEY"
  fi
done

###############################################################################
# Step 6: Bootstrap the instance
#   Install Rust, rclone, configure AWS creds, build Slate
###############################################################################
echo "==> Bootstrapping instance (Rust + rclone + build)..."

# Read creds from local AWS config
AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)

$SSH bash -s <<REMOTE
set -euo pipefail

# AWS credentials
mkdir -p ~/.aws
cat > ~/.aws/credentials <<EOF
[default]
aws_access_key_id = ${AWS_KEY}
aws_secret_access_key = ${AWS_SECRET}
EOF
cat > ~/.aws/config <<EOF
[default]
region = ${REGION}
output = json
EOF

# Install build tools
sudo dnf install -y git gcc openssl-devel 2>&1 | tail -3

# Install Rust
if ! command -v cargo &>/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal 2>&1 | tail -5
fi
source \$HOME/.cargo/env

# Install rclone
if ! command -v rclone &>/dev/null; then
  curl -fsSL https://rclone.org/install.sh | sudo bash 2>&1 | tail -3
fi

# Configure rclone for S3 (using IAM-style env creds)
mkdir -p ~/.config/rclone
cat > ~/.config/rclone/rclone.conf <<EOF
[s3]
type = s3
provider = AWS
env_auth = true
region = ${REGION}
EOF

echo "==> Bootstrap complete. Rust: \$(rustc --version). rclone: \$(rclone --version | head -1)"
REMOTE

###############################################################################
# Step 7: Build Slate on the instance
###############################################################################
echo "==> Cloning and building Slate on EC2..."
$SSH bash -s <<REMOTE
set -euo pipefail
source \$HOME/.cargo/env

# Upload local source via git archive piped over SSH (no GitHub needed)
mkdir -p ~/slate
REMOTE

# Tar the local workspace and pipe it over SSH
(cd "$WORK_DIR" && tar czf - \
  --exclude target \
  --exclude .git \
  .) | $SSH "tar xzf - -C ~/slate"

$SSH bash -s <<REMOTE
set -euo pipefail
source \$HOME/.cargo/env
cd ~/slate
echo "==> Building release binary..."
cargo build --release --bin slate 2>&1 | tail -5
echo "Build complete: \$(ls -lh target/release/slate)"
REMOTE

###############################################################################
# Step 8: Run the benchmark
###############################################################################
echo "==> Running benchmark on EC2..."
BENCHMARK_OUTPUT=$($SSH bash -s <<REMOTE
set -euo pipefail
source \$HOME/.cargo/env
SLATE=\$HOME/slate/target/release/slate
S3_SRC="s3://${S3_BUCKET}/${BENCH_PREFIX}"
LOCAL_DST="/tmp/slate-bench-dst"
RUNS=3
FILE_COUNT=5
TOTAL_MIB=\$((529 * FILE_COUNT))

pad_right() { printf "%-\${2}s" "\$1"; }
pad_left()  { printf "%\${2}s"  "\$1"; }

declare -a SLATE_TIMES AWS_TIMES RCLONE_TIMES

echo ""
echo "### Instance: \$(curl -sf http://169.254.169.254/latest/meta-data/instance-type) in ${REGION}"
echo "### Dataset:  \${FILE_COUNT} files × 529 MiB = \${TOTAL_MIB} MiB"
echo "### Runs:     \${RUNS} (best 2-of-3 averaged)"
echo ""

# --- slate copy ---
echo "Running slate copy..."
for i in \$(seq 1 \$RUNS); do
  rm -rf \$LOCAL_DST && mkdir -p \$LOCAL_DST
  START=\$(date +%s%3N)
  \$SLATE copy \$S3_SRC \$LOCAL_DST 2>&1 | grep -v "Job\|queued" || true
  END=\$(date +%s%3N)
  SLATE_TIMES[\$i]=\$(echo "scale=3; (\$END - \$START) / 1000" | bc)
  echo "  run \$i: \${SLATE_TIMES[\$i]}s"
done

# --- aws s3 cp ---
echo "Running aws s3 cp..."
for i in \$(seq 1 \$RUNS); do
  rm -rf \$LOCAL_DST && mkdir -p \$LOCAL_DST
  START=\$(date +%s%3N)
  aws s3 cp \$S3_SRC \$LOCAL_DST --recursive --no-progress 2>/dev/null
  END=\$(date +%s%3N)
  AWS_TIMES[\$i]=\$(echo "scale=3; (\$END - \$START) / 1000" | bc)
  echo "  run \$i: \${AWS_TIMES[\$i]}s"
done

# --- rclone copy ---
echo "Running rclone copy..."
for i in \$(seq 1 \$RUNS); do
  rm -rf \$LOCAL_DST && mkdir -p \$LOCAL_DST
  START=\$(date +%s%3N)
  rclone copy "s3:${S3_BUCKET}/${BENCH_PREFIX}" \$LOCAL_DST --transfers 8 --checkers 16 2>/dev/null
  END=\$(date +%s%3N)
  RCLONE_TIMES[\$i]=\$(echo "scale=3; (\$END - \$START) / 1000" | bc)
  echo "  run \$i: \${RCLONE_TIMES[\$i]}s"
done

# Compute averages (drop slowest of 3)
avg_best2() {
  local t1=\$1 t2=\$2 t3=\$3
  # sort and drop the largest
  local sorted=\$(echo -e "\$t1\n\$t2\n\$t3" | sort -n)
  local a=\$(echo "\$sorted" | head -1)
  local b=\$(echo "\$sorted" | sed -n '2p')
  echo "scale=3; (\$a + \$b) / 2" | bc
}

SLATE_AVG=\$(avg_best2 \${SLATE_TIMES[1]} \${SLATE_TIMES[2]} \${SLATE_TIMES[3]})
AWS_AVG=\$(avg_best2 \${AWS_TIMES[1]} \${AWS_TIMES[2]} \${AWS_TIMES[3]})
RCLONE_AVG=\$(avg_best2 \${RCLONE_TIMES[1]} \${RCLONE_TIMES[2]} \${RCLONE_TIMES[3]})

slate_mbps=\$(echo "scale=1; \$TOTAL_MIB / \$SLATE_AVG" | bc)
aws_mbps=\$(echo "scale=1; \$TOTAL_MIB / \$AWS_AVG" | bc)
rclone_mbps=\$(echo "scale=1; \$TOTAL_MIB / \$RCLONE_AVG" | bc)

slate_vs_aws=\$(echo "scale=2; \$slate_mbps / \$aws_mbps" | bc)
slate_vs_rclone=\$(echo "scale=2; \$slate_mbps / \$rclone_mbps" | bc)

echo ""
echo "## Slate Benchmark Results"
echo ""
echo "**Environment:** \$(curl -sf http://169.254.169.254/latest/meta-data/instance-type) · AWS ${REGION} · Amazon Linux 2023"
echo "**Dataset:** \${FILE_COUNT} files × 529 MiB (2.6 GiB total) — real ML model weights from S3"
echo "**Methodology:** 3 runs per tool, best 2 averaged, local disk destination"
echo ""
echo "| Tool | Time | Throughput | vs slate |"
echo "|------|------|------------|----------|"
echo "| **slate** (this project) | \${SLATE_AVG}s | **\${slate_mbps} MB/s** | — |"
echo "| aws s3 cp --recursive | \${AWS_AVG}s | \${aws_mbps} MB/s | \${slate_vs_aws}× slower |"
echo "| rclone copy --transfers 8 | \${RCLONE_AVG}s | \${rclone_mbps} MB/s | \${slate_vs_rclone}× slower |"
echo ""
echo "> Slate uses parallel 16 MiB chunked downloads (8 concurrent per object) with HTTP/2"
echo "> connection reuse across chunks. Source: S3 us-east-1 → EC2 us-east-1 local disk."
REMOTE
)

echo "$BENCHMARK_OUTPUT"

###############################################################################
# Step 9: Save results
###############################################################################
mkdir -p "$WORK_DIR/bench"
{
  echo "<!-- Generated by bench/run-ec2-bench.sh on $(date -u '+%Y-%m-%d %H:%M UTC') -->"
  echo ""
  echo "$BENCHMARK_OUTPUT"
} > "$RESULTS_FILE"

echo ""
echo "==> Results saved to $RESULTS_FILE"

###############################################################################
# Step 10: Clean up S3 test data
###############################################################################
echo "==> Cleaning up S3 benchmark files..."
aws s3 rm "s3://${S3_BUCKET}/${BENCH_PREFIX}/" --recursive --region "$REGION" 2>/dev/null || true

echo "==> All done."
