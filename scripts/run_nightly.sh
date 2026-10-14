#!/usr/bin/env bash
# Nightly batch: HDFS partition -> 3 MapReduce reports on YARN -> local report files -> dashboard.
#
# Usage:
#   scripts/run_nightly.sh                  # processes YESTERDAY's partition (cron at 01:30)
#   scripts/run_nightly.sh 2019-10-01       # re-run / back-fill a specific day
#   HDFS_INPUT=/clickstream/raw scripts/run_nightly.sh 2019-10  # custom input path (e.g. whole Kaggle month)
#
# Exit codes: 0 ok | 2 jar missing | 3 no input | other = job/collect failure
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT}/config/pipeline.env"

RUN_DATE="${1:-$(date -d yesterday +%F)}"
log() { echo "$(date '+%F %T') [nightly] $*"; }

mkdir -p "${ROOT}/logs"

# never let two nightly runs overlap (a slow run + the next cron tick)
exec 9>"${ROOT}/.nightly.lock"
if ! flock -n 9; then
  log "previous run is still active - skipping"
  exit 0
fi

JAR_PATH="$JAR"
case "$JAR_PATH" in /*) ;; *) JAR_PATH="${ROOT}/${JAR_PATH}" ;; esac
if [ ! -f "$JAR_PATH" ]; then
  log "jar not found: $JAR_PATH  (build it with: mvn -q -DskipTests package)"
  exit 2
fi

INPUT="${HDFS_INPUT:-${HDFS_RAW_DIR}/dt=${RUN_DATE}}"
OUTBASE="${HDFS_OUT_DIR}/dt=${RUN_DATE}"
REPORT="${ROOT}/${REPORT_DIR}/${RUN_DATE}"
STARTED="$(date '+%F %T')"
T0=$SECONDS

log "run=${RUN_DATE} input=${INPUT} queue=${YARN_QUEUE} reducers=${REDUCERS}"

# 1) make sure the day's logs are in HDFS
if ! ${HDFS_BIN} dfs -test -d "$INPUT"; then
  if compgen -G "${LANDING_DIR}/*.csv" > /dev/null; then
    log "partition missing in HDFS - ingesting from ${LANDING_DIR}"
    "${ROOT}/scripts/hdfs_ingest.sh" "$LANDING_DIR" "$RUN_DATE"
  else
    log "no input: ${INPUT} not in HDFS and nothing in ${LANDING_DIR}"
    exit 3
  fi
fi

# 2) run the three reports (the driver submits them to YARN concurrently)
log "submitting MapReduce jobs to YARN"
${HADOOP_BIN} jar "$JAR_PATH" \
    -Dmapreduce.job.queuename="${YARN_QUEUE}" \
    -Dmapreduce.job.reduces="${REDUCERS}" \
    -Dclickstream.topn="${TOP_N}" \
    all "$INPUT" "$OUTBASE"

# 3) pull the (small) report outputs out of HDFS
log "collecting reports -> ${REPORT}"
mkdir -p "$REPORT"
for r in peak_hour top_pages error_rate; do
  rm -f "${REPORT}/${r}.tsv"
  ${HDFS_BIN} dfs -getmerge "${OUTBASE}/${r}" "${REPORT}/${r}.tsv"
  if [ ! -s "${REPORT}/${r}.tsv" ]; then
    log "ERROR: report ${r} is empty"
    exit 4
  fi
done

DURATION=$((SECONDS - T0))
cat > "${REPORT}/run_info.json" <<EOF
{"run": "${RUN_DATE}", "input": "${INPUT}", "output": "${OUTBASE}", "queue": "${YARN_QUEUE}",
 "started_at": "${STARTED}", "finished_at": "$(date '+%F %T')", "duration_sec": ${DURATION}}
EOF

# 4) refresh the dashboard
if command -v python3 >/dev/null 2>&1; then
  python3 "${ROOT}/scripts/build_dashboard.py" --reports "${ROOT}/${REPORT_DIR}" --out "${ROOT}/dashboard/index.html"
else
  log "python3 not available here (e.g. inside the Hadoop container) - run on your own computer:"
  log "    python3 scripts/build_dashboard.py --reports reports --out dashboard/index.html"
fi
log "done in ${DURATION}s"
