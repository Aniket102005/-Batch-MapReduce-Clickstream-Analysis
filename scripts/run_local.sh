#!/usr/bin/env bash
# End-to-end demo on ONE machine (no cluster, no HDFS): Hadoop "local" mode.
# Needs: Java 8+, Hadoop client on PATH (`hadoop version`), Maven (only to build the jar), Python 3.
#
# Usage: scripts/run_local.sh [input.csv] [run-label YYYY-MM-DD]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_CSV="${1:-${ROOT}/data/sample_clickstream.csv}"
RUN="${2:-$(date +%F)}"

if [ ! -f "$INPUT_CSV" ]; then
  echo "no input given - generating a sample file"
  mkdir -p "$(dirname "$INPUT_CSV")"
  python3 "${ROOT}/scripts/generate_sample_data.py" --out "$INPUT_CSV"
fi

JAR="${ROOT}/target/clickstream-mr.jar"
[ -f "$JAR" ] || (cd "$ROOT" && mvn -q -DskipTests package)

WORK="${ROOT}/work/local"
rm -rf "$WORK"
mkdir -p "$WORK/in"
ln -s "$(readlink -f "$INPUT_CSV")" "$WORK/in/$(basename "$INPUT_CSV")"

hadoop jar "$JAR" \
    -Dmapreduce.framework.name=local -Dfs.defaultFS=file:/// \
    all "$WORK/in" "$WORK/out"

REPORT="${ROOT}/reports/${RUN}"
mkdir -p "$REPORT"
cat "$WORK"/out/peak_hour/part-r-*   > "$REPORT/peak_hour.tsv"
cat "$WORK"/out/top_pages/part-r-*   > "$REPORT/top_pages.tsv"
cat "$WORK"/out/error_rate/part-r-*  > "$REPORT/error_rate.tsv"

python3 "${ROOT}/scripts/verify_results.py" --input "$INPUT_CSV" --reports "$REPORT"
python3 "${ROOT}/scripts/build_dashboard.py" --reports "${ROOT}/reports" --out "${ROOT}/dashboard/index.html"
echo "Open dashboard/index.html in a browser."
