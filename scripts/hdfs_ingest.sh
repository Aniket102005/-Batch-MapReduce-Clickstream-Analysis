#!/usr/bin/env bash
# Upload one day's raw clickstream CSV file(s) into the HDFS raw zone.
#
# Usage: scripts/hdfs_ingest.sh <local_file_or_dir> <YYYY-MM-DD>
#   scripts/hdfs_ingest.sh /data/clickstream/landing 2019-10-01
#
# Result: hdfs://$HDFS_RAW_DIR/dt=2019-10-01/*.csv   (raw logs are never modified)
# When a directory is given, uploaded files are moved to <dir>/processed/<date>/.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT}/config/pipeline.env"

SRC="${1:?usage: hdfs_ingest.sh <local_file_or_dir> <YYYY-MM-DD>}"
DT="${2:?usage: hdfs_ingest.sh <local_file_or_dir> <YYYY-MM-DD>}"
DEST="${HDFS_RAW_DIR}/dt=${DT}"

if [ -d "$SRC" ]; then
  shopt -s nullglob
  files=("$SRC"/*.csv)
  shopt -u nullglob
else
  files=("$SRC")
fi
if [ "${#files[@]}" -eq 0 ] || [ ! -e "${files[0]}" ]; then
  echo "no CSV files found in $SRC" >&2
  exit 3
fi

${HDFS_BIN} dfs -mkdir -p "$DEST"
for f in "${files[@]}"; do
  echo "uploading $f -> $DEST/"
  ${HDFS_BIN} dfs -D dfs.blocksize="${HDFS_BLOCK_SIZE}" -D dfs.replication="${HDFS_REPLICATION}" \
      -put -f "$f" "$DEST/"
done
${HDFS_BIN} dfs -ls -h "$DEST"

if [ -d "$SRC" ]; then
  mkdir -p "$SRC/processed/$DT"
  mv "${files[@]}" "$SRC/processed/$DT/"
fi
