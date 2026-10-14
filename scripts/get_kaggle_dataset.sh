#!/usr/bin/env bash
# Download the Kaggle dataset used by this project:
#   "eCommerce behavior data from multi category store"  (mkechinov)
#   https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
#
# Files: 2019-Oct.csv (~42 M rows) and 2019-Nov.csv (~67 M rows), several GB each; 9 columns:
#   event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
#
# One-time setup:
#   pip install kaggle
#   Kaggle -> Settings -> API -> "Create New Token"  -> save as ~/.kaggle/kaggle.json
#   chmod 600 ~/.kaggle/kaggle.json
#   (You may also have to open the dataset page once and accept its terms.)
#
# Usage: scripts/get_kaggle_dataset.sh [MAX_LINES]
#   MAX_LINES  optional: also write data/kaggle_sample.csv with only the first N lines of
#              2019-Oct.csv (handy for a laptop, e.g. 5000000)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="${ROOT}/data/kaggle"
mkdir -p "$DEST"

if ! command -v kaggle >/dev/null 2>&1; then
  echo "kaggle CLI not found. Run: pip install kaggle   (and put your API token in ~/.kaggle/kaggle.json)" >&2
  exit 1
fi

kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store -p "$DEST" --unzip
ls -lh "$DEST"

if [ "${1:-}" != "" ]; then
  head -n "$1" "$DEST/2019-Oct.csv" > "${ROOT}/data/kaggle_sample.csv"
  echo "wrote data/kaggle_sample.csv ($1 lines)"
fi
