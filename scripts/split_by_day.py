#!/usr/bin/env python3
"""Split one big clickstream CSV into one file per calendar day.

The Kaggle dataset ships as one file per MONTH (2019-Oct.csv, 2019-Nov.csv).
A nightly batch normally receives ONE DAY of logs, so this script simulates the
daily drops: it writes <outdir>/<YYYY-MM-DD>.csv (header repeated in each file).
Lines whose timestamp cannot be read go to <outdir>/unknown.csv (nothing is dropped).

Usage:
  python3 scripts/split_by_day.py data/kaggle/2019-Oct.csv data/daily
"""
import os
import re
import sys

DATE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]) ")


def main():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        sys.exit("Usage: split_by_day.py <src_csv> <outdir> [max_rows]")
    src, outdir = sys.argv[1], sys.argv[2]
    max_rows = int(sys.argv[3]) if len(sys.argv) == 4 else None

    os.makedirs(outdir, exist_ok=True)
    handles = {}
    counts = {}
    total_read = 0

    print(f"Splitting {src} -> {outdir} ...")
    with open(src, "r", encoding="utf-8", errors="replace", newline="") as fh:
        header = fh.readline()
        for line in fh:
            total_read += 1
            key = line[:10] if DATE.match(line) else "unknown"
            out = handles.get(key)
            if out is None:
                out = open(os.path.join(outdir, key + ".csv"), "w", encoding="utf-8", newline="")
                out.write(header)
                handles[key] = out
                counts[key] = 0
            out.write(line)
            counts[key] += 1

            if total_read % 1_000_000 == 0:
                print(f"  Processed {total_read:,} rows...")
            if max_rows and total_read >= max_rows:
                print(f"Reached limit of {max_rows:,} rows.")
                break

    for h in handles.values():
        h.close()

    print("\nSummary of daily partitions generated:")
    for k in sorted(counts):
        print(f"  {k}.csv: {counts[k]:,} rows")


if __name__ == "__main__":
    main()
