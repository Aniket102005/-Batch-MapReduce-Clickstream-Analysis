#!/usr/bin/env python3
"""Independent check of the MapReduce reports.

Re-computes peak hour, top-N pages and error rate with plain Python (single
process, no Hadoop) using the SAME validation rules as ClickEvent.java, then
compares with the report files produced by the cluster/local run.

Usage:
  python3 scripts/verify_results.py --input data/sample_clickstream.csv \
      --reports reports/2019-10-04
Exit code 0 = all reports match, 1 = mismatch.
"""
import argparse
import csv
import os
import re
import sys
from collections import Counter

TS = re.compile(r"^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}) UTC$")
DIGITS = re.compile(r"^\d+$")
PRICE = re.compile(r"^-?\d+(\.\d+)?$")
EVENTS = {"view", "cart", "remove_from_cart", "purchase"}


def parse(line):
    """Return (problem|None, date|None, hour|None, event, product, category, brand)."""
    f = line.rstrip("\r\n").split(",")
    if len(f) != 9:
        return "WRONG_FIELD_COUNT", None, None, None, None, None, None
    m = TS.match(f[0])
    date = hour = None
    if m:
        mo, d, hh, mi, ss = (int(m.group(i)) for i in (2, 3, 4, 5, 6))
        if 1 <= mo <= 12 and 1 <= d <= 31 and hh <= 23 and mi <= 59 and ss <= 59:
            date = "%s-%s-%s" % (m.group(1), m.group(2), m.group(3))
            hour = m.group(4)
    if date is None:
        return "BAD_TIMESTAMP", None, None, None, None, None, None
    if f[1] not in EVENTS:
        return "BAD_EVENT_TYPE", date, hour, f[1], None, None, None
    if not DIGITS.match(f[2]):
        return "BAD_PRODUCT_ID", date, hour, f[1], None, None, None
    if not DIGITS.match(f[7]):
        return "BAD_USER_ID", date, hour, f[1], None, None, None
    if not PRICE.match(f[6]) or float(f[6]) <= 0.0:
        return "BAD_PRICE", date, hour, f[1], None, None, None
    return None, date, hour, f[1], f[2], f[4] or "NA", f[5] or "NA"


def read_tsv(path):
    with open(path, newline="") as fh:
        return [line.rstrip("\n").split("\t") for line in fh if line.strip()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="raw clickstream CSV")
    ap.add_argument("--reports", required=True, help="dir with peak_hour.tsv, top_pages.tsv, error_rate.tsv")
    ap.add_argument("--topn", type=int, default=10)
    ap.add_argument("--top-event", default="view")
    args = ap.parse_args()

    hours = Counter()
    pages = Counter()
    day_total = Counter()
    day_err = Counter()
    reasons = Counter()

    with open(args.input, newline="") as fh:
        for line in fh:
            if not line.strip() or line.startswith("event_time"):
                continue
            problem, date, hour, event, pid, cat, brand = parse(line)
            day = date or "UNKNOWN"
            day_total[day] += 1
            if problem:
                day_err[day] += 1
                reasons[problem] += 1
                continue
            hours[hour] += 1
            if event == args.top_event:
                pages[(pid, cat, brand)] += 1

    ok = True

    def check(label, cond, detail=""):
        nonlocal ok
        print("[%s] %s %s" % ("PASS" if cond else "FAIL", label, detail))
        ok &= cond

    # --- peak hour
    rows = read_tsv(os.path.join(args.reports, "peak_hour.tsv"))
    got_hours = {r[0]: int(r[1]) for r in rows if r[0] != "PEAK_HOUR"}
    got_peak = next(r for r in rows if r[0] == "PEAK_HOUR")
    exp_peak_hour = min(hours, key=lambda h: (-hours[h], h))
    check("hourly counts", got_hours == dict(hours), "(%d hours)" % len(hours))
    check("peak hour", got_peak[1] == exp_peak_hour and int(got_peak[2]) == hours[exp_peak_hour],
          "(expected %s:00 with %d events)" % (exp_peak_hour, hours[exp_peak_hour]))

    # --- top pages
    rows = read_tsv(os.path.join(args.reports, "top_pages.tsv"))
    got_top = [(r[1], r[2], r[3], int(r[4])) for r in rows]
    best = sorted(pages.items(), key=lambda kv: (-kv[1], "\t".join(kv[0])))[:args.topn]
    exp_top = [(k[0], k[1], k[2], c) for k, c in best]
    check("top-%d pages" % args.topn, got_top == exp_top)

    # --- error rate
    rows = read_tsv(os.path.join(args.reports, "error_rate.tsv"))
    got_days = {r[0][4:]: (int(r[1]), int(r[2])) for r in rows if r[0].startswith("DAY|")}
    got_reasons = {r[0][7:]: int(r[2]) for r in rows if r[0].startswith("REASON|")}
    exp_days = {d: (day_total[d], day_err[d]) for d in day_total}
    check("daily totals/errors", got_days == exp_days, "(%d days)" % len(exp_days))
    check("error reasons", got_reasons == dict(reasons))
    tot, err = sum(day_total.values()), sum(day_err.values())
    print("overall: %d records, %d invalid -> %.4f%% error rate" % (tot, err, 100.0 * err / max(tot, 1)))

    print("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
