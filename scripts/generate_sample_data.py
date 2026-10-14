#!/usr/bin/env python3
"""Generate a SAMPLE clickstream CSV with the same schema as the Kaggle dataset
"eCommerce behavior data from multi category store" (mkechinov).

Use it when you cannot download the multi-GB Kaggle files (offline lab, quick
demo, unit-style testing). It produces:
  * a diurnal traffic pattern (evening peak),
  * Zipf-distributed product popularity (a few hot pages),
  * view -> cart -> purchase funnels per session,
  * a small fraction of deliberately corrupted rows so the error-rate report
    has something to show.

Usage:
  python3 scripts/generate_sample_data.py --rows 500000 --days 3 \
      --out data/sample_clickstream.csv
"""
import argparse
import bisect
import csv
import itertools
import math
import random
from datetime import datetime, timedelta

HEADER = ["event_time", "event_type", "product_id", "category_id",
          "category_code", "brand", "price", "user_id", "user_session"]

CATEGORIES = [
    "electronics.smartphone", "electronics.audio.headphone", "electronics.video.tv",
    "electronics.clocks", "computers.notebook", "computers.desktop",
    "computers.peripherals.mouse", "appliances.kitchen.refrigerator",
    "appliances.kitchen.washer", "appliances.environment.vacuum",
    "apparel.shoes", "apparel.shoes.keds", "apparel.tshirt", "apparel.jacket",
    "furniture.living_room.sofa", "furniture.bedroom.bed", "kids.toys",
    "kids.carriage", "sport.bicycle", "sport.trainer", "auto.accessories.player",
    "construction.tools.drill", "accessories.bag", "stationery.cartrige", "",
]
BRANDS = ["samsung", "apple", "xiaomi", "huawei", "lg", "sony", "bosch", "philips",
          "asus", "acer", "lenovo", "hp", "nike", "adidas", "puma", "indesit",
          "artel", "oppo", "canon", "dell", "jbl", "logitech", "tefal", "casio",
          "", "", ""]  # '' = missing brand, like the real data


def hour_weights():
    """Relative traffic per hour of day (UTC): night trough, evening peak."""
    w = []
    for h in range(24):
        base = 0.35 + 0.65 * (1 + math.sin((h - 9) / 24 * 2 * math.pi)) / 2
        base += 0.9 * math.exp(-((h - 19) ** 2) / 8.0)   # sharp evening peak
        w.append(base)
    return w


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500_000, help="approximate number of valid rows")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--start", default="2019-10-01")
    ap.add_argument("--products", type=int, default=3000)
    ap.add_argument("--error-rate", type=float, default=0.015,
                    help="fraction of extra corrupted rows to inject")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data/sample_clickstream.csv")
    ap.add_argument("--no-header", action="store_true")
    args = ap.parse_args()

    rnd = random.Random(args.seed)
    start = datetime.strptime(args.start, "%Y-%m-%d")

    # product catalogue
    products = []
    for i in range(args.products):
        pid = 1_000_000 + i * 7
        cat_code = rnd.choice(CATEGORIES)
        products.append({
            "pid": pid,
            "cid": 2_053_000_000_000_000_000 + (CATEGORIES.index(cat_code) + 1) * 1_000_003,
            "cat": cat_code,
            "brand": rnd.choice(BRANDS),
            "price": round(math.exp(rnd.gauss(4.8, 1.0)), 2) + 1.0,
        })
    # Zipf popularity -> cumulative weights for fast sampling
    cum = list(itertools.accumulate(1.0 / (r + 1) ** 1.05 for r in range(args.products)))
    total_w = cum[-1]

    def pick_product():
        return products[bisect.bisect_left(cum, rnd.random() * total_w)]

    hw = hour_weights()
    hcum = list(itertools.accumulate(hw))

    def pick_start():
        day = rnd.randrange(args.days)
        hour = bisect.bisect_left(hcum, rnd.random() * hcum[-1])
        return start + timedelta(days=day, hours=hour,
                                 minutes=rnd.randrange(60), seconds=rnd.randrange(60))

    rows = []
    valid = 0
    session_no = 0
    while valid < args.rows:
        session_no += 1
        user = 500_000_000 + rnd.randrange(400_000)
        sess = "%08x-%04x-%04x-%04x-%012x" % (
            rnd.getrandbits(32), rnd.getrandbits(16), rnd.getrandbits(16),
            rnd.getrandbits(16), rnd.getrandbits(48))
        t = pick_start()
        n_views = 1 + int(rnd.expovariate(1 / 3.0))
        carted = []
        for _ in range(n_views):
            p = pick_product()
            t += timedelta(seconds=rnd.randrange(3, 90))
            rows.append([t.strftime("%Y-%m-%d %H:%M:%S UTC"), "view", p["pid"], p["cid"],
                         p["cat"], p["brand"], "%.2f" % p["price"], user, sess])
            valid += 1
            if rnd.random() < 0.10:
                t += timedelta(seconds=rnd.randrange(5, 60))
                rows.append([t.strftime("%Y-%m-%d %H:%M:%S UTC"), "cart", p["pid"], p["cid"],
                             p["cat"], p["brand"], "%.2f" % p["price"], user, sess])
                valid += 1
                carted.append(p)
        for p in carted:
            r = rnd.random()
            t += timedelta(seconds=rnd.randrange(20, 600))
            if r < 0.30:
                kind = "purchase"
            elif r < 0.40:
                kind = "remove_from_cart"
            else:
                continue
            rows.append([t.strftime("%Y-%m-%d %H:%M:%S UTC"), kind, p["pid"], p["cid"],
                         p["cat"], p["brand"], "%.2f" % p["price"], user, sess])
            valid += 1

    rows.sort(key=lambda r: r[0])

    # inject corrupted rows
    originals = list(rows)                    # corrupt copies of clean rows only
    n_bad = int(len(originals) * args.error_rate)
    for _ in range(n_bad):
        r = list(rnd.choice(originals))
        kind = rnd.randrange(6)
        if kind == 0:
            r[0] = rnd.choice(["2019-13-45 99:00:00 UTC", "not-a-date", ""])
        elif kind == 1:
            r[1] = rnd.choice(["click", "", "VIEW"])
        elif kind == 2:
            r[2] = ""
        elif kind == 3:
            r[7] = ""
        elif kind == 4:
            r[6] = rnd.choice(["-5.00", "0", "abc"])
        else:
            r = r[:rnd.randrange(3, 8)]          # truncated line
        rows.insert(rnd.randrange(len(rows)), r)

    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        if not args.no_header:
            w.writerow(HEADER)
        w.writerows(rows)
    print("wrote %d lines (%d injected bad rows) -> %s" % (len(rows), n_bad, args.out))


if __name__ == "__main__":
    main()
