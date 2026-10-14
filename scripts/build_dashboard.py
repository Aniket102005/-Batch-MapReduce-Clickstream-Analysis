#!/usr/bin/env python3
"""Build the batch report dashboard (one self-contained HTML file).

Reads every run folder under --reports (reports/<YYYY-MM-DD>/ containing
peak_hour.tsv, top_pages.tsv, error_rate.tsv) and writes dashboard/index.html
showing the latest run plus a history of previous nightly runs.
No server, no internet: open the HTML file in a browser.

Usage:
  python3 scripts/build_dashboard.py --reports reports --out dashboard/index.html
"""
import argparse
import json
import os
import re
from datetime import datetime

RUN_DIR = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def read_tsv(path):
    with open(path, newline="") as fh:
        return [line.rstrip("\n").split("\t") for line in fh if line.strip()]


def load_run(path, name):
    peak_rows = read_tsv(os.path.join(path, "peak_hour.tsv"))
    hours = [{"hour": r[0], "events": int(r[1])} for r in peak_rows if r[0] != "PEAK_HOUR"]
    peak = next((r for r in peak_rows if r[0] == "PEAK_HOUR"), None)

    top = [{"rank": int(r[0]), "product_id": r[1], "category": r[2], "brand": r[3], "views": int(r[4])}
           for r in read_tsv(os.path.join(path, "top_pages.tsv"))]

    days, reasons = [], []
    for r in read_tsv(os.path.join(path, "error_rate.tsv")):
        if r[0].startswith("DAY|"):
            days.append({"day": r[0][4:], "total": int(r[1]), "errors": int(r[2]),
                         "rate": None if r[3] == "NA" else float(r[3])})
        elif r[0].startswith("REASON|"):
            reasons.append({"reason": r[0][7:], "errors": int(r[2])})
    days.sort(key=lambda d: d["day"])
    reasons.sort(key=lambda d: -d["errors"])
    total = sum(d["total"] for d in days)
    errors = sum(d["errors"] for d in days)

    meta = {}
    meta_path = os.path.join(path, "run_info.json")
    if os.path.exists(meta_path):
        with open(meta_path) as fh:
            meta = json.load(fh)

    return {
        "run": name,
        "meta": meta,
        "total_events": total,
        "total_errors": errors,
        "error_rate": round(100.0 * errors / total, 4) if total else 0.0,
        "peak_hour": peak[1] if peak else None,
        "peak_events": int(peak[2]) if peak else None,
        "hours": hours,
        "top_pages": top,
        "days": days,
        "reasons": reasons,
    }


TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Clickstream Batch Reports</title>
<style>
  :root { --bg:#f6f7f9; --card:#fff; --ink:#1c2430; --mute:#667085; --line:#e4e7ec;
          --blue:#2f6fed; --peak:#e8590c; --bad:#d92d20; --good:#12805c; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#10141a; --card:#181e27; --ink:#e8ecf2; --mute:#98a2b3; --line:#2a3340;
            --blue:#6e9bff; --peak:#ff8a4c; --bad:#ff6b5e; --good:#4cc9a0; } }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--ink);
         font:15px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif; }
  header { padding:20px 24px 4px; display:flex; flex-wrap:wrap; gap:12px; align-items:baseline; }
  h1 { font-size:20px; margin:0; }
  .sub { color:var(--mute); font-size:13px; }
  main { padding:12px 24px 40px; max-width:1180px; margin:0 auto; }
  .kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:12px; margin:12px 0; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:16px; }
  .kpi .l { color:var(--mute); font-size:12px; text-transform:uppercase; letter-spacing:.04em; }
  .kpi .v { font-size:26px; font-weight:650; margin-top:2px; }
  .kpi .d { color:var(--mute); font-size:12px; }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
  @media (max-width:860px) { .grid { grid-template-columns:1fr; } }
  h2 { font-size:15px; margin:0 0 10px; }
  svg { width:100%; height:auto; display:block; }
  svg text { fill:var(--mute); font-size:11px; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); }
  th { color:var(--mute); font-weight:600; font-size:12px; }
  td.n, th.n { text-align:right; font-variant-numeric:tabular-nums; }
  select { background:var(--card); color:var(--ink); border:1px solid var(--line); border-radius:6px; padding:4px 8px; }
  .note { color:var(--mute); font-size:12px; margin-top:8px; }
</style>
</head>
<body>
<header>
  <h1>Clickstream Batch Reports</h1>
  <span class="sub" id="sub"></span>
  <span style="flex:1"></span>
  <label class="sub">Run <select id="runSel"></select></label>
</header>
<main>
  <section class="kpis" id="kpis"></section>
  <section class="grid">
    <div class="card"><h2>Traffic by hour of day (UTC)</h2><div id="hours"></div>
      <div class="note">Highlighted bar = peak-traffic hour.</div></div>
    <div class="card"><h2>Top 10 pages (most viewed products)</h2><div id="top"></div></div>
    <div class="card"><h2>Daily error rate (%)</h2><div id="errs"></div>
      <div class="note">Error = malformed or invalid log record (see README for the rules).</div></div>
    <div class="card"><h2>Errors by reason</h2><div id="reasons"></div></div>
  </section>
  <section class="card" style="margin-top:12px"><h2>Nightly run history</h2><div id="hist"></div></section>
</main>
<script>
const RUNS = __DATA__;
const fmt = n => n.toLocaleString("en-US");
const esc = s => String(s).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const NS = "http://www.w3.org/2000/svg";

function svg(w, h, inner) { return `<svg viewBox="0 0 ${w} ${h}" role="img">${inner}</svg>`; }

function barChart(el, items, peakKey) {
  const W = 560, H = 230, L = 44, B = 24, T = 8, R = 6;
  const max = Math.max(...items.map(i => i.v), 1);
  const bw = (W - L - R) / items.length;
  let s = "";
  for (let k = 0; k <= 4; k++) {
    const y = T + (H - T - B) * (1 - k / 4);
    s += `<line x1="${L}" x2="${W-R}" y1="${y}" y2="${y}" stroke="${css('--line')}"/>`;
    s += `<text x="${L-6}" y="${y+4}" text-anchor="end">${fmt(Math.round(max*k/4))}</text>`;
  }
  items.forEach((it, i) => {
    const h = (H - T - B) * it.v / max, x = L + i * bw + 1.5, y = H - B - h;
    const col = it.k === peakKey ? css('--peak') : css('--blue');
    s += `<rect x="${x}" y="${y}" width="${Math.max(bw-3,1)}" height="${h}" rx="2" fill="${col}"><title>${esc(it.k)}:00 - ${fmt(it.v)} events</title></rect>`;
    if (i % 3 === 0) s += `<text x="${x+bw/2-1.5}" y="${H-8}" text-anchor="middle">${esc(it.k)}</text>`;
  });
  el.innerHTML = svg(W, H, s);
}

function hbars(el, rows) {
  const W = 560, rowH = 26, L = 190, R = 70, H = rows.length * rowH + 6;
  const max = Math.max(...rows.map(r => r.v), 1);
  let s = "";
  rows.forEach((r, i) => {
    const y = 3 + i * rowH, w = (W - L - R) * r.v / max;
    s += `<text x="${L-8}" y="${y+16}" text-anchor="end">${esc(r.label)}</text>`;
    s += `<rect x="${L}" y="${y+3}" width="${Math.max(w,1)}" height="${rowH-9}" rx="3" fill="${css('--blue')}"><title>${esc(r.tip||r.label)}</title></rect>`;
    s += `<text x="${L+w+6}" y="${y+16}">${fmt(r.v)}</text>`;
  });
  el.innerHTML = svg(W, H, s);
}

function lineChart(el, pts) {
  const W = 560, H = 210, L = 44, B = 28, T = 10, R = 14;
  if (!pts.length) { el.textContent = "No data"; return; }
  const max = Math.max(...pts.map(p => p.v), 0.01) * 1.15;
  const X = i => L + (pts.length === 1 ? (W-L-R)/2 : (W-L-R) * i / (pts.length-1));
  const Y = v => T + (H-T-B) * (1 - v / max);
  let s = "";
  for (let k = 0; k <= 4; k++) {
    const y = T + (H-T-B) * (1 - k/4);
    s += `<line x1="${L}" x2="${W-R}" y1="${y}" y2="${y}" stroke="${css('--line')}"/>`;
    s += `<text x="${L-6}" y="${y+4}" text-anchor="end">${(max*k/4).toFixed(2)}</text>`;
  }
  s += `<polyline fill="none" stroke="${css('--bad')}" stroke-width="2" points="${pts.map((p,i)=>X(i)+','+Y(p.v)).join(' ')}"/>`;
  pts.forEach((p, i) => {
    s += `<circle cx="${X(i)}" cy="${Y(p.v)}" r="3.5" fill="${css('--bad')}"><title>${esc(p.k)}: ${p.v.toFixed(3)}%</title></circle>`;
    s += `<text x="${X(i)}" y="${H-8}" text-anchor="middle">${esc(p.k.slice(5))}</text>`;
  });
  el.innerHTML = svg(W, H, s);
}

function table(el, head, rows) {
  el.innerHTML = "<table><thead><tr>" + head.map(h => `<th class="${h.n?'n':''}">${esc(h.t)}</th>`).join("") +
    "</tr></thead><tbody>" + rows.map(r => "<tr>" + r.map((c, i) => `<td class="${head[i].n?'n':''}">${c}</td>`).join("") + "</tr>").join("") + "</tbody></table>";
}

function render(run) {
  const r = RUNS.find(x => x.run === run);
  const peak = r.peak_hour;
  document.getElementById("sub").textContent =
    "Batch run " + r.run + (r.meta.input ? " | input: " + r.meta.input : "") +
    (r.meta.finished_at ? " | finished " + r.meta.finished_at : "");
  const top1 = r.top_pages[0];
  document.getElementById("kpis").innerHTML = [
    ["Events processed", fmt(r.total_events), "valid + invalid records"],
    ["Peak-traffic hour", peak ? peak + ":00-" + peak + ":59" : "n/a", peak ? fmt(r.peak_events) + " events" : ""],
    ["Top page", top1 ? "Product " + esc(top1.product_id) : "n/a", top1 ? fmt(top1.views) + " views" : ""],
    ["Error rate", r.error_rate.toFixed(3) + "%", fmt(r.total_errors) + " invalid records"],
  ].map(k => `<div class="card kpi"><div class="l">${k[0]}</div><div class="v">${k[1]}</div><div class="d">${k[2]}</div></div>`).join("");

  barChart(document.getElementById("hours"), r.hours.map(h => ({k: h.hour, v: h.events})), peak);
  hbars(document.getElementById("top"), r.top_pages.map(p => ({
    label: "#" + p.rank + "  " + p.product_id + "  " + (p.category.split(".").pop() || p.category),
    v: p.views, tip: p.category + " / " + p.brand })));
  lineChart(document.getElementById("errs"), r.days.filter(d => d.day !== "UNKNOWN" && d.rate !== null).map(d => ({k: d.day, v: d.rate})));
  table(document.getElementById("reasons"), [{t: "Reason"}, {t: "Records", n: 1}, {t: "Share", n: 1}],
    r.reasons.map(x => [esc(x.reason), fmt(x.errors), (100 * x.errors / Math.max(r.total_errors, 1)).toFixed(1) + "%"]));
}

function renderHistory() {
  const rows = RUNS.slice().reverse().map(r => [esc(r.run), fmt(r.total_events),
    r.peak_hour ? r.peak_hour + ":00" : "-", esc(r.top_pages[0] ? r.top_pages[0].product_id : "-"),
    r.error_rate.toFixed(3) + "%", esc(r.meta.duration_sec != null ? r.meta.duration_sec + " s" : "-")]);
  table(document.getElementById("hist"),
    [{t: "Run"}, {t: "Events", n: 1}, {t: "Peak hour"}, {t: "#1 product"}, {t: "Error rate", n: 1}, {t: "Batch time", n: 1}], rows);
}

const sel = document.getElementById("runSel");
RUNS.slice().reverse().forEach(r => { const o = document.createElement("option"); o.value = o.textContent = r.run; sel.appendChild(o); });
sel.onchange = () => render(sel.value);
render(RUNS[RUNS.length - 1].run);
renderHistory();
matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => render(sel.value));
</script>
</body>
</html>
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reports", default="reports")
    ap.add_argument("--out", default="dashboard/index.html")
    args = ap.parse_args()

    runs = []
    for name in sorted(os.listdir(args.reports)):
        path = os.path.join(args.reports, name)
        if RUN_DIR.match(name) and os.path.isfile(os.path.join(path, "peak_hour.tsv")):
            runs.append(load_run(path, name))
    if not runs:
        raise SystemExit("no report folders found under " + args.reports)

    data = json.dumps(runs).replace("</", "<\\/")
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as fh:
        fh.write(TEMPLATE.replace("__DATA__", data))
    print("dashboard written: %s (%d runs, latest %s, generated %s)" % (
        args.out, len(runs), runs[-1]["run"], datetime.now().strftime("%Y-%m-%d %H:%M")))


if __name__ == "__main__":
    main()
