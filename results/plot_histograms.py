#!/usr/bin/env python3
"""Draw the latency histograms in a CSV from extract_histograms.py as an SVG.

Usage: plot_histograms.py histograms.csv [--css-vars] > histograms.svg

Two columns (Lakebase, RDS), one row per transaction type, sharing a log-scale
latency axis and, within a row, the y scale. Buckets are 1 ms wide in the log, so
they are regrouped into bins of roughly equal width on the log axis (1 ms bins
below 10 ms, then 10 per decade). A bin's height is its share of calls divided by
its width in tenths of a decade, so bins of different widths compare fairly.

--css-vars colours the SVG with CSS custom properties (for a themed HTML page);
without it, the SVG uses fixed light-theme colours (for the README).
"""
import csv
import math
import sys
from collections import defaultdict

RUNS = [("lakebase", "Lakebase (Free Edition, 1 CU)"), ("rds", "RDS PostgreSQL 17 (db.m7g.large)")]
TXNS = [("REPORT_QUOTA_USAGE", "ReportQuotaUsage"), ("ADD_CREDIT", "AddCredit")]

LIGHT = {"bg": "#fcfcfb", "fg": "#0b0b0b", "muted": "#52514e", "grid": "#e4e3df",
         "lakebase": "#2a78d6", "rds": "#eb6834"}


def color(name, css):
    return f"var(--{name})" if css else LIGHT[name]


def bin_edges():
    edges = [0.5] + list(range(1, 10))  # [0.5,1) holds the 0 ms bucket; then 1 ms bins
    steps = [10, 12, 15, 20, 25, 30, 40, 50, 60, 80]
    scale = 1
    while True:
        for s in steps:
            e = s * scale
            edges.append(min(e, 2000))
            if e >= 2000:
                return edges
        scale *= 10


def percentile(buckets, p):
    total = sum(buckets.values())
    seen = 0
    for ms in sorted(buckets):
        seen += buckets[ms]
        if seen >= total * p:
            return ms
    return max(buckets)


def main():
    path = sys.argv[1]
    css = "--css-vars" in sys.argv[2:]
    data = defaultdict(lambda: defaultdict(int))
    for r in csv.DictReader(open(path)):
        data[(r["run"], r["transaction"])][int(r["latency_ms"])] += int(r["calls"])

    edges = bin_edges()
    W, H = 920, 640
    ml, mr, mt, mb, gap_x, gap_y = 64, 16, 64, 56, 40, 70
    pw = (W - ml - mr - gap_x) / 2
    ph = (H - mt - mb - gap_y) / 2
    lo, hi = math.log10(0.5), math.log10(2000)

    def x(ms):
        return (math.log10(ms) - lo) / (hi - lo) * pw

    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
           f'font-family="Inter, -apple-system, Segoe UI, sans-serif" role="img" '
           f'aria-label="Latency histograms, Lakebase and RDS side by side">']
    if not css:
        out.append(f'<rect width="{W}" height="{H}" fill="{LIGHT["bg"]}"/>')
    fg, muted, grid = color("fg", css), color("muted", css), color("grid", css)

    for ci, (run, run_label) in enumerate(RUNS):
        cx = ml + ci * (pw + gap_x)
        out.append(f'<text x="{cx}" y="24" font-size="15" font-weight="600" fill="{fg}">'
                   f'<tspan fill="{color(run, css)}">■</tspan> {run_label}</text>')

    for ri, (txn, txn_label) in enumerate(TXNS):
        cy = mt + ri * (ph + gap_y)
        bins = {}
        for run, _ in RUNS:
            b = data[(run, txn)]
            total = sum(b.values())
            heights = []
            for i in range(len(edges) - 1):
                a, z = edges[i], edges[i + 1]
                lo_ms = 0 if i == 0 else a
                calls = sum(c for ms, c in b.items() if lo_ms <= ms < z or (z == 2000 and ms >= 1999 and a <= ms))
                width = math.log10(z / a) / 0.1
                heights.append((a, z, calls, 100 * calls / total / width if total else 0))
            bins[run] = (heights, total, b)
        ymax = max(h for run in bins for *_, h in bins[run][0])
        ymax = math.ceil(ymax / 5) * 5

        for ci, (run, _) in enumerate(RUNS):
            cx = ml + ci * (pw + gap_x)
            heights, total, b = bins[run]
            out.append(f'<g transform="translate({cx:.1f},{cy:.1f})">')
            for t in range(0, ymax + 1, 5 if ymax <= 30 else 10):
                yy = ph - t / ymax * ph
                out.append(f'<line x1="0" x2="{pw:.1f}" y1="{yy:.1f}" y2="{yy:.1f}" stroke="{grid}" stroke-width="1"/>')
                if ci == 0:
                    out.append(f'<text x="-8" y="{yy + 4:.1f}" font-size="11" text-anchor="end" fill="{muted}">{t}%</text>')
            for t in (1, 10, 100, 1000):
                xx = x(t)
                out.append(f'<line x1="{xx:.1f}" x2="{xx:.1f}" y1="0" y2="{ph:.1f}" stroke="{grid}" stroke-width="1"/>')
                out.append(f'<text x="{xx:.1f}" y="{ph + 16:.1f}" font-size="11" text-anchor="middle" fill="{muted}">'
                           f'{t:,} ms</text>')
            for a, z, calls, h in heights:
                if not calls:
                    continue
                x0, x1 = x(a) + 0.5, x(z) - 0.5
                hh = max(h / ymax * ph, 1)
                label = "&lt; 1 ms" if a == 0.5 else (f"{a:g} ms" if z - a == 1 else f"{a:g}–{z:g} ms")
                out.append(f'<rect x="{x0:.1f}" y="{ph - hh:.1f}" width="{max(x1 - x0, 1):.1f}" height="{hh:.1f}" '
                           f'rx="1" fill="{color(run, css)}"><title>{label}: {calls:,} calls '
                           f'({100 * calls / total:.2f}%)</title></rect>')
            out.append(f'<line x1="0" x2="{pw:.1f}" y1="{ph:.1f}" y2="{ph:.1f}" stroke="{muted}" stroke-width="1"/>')
            for p, name in ((0.5, "median"), (0.99, "p99")):
                v = percentile(b, p)
                xx = x(max(v, 0.5) + 0.5)
                out.append(f'<line x1="{xx:.1f}" x2="{xx:.1f}" y1="0" y2="{ph:.1f}" stroke="{fg}" '
                           f'stroke-width="1" stroke-dasharray="3 3"/>')
                if name == "median":
                    anchor = "end" if xx > 80 else "start"
                else:
                    anchor = "start" if xx < pw - 80 else "end"
                dx = 4 if anchor == "start" else -4
                out.append(f'<text x="{xx + dx:.1f}" y="{12 if name == "median" else 26}" font-size="11" '
                           f'text-anchor="{anchor}" fill="{fg}">{name} {v:,} ms</text>')
            out.append(f'<text x="0" y="-10" font-size="13" font-weight="600" fill="{fg}">{txn_label}'
                       f'<tspan font-weight="400" fill="{muted}">  {total:,} calls</tspan></text>')
            out.append('</g>')

    out.append(f'<text x="{ml}" y="{H - 14}" font-size="11" fill="{muted}">Latency (log scale). Bar height: share of '
               f'calls per tenth of a decade of latency, so bins of different widths compare fairly.</text>')
    out.append('</svg>')
    print("\n".join(out))


if __name__ == "__main__":
    main()
