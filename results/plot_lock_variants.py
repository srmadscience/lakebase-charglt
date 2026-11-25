#!/usr/bin/env python3
"""Draw latency by percentile for the lock variants tested by bench/lock-ab.sh, as an SVG.

Usage: plot_lock_variants.py <directory> > latency-by-variant.svg

One panel per user count, one line per variant, all repetitions pooled. It plots the time
in the client's retry loop: every attempt and the sleeps between them, without the client's
queue, and including abandoned calls, so that the variants compare fairly. Also prints
a Markdown table of the same numbers to stderr.
"""
import glob
import math
import re
import sys
from collections import defaultdict

LINE = re.compile(r"^(?:>= )?(\d+)\t([\d.]+)\tTime in retry loop$")
VARIANTS = [("wait", "FOR UPDATE", "#2a78d6"), ("nowait", "FOR UPDATE NOWAIT", "#eb6834"),
            ("timeout", "FOR UPDATE, lock_timeout", "#1baf7a")]
BG, FG, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
XTICKS = [(0, "0"), (.5, "50%"), (.9, "90%"), (.99, "99%"), (.999, "99.9%")]
W, H, ML, MR, MT, MB, GAPX, GAPY = 920, 680, 64, 28, 70, 70, 64, 78


def percentile(hist, q):
    total, seen = sum(hist.values()), 0
    for ms in sorted(hist):
        seen += hist[ms]
        if seen >= q * total:
            return ms
    return max(hist)


def nice_step(top):
    for step in (10, 20, 25, 50, 100, 200, 250, 500, 1000, 2000):
        if top / step <= 5:
            return step
    return 5000


def main():
    data = defaultdict(lambda: defaultdict(float))
    for path in glob.glob(f"{sys.argv[1]}/*-users-rep*.log"):
        m = re.search(r"/(\w+)-(\d+)-users-rep", path)
        hist = data[(int(m[2]), m[1])]
        for line in open(path, errors="replace"):
            b = LINE.match(line.rstrip("\n"))
            if b:
                hist[int(b[1])] += float(b[2])
    users = sorted({u for u, _ in data}, reverse=True)
    pw, ph = (W - ML - MR - GAPX) / 2, (H - MT - MB - GAPY) / 2
    qs = [1 - 10 ** (-i / 40) for i in range(0, 121)]  # 0 to 99.9%, even steps of the x axis

    def x(q):
        return -math.log10(1 - q) / 3 * pw

    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="Inter, -apple-system, '
           f'Segoe UI, sans-serif" role="img" aria-label="Latency by percentile for three ways of locking a row">',
           f'<rect width="{W}" height="{H}" fill="{BG}"/>']
    lx = ML
    for name, label, col in VARIANTS:
        out.append(f'<line x1="{lx}" x2="{lx + 22}" y1="20" y2="20" stroke="{col}" stroke-width="2" stroke-linecap="round"/>'
                   f'<text x="{lx + 28}" y="24" font-size="13" fill="{FG}">{label}</text>')
        lx += 28 + len(label) * 8.4 + 30
    print("| Users | Variant | Calls | p50 | p95 | p99 | p99.9 |\n|---|---|---|---|---|---|---|", file=sys.stderr)

    for i, u in enumerate(users):
        cx, cy = ML + (i % 2) * (pw + GAPX), MT + (i // 2) * (ph + GAPY)
        hists = {v: data[(u, v)] for v, _, _ in VARIANTS if data[(u, v)]}
        top = max(percentile(h, .999) for h in hists.values())
        step = nice_step(top)
        ymax = math.ceil(top / step) * step
        out.append(f'<g transform="translate({cx:.1f},{cy:.1f})">')
        out.append(f'<text x="0" y="-12" font-size="13" font-weight="600" fill="{FG}">{u:,} users</text>')
        for t in range(0, ymax + 1, step):
            yy = ph - t / ymax * ph
            out.append(f'<line x1="0" x2="{pw:.1f}" y1="{yy:.1f}" y2="{yy:.1f}" stroke="{GRID}"/>'
                       f'<text x="-8" y="{yy + 4:.1f}" font-size="11" text-anchor="end" fill="{MUTED}">{t:,}</text>')
        for q, name in XTICKS:
            out.append(f'<text x="{x(q):.1f}" y="{ph + 16:.1f}" font-size="11" text-anchor="middle" fill="{MUTED}">{name}</text>')
        for v, label, col in VARIANTS:
            if v not in hists:
                continue
            h = hists[v]
            pts = " ".join(f"{x(q):.1f},{ph - percentile(h, q) / ymax * ph:.1f}" for q in qs)
            p50, p95, p99, p999 = (percentile(h, q) for q in (.5, .95, .99, .999))
            out.append(f'<polyline points="{pts}" fill="none" stroke="{col}" stroke-width="2" stroke-linejoin="round" '
                       f'stroke-linecap="round"><title>{label}, {u:,} users: median {p50:,} ms, p95 {p95:,} ms, '
                       f'p99 {p99:,} ms, p99.9 {p999:,} ms</title></polyline>')
            print(f"| {u:,} | {v} | {sum(h.values()):,.0f} | {p50:,} | {p95:,} | {p99:,} | {p999:,} |", file=sys.stderr)
        out.append(f'<line x1="0" x2="{pw:.1f}" y1="{ph:.1f}" y2="{ph:.1f}" stroke="{MUTED}"/></g>')
    out.append(f'<text x="{ML}" y="{H - 28}" font-size="11" fill="{MUTED}">Milliseconds in the retry loop (attempts and sleeps, '
               f'without the client\'s queue), by percentile of calls. Each panel has its own y scale.</text>'
               f'<text x="{ML}" y="{H - 12}" font-size="11" fill="{MUTED}">Abandoned calls are included. The three repetitions are '
               f'pooled. Lakebase, 1 CU, 150 workers, 100 ms lock_timeout, 2026-11-24.</text></svg>')
    print("\n".join(out))


main()
