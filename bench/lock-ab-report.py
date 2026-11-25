#!/usr/bin/env python3
"""Summarise the logs written by lock-ab.sh.

Usage: lock-ab-report.py <directory> [baseline variant]

Writes <directory>/runs.csv, one line per run, and prints tables for each user count.
"Change" compares each run with the baseline (default wait) run of the same repetition.
"""
import csv
import glob
import os
import re
import statistics as st
import sys

BUCKET = re.compile(r"^(>= )?(\d+)\t([\d.]+)\t(REPORT_QUOTA_USAGE|ADD_CREDIT|Time spent in request queue|"
                    r"Time spent waiting to join request queue|Attempts per call|Time in retry loop|"
                    r"Latency of abandoned call)$")
QUEUED, JOIN = "Time spent in request queue", "Time spent waiting to join request queue"
ATTEMPTS, LOOP, ABANDONED = "Attempts per call", "Time in retry loop", "Latency of abandoned call"
CALL_MAX = 1999  # ADD_CREDIT and REPORT_QUOTA_USAGE put anything longer in this bucket


def percentile(hist, q):
    total, seen = sum(hist.values()), 0
    for ms in sorted(hist):
        seen += hist[ms]
        if seen >= q * total:
            return ms
    return 0


def number(text, pattern):
    m = re.search(pattern, text)
    return int(m[1]) if m else 0


def read_run(path):
    text = open(path, errors="replace").read()
    head = re.match(r"AB (.*)", text)
    if not head or "TPS = " not in text:
        print(f"ignoring {path}: no result", file=sys.stderr)
        return None
    run = dict(kv.split("=", 1) for kv in head[1].split())
    hists = {}
    for line in text.splitlines():
        m = BUCKET.match(line)
        if m:
            h = hists.setdefault(m[4], {})
            h[int(m[2])] = h.get(int(m[2]), 0) + float(m[3])
    sent = number(text, r"Processed (\d+) transactions")
    ms = number(text, r"transactions in (\d+) milliseconds")
    locked = number(text, r"user's row was locked = (\d+)")
    gave_up = number(text, r"row stayed locked = (\d+)")
    report = hists.get("REPORT_QUOTA_USAGE", {})
    attempts, loop, abandoned = (hists.get(k, {}) for k in (ATTEMPTS, LOOP, ABANDONED))
    # Every call, including the abandoned ones that the per-transaction histograms leave out
    calls = {}
    for k in ("REPORT_QUOTA_USAGE", "ADD_CREDIT", ABANDONED):
        for b, n in hists.get(k, {}).items():
            calls[min(b, CALL_MAX)] = calls.get(min(b, CALL_MAX), 0) + n
    count = lambda h: int(sum(h.values()))
    total = lambda h: sum(k * v for k, v in h.items())
    mean = lambda h: total(h) / max(1, count(h))

    # The histograms and the counters are kept separately, so they should agree
    problems = [f"{what}: {a:,} but expected {b:,}" for what, a, b in (
        ("calls in the attempts histogram", count(attempts), sent),
        ("attempts", int(total(attempts)), sent + locked - gave_up),
        ("calls in the retry loop histogram", count(loop), sent),
        ("abandoned calls in the histogram", count(abandoned), gave_up),
        ("calls in all latency histograms", count(calls), sent)) if a != b]
    waits = [l.rstrip("\n").split("|") for l in open(path[:-4] + ".locks")] if os.path.exists(path[:-4] + ".locks") else []
    waits = [w for w in waits if len(w) >= 5 and int(w[4]) > 0]
    return {
        "variant": run["variant"], "users": int(run["users"]), "rep": int(run["rep"]),
        "position": int(run["position"]), "lock_timeout_ms": run["lock_timeout_ms"], "db_mb": run["db_mb"],
        "sent": sent, "tps": round((sent - gave_up) / (ms / 1000), 1),
        "p50": percentile(report, .5), "p95": percentile(report, .95), "p99": percentile(report, .99),
        "all_p50": percentile(calls, .5), "all_p95": percentile(calls, .95), "all_p99": percentile(calls, .99),
        "loop_mean": round(mean(loop), 1), "loop_p50": percentile(loop, .5),
        "loop_p95": percentile(loop, .95), "loop_p99": percentile(loop, .99),
        "attempts_mean": round(mean(attempts), 3),
        "first_try_pct": round(100 * attempts.get(1, 0) / max(1, count(attempts)), 2),
        "attempts_p99": percentile(attempts, .99), "attempts_max": max(attempts, default=0),
        "abandoned_p50": percentile(abandoned, .5), "abandoned_max": max(abandoned, default=0),
        "queue_mean": round(mean(hists.get(QUEUED, {})), 1),
        "failed_per_call": round(locked / sent, 4), "abandoned": gave_up,
        "blocked_s": round(sum(k * v for k, v in hists.get(JOIN, {}).items()) / 1000, 1),
        "lock_waiters": round(st.mean(int(w[1]) for w in waits), 1) if waits else 0,
        "exceptions": text.count("Exception"), "problems": "; ".join(problems),
    }


def main():
    directory = sys.argv[1]
    baseline = sys.argv[2] if len(sys.argv) > 2 else "wait"
    runs = [r for r in map(read_run, sorted(glob.glob(os.path.join(directory, "*-users-rep*.log")))) if r]
    if not runs:
        sys.exit("no results")
    runs.sort(key=lambda r: (-r["users"], r["rep"], r["position"]))
    with open(os.path.join(directory, "runs.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=runs[0])
        w.writeheader()
        w.writerows(runs)

    for users in sorted({r["users"] for r in runs}, reverse=True):
        here = [r for r in runs if r["users"] == users]
        base = {r["rep"]: r["tps"] for r in here if r["variant"] == baseline}
        print(f"\n{users:,} users (change against {baseline}, same repetition)\n")
        print("| Variant | TPS by repetition | Mean TPS | Change, mean (min to max) | Successful calls: "
              "median / p95 / p99 (ms) | All calls: median / p95 / p99 (ms) | Generator blocked (s) | Lock waiters |")
        print("|---|---|---|---|---|---|---|---|")
        variants = sorted({r["variant"] for r in here}, key=lambda v: (v != baseline, v))
        for variant in variants:
            rs = [r for r in here if r["variant"] == variant]
            avg = lambda k: st.mean(r[k] for r in rs)
            ms3 = lambda *ks: " / ".join(f"{avg(k):,.0f}" + ("+" if any(r[k] >= CALL_MAX for r in rs) else "") for k in ks)
            diffs = [100 * (r["tps"] / base[r["rep"]] - 1) for r in rs if base.get(r["rep"])]
            if variant == baseline:
                change = "-"
            elif diffs:
                change = f"{st.mean(diffs):+.1f}% ({min(diffs):+.1f} to {max(diffs):+.1f})"
            else:
                change = "no baseline"
            print(f"| {variant} | {', '.join(format(r['tps'], ',.0f') for r in sorted(rs, key=lambda r: r['rep']))} "
                  f"| {avg('tps'):,.0f} | {change} | {ms3('p50', 'p95', 'p99')} "
                  f"| {ms3('all_p50', 'all_p95', 'all_p99')} | {avg('blocked_s'):.1f} | {avg('lock_waiters'):.1f} |")

        print("\n| Variant | Time in retry loop: mean / median / p95 / p99 (ms) | Succeeded first try | "
              "Attempts per call: mean / p99 / max | Failed attempts per call | Abandoned, mean per run (% of calls) | "
              "Abandoned call latency: median / max (ms) |")
        print("|---|---|---|---|---|---|---|")
        for variant in variants:
            rs = [r for r in here if r["variant"] == variant]
            avg = lambda k: st.mean(r[k] for r in rs)
            gave_up = [r for r in rs if r["abandoned"]]
            pct = 100 * sum(r["abandoned"] for r in rs) / sum(r["sent"] for r in rs)
            latency = (f"{st.mean(r['abandoned_p50'] for r in gave_up):,.0f} / {max(r['abandoned_max'] for r in gave_up):,}"
                       if gave_up else "-")
            print(f"| {variant} | {avg('loop_mean'):,.1f} / {avg('loop_p50'):,.0f} / {avg('loop_p95'):,.0f} / {avg('loop_p99'):,.0f} "
                  f"| {avg('first_try_pct'):.1f}% | {avg('attempts_mean'):.2f} / {avg('attempts_p99'):.0f} / {max(r['attempts_max'] for r in rs)} "
                  f"| {avg('failed_per_call'):.3f} | {avg('abandoned'):,.0f} ({pct:.2f}%) | {latency} |")
    bad = [r for r in runs if r["exceptions"]]
    for r in bad:
        print(f"\nCheck the log: {r['exceptions']} exceptions in {r['variant']}, {r['users']} users, rep {r['rep']}")
    for r in runs:
        if r["problems"]:
            print(f"\nDoesn't add up: {r['variant']}, {r['users']} users, rep {r['rep']}: {r['problems']}")
    print("\nPercentiles and means are averaged over the repetitions. Successful and all call latencies include the "
          "client's queue; time in the retry loop doesn't. 'All calls' adds the abandoned ones, and + means some "
          f"values reached the {CALL_MAX} ms limit of the histogram. A change is only convincing if its sign is the same "
          "in every repetition.")


main()
