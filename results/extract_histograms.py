#!/usr/bin/env python3
"""Extract per-millisecond latency histograms from ChargingDemoTransactions logs.

Usage: extract_histograms.py label=log [label=log ...] > histograms.csv

Writes CSV rows: run,transaction,latency_ms,calls. latency_ms is the bucket's lower
bound in milliseconds; the last bucket, ">= 1999", is written as 1999 with
overflow=1 so it can be told apart from the 1999 ms bucket.
"""
import csv
import re
import sys

LINE = re.compile(r"^(>= )?(\d+)\t([\d.]+)\t(REPORT_QUOTA_USAGE|ADD_CREDIT)$")

out = csv.writer(sys.stdout)
out.writerow(["run", "transaction", "latency_ms", "overflow", "calls"])
for arg in sys.argv[1:]:
    label, path = arg.split("=", 1)
    with open(path) as f:
        for line in f:
            m = LINE.match(line.rstrip("\n"))
            if m:
                out.writerow([label, m.group(4), m.group(2), 1 if m.group(1) else 0, int(float(m.group(3)))])
