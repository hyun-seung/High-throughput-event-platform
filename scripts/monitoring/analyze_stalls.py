#!/usr/bin/env python3
"""Compare saved benchmark samples without generating traffic or changing runtime state."""
import argparse
import gzip
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "poc"))
from run import metric


def analyze(directory):
    starts = []
    with (directory / "requests.jsonl").open() as stream:
        for line in stream:
            row = json.loads(line)
            if row["kind"] == "start":
                starts.append(row["started"] / 1000)
    base, end = min(starts), max(starts)
    samples = [json.loads(line) for line in (directory / "samples.jsonl").read_text().splitlines()]
    stages = {"api": ["api_publish"], "ingress": ["ingress_store", "ingress_publish"],
              "dispatch": ["dispatch_claim", "dispatch_http", "dispatch_store"]}
    report = {"inputStartUtc": datetime.fromtimestamp(base, timezone.utc).isoformat(),
              "source": str(directory), "auditObservations": {}, "intervals": []}
    for app in stages:
        with gzip.open(directory / f"{app}-runtime.log.gz", "rt") as stream:
            times = sorted(datetime.fromisoformat(line.split()[0].replace("Z", "+00:00")).timestamp()
                           for line in stream if "delivery.audit" in line)
        bins = Counter(int((stamp - base) // 10) * 10 for stamp in times if base <= stamp <= end)
        gaps = sorted(((right - left, left - base) for left, right in zip(times, times[1:])
                       if base <= left < right <= end), reverse=True)[:5]
        report["auditObservations"][app] = {
            "countsByInputRelative10s": dict(sorted(bins.items())),
            "largestGaps": [{"seconds": gap, "startSeconds": start} for gap, start in gaps]}
    for index in range(1, len(samples)):
        row = {"startSeconds": round(samples[index - 1]["time"] - base, 3),
               "endSeconds": round(samples[index]["time"] - base, 3),
               "lag": samples[index]["lag"], "stageMeanMs": {}, "gcPauseSumMs": {}}
        for app, names in stages.items():
            with gzip.open(directory / f"{index - 1:04}-{app}.prom.gz", "rt") as stream:
                before = stream.read()
            with gzip.open(directory / f"{index:04}-{app}.prom.gz", "rt") as stream:
                after = stream.read()
            for stage in names:
                count = (metric(after, "delivery_stage_duration_seconds_count", stage=stage)
                         - metric(before, "delivery_stage_duration_seconds_count", stage=stage))
                total = (metric(after, "delivery_stage_duration_seconds_sum", stage=stage)
                         - metric(before, "delivery_stage_duration_seconds_sum", stage=stage))
                if count < 0 or total < 0:
                    raise ValueError("Metric counter reset: cannot compare this interval")
                row["stageMeanMs"][stage] = round(total / count * 1000, 3) if count else None
            row["gcPauseSumMs"][app] = round(1000 * (metric(after, "jvm_gc_pause_seconds_sum")
                                                               - metric(before, "jvm_gc_pause_seconds_sum")), 3)
        report["intervals"].append(row)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase_directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(analyze(args.phase_directory), ensure_ascii=False, indent=2))
