#!/usr/bin/env python3
"""Measure bounded, paced traffic through the isolated messaging stack (local mock providers)."""

import argparse
import hashlib
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import math
import re
import subprocess
import threading
import time
import uuid
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from main_flow import API, AP_HEALTH_ENDPOINTS, ROOT, application_health, request_json, sql
from webhook_flow import key, read_items


PROMETHEUS = "http://127.0.0.1:19099/api/v1/query?"
JOBS = "|".join(AP_HEALTH_ENDPOINTS)
AP_TOPICS = {"pre-send": "message.received.v1", "skt-sender": "message.skt.http.send.v1",
             "kt-sender": "message.kt.http.send.v1", "lgu-sender": "message.lgu.http.send.v1",
             "result-manager": "MSG_RESULT", "complete-manager": "MSG-RESULT-FINALIZED",
             "webhook-sender": "WEBHOOK-SEND", "tcp-sender": "message.tcp.requested.v1"}
AP_HTTP = {"api": "/api/v1/messages", "webhook-receive": "/api/v1/message-webhooks/{carrier}"}
LAG_SERIES = {"messaging-pre-send-manager/message.received.v1", "messaging-result-manager/MSG_RESULT",
              "messaging-complete-manager/MSG-RESULT-FINALIZED", "messaging-webhook-sender/WEBHOOK-SEND",
              "messaging-tcp-sender/message.tcp.requested.v1",
              *(f"messaging-{carrier}-http-sender/message.{carrier}.http.send.v1" for carrier in ("skt", "kt", "lgu"))}
# Include the metric name in union matching: CPU/up and probe-up/probe-time share other labels.
METRICS = " or on(__name__,job,instance,group,topic,partition,area,id) ".join((
    'platform_kafka_committed_lag{group=~"messaging-.*"}', 'platform_kafka_log_end_offset', 'platform_kafka_probe_up',
    'platform_kafka_probe_last_success_timestamp_seconds', f'process_cpu_usage{{job=~"{JOBS}"}}',
    f'jvm_memory_used_bytes{{area="heap",job=~"{JOBS}"}}', f'up{{job=~"{JOBS}"}}',
    f'process_start_time_seconds{{job=~"{JOBS}"}}',
    'spring_kafka_listener_seconds_count{result="success"}', 'spring_kafka_listener_seconds_sum{result="success"}',
    'messaging_complete_sql_batch_size_sum',
    'messaging_scheduled_work_duration_seconds_count{phase="item"}',
    'messaging_scheduled_work_duration_seconds_sum{phase="item"}',
    'http_server_requests_seconds_count{job=~"api|webhook-receive",status="202",method="POST"}',
    'http_server_requests_seconds_sum{job=~"api|webhook-receive",status="202",method="POST"}'))


def distribution(values):
    values = sorted(values)
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}
    return {"count": len(values), **{name: round(values[math.ceil(len(values) * p) - 1], 3)
            for name, p in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}}


def case_for(number, failure_percent, secondary_percent):
    slot = number % 100
    return "primary-failure" if slot < failure_percent else (
        "secondary-success" if slot < failure_percent + secondary_percent else "primary-success")


def telemetry():
    with urlopen(PROMETHEUS + urlencode({"query": METRICS}), timeout=5) as response:
        result = json.load(response)
    if result.get("status") != "success":
        raise RuntimeError("Prometheus query failed")
    snapshot = {"at": time.time(), "cpuRatio": {}, "heapBytes": {}, "lag": {}, "up": {},
                "endOffsets": {}, "processStartedAt": {}, "listenerCount": {}, "listenerSeconds": {},
                "completionBatchRecords": None, "httpCount": {}, "httpSeconds": {},
                "scheduledCount": {}, "scheduledSeconds": {}}
    for item in result["data"]["result"]:
        labels = item["metric"]
        value = float(item["value"][1])
        if not math.isfinite(value):
            raise ValueError("Non-finite telemetry value")
        name, job = labels["__name__"], labels.get("job")
        if name == "process_cpu_usage":
            snapshot["cpuRatio"][job] = value
        elif name == "jvm_memory_used_bytes":
            snapshot["heapBytes"][job] = snapshot["heapBytes"].get(job, 0) + value
        elif name == "platform_kafka_committed_lag":
            label = labels["group"] + "/" + labels["topic"]
            snapshot["lag"][label] = snapshot["lag"].get(label, 0) + value
        elif name == "up":
            snapshot["up"][job] = value
        elif name == "platform_kafka_log_end_offset":
            topic = labels["topic"]
            snapshot["endOffsets"][topic] = snapshot["endOffsets"].get(topic, 0) + value
        elif name == "process_start_time_seconds":
            snapshot["processStartedAt"][job] = value
        elif name == "spring_kafka_listener_seconds_count":
            snapshot["listenerCount"][job] = snapshot["listenerCount"].get(job, 0) + value
        elif name == "spring_kafka_listener_seconds_sum":
            snapshot["listenerSeconds"][job] = snapshot["listenerSeconds"].get(job, 0) + value
        elif name == "messaging_complete_sql_batch_size_sum":
            snapshot["completionBatchRecords"] = value
        elif name.startswith("http_server_requests_seconds_") and labels.get("uri") == AP_HTTP.get(job):
            field = "httpCount" if name.endswith("_count") else "httpSeconds"
            snapshot[field][job] = snapshot[field].get(job, 0) + value
        elif name.startswith("messaging_scheduled_work_duration_seconds_"):
            field = "scheduledCount" if name.endswith("_count") else "scheduledSeconds"
            snapshot[field][labels["worker"]] = value
        elif name == "platform_kafka_probe_up":
            snapshot["probeUp"] = value
        elif name == "platform_kafka_probe_last_success_timestamp_seconds":
            snapshot["probeAt"] = value
    snapshot["valid"] = (snapshot.get("probeUp") == 1 and LAG_SERIES <= set(snapshot["lag"])
        and 0 <= snapshot["at"] - snapshot.get("probeAt", 0) <= 30
        and all(set(snapshot[name]) == set(AP_HEALTH_ENDPOINTS) for name in ("cpuRatio", "heapBytes", "up"))
        and all(value == 1 for value in snapshot["up"].values()))
    return snapshot


def history_query(run_id, start, detail=False):
    # Narrow by client/time as well as the run prefix; never include another run's traffic.
    where = (f"m.client_id=1 AND m.received_at >= to_timestamp({start - 60}) "
             f"AND m.message_id LIKE 'perf-{run_id}-%'")
    if not detail:
        return json.loads(sql("SELECT json_build_object('histories',count(*),'completed',count(*) FILTER "
            "(WHERE m.cleanup_status='DONE' AND o.status='DELIVERED' AND EXISTS "
            "(SELECT 1 FROM messaging_webhook.tbl_webhook_hist h WHERE h.batch_id=o.batch_id AND h.acknowledged))) "
            "FROM messaging_completion.tbl_msg_hist m LEFT JOIN messaging_webhook.tbl_webhook_outbox o "
            "ON o.client_msg_id=m.client_msg_id WHERE " + where))
    return json.loads(sql("SELECT COALESCE(json_agg(json_build_object('messageId',m.message_id,"
        "'clientMsgId',m.client_msg_id,'outcome',m.outcome,'stage',m.final_stage,'cleanup',m.cleanup_status,"
        "'receivedAt',extract(epoch from m.received_at),'decidedAt',extract(epoch from m.decided_at),"
        "'recordedAt',extract(epoch from m.recorded_at),'cleanedAt',extract(epoch from m.cleaned_at),'notifiedAt',"
        "CASE WHEN o.status='DELIVERED' THEN extract(epoch from o.updated_at) END,"
        "'billable',(SELECT count(*) FROM messaging_completion.tbl_cdr_hist c WHERE c.client_msg_id=m.client_msg_id),"
        "'acknowledged',EXISTS(SELECT 1 FROM messaging_webhook.tbl_webhook_hist h "
        "WHERE h.batch_id=o.batch_id AND h.acknowledged))),'[]'::json) "
        "FROM messaging_completion.tbl_msg_hist m LEFT JOIN messaging_webhook.tbl_webhook_outbox o "
        "ON o.client_msg_id=m.client_msg_id WHERE " + where))


def reconcile(requests, rows):
    accepted = {r["messageId"]: r for r in requests if r.get("accepted")}
    recorded = {r["messageId"]: r for r in rows}
    problems = []
    if len(recorded) != len(rows):
        problems.append("duplicate-message-history")
    if set(accepted) != set(recorded):
        problems.append("accepted-history-id-mismatch")
    if len({r["clientMsgId"] for r in accepted.values()}) != len(accepted):
        problems.append("duplicate-admission-id")
    for identifier in accepted.keys() & recorded.keys():
        request, row = accepted[identifier], recorded[identifier]
        expected_outcome = "FAILURE" if request["case"] == "primary-failure" else "SUCCESS"
        expected_stage = "SECONDARY" if request["case"] == "secondary-success" else "PRIMARY"
        if (row["clientMsgId"] != request["clientMsgId"] or row["outcome"] != expected_outcome
                or row["stage"] != expected_stage or row["billable"] != int(request["case"] == "primary-success")
                or row["cleanup"] != "DONE" or row["cleanedAt"] is None
                or not row["acknowledged"] or row["notifiedAt"] is None):
            problems.append("result-mismatch:" + identifier)
        if any(row[field] is not None and row[field] < row["receivedAt"]
               for field in ("decidedAt", "recordedAt", "cleanedAt", "notifiedAt")):
            problems.append("negative-server-latency:" + identifier)
    return problems


def summarize(requests, rows, start, seconds):
    started = [r for r in requests if "startedAt" in r]
    accepted = [r for r in started if r.get("accepted")]
    end = start + seconds
    done = [r for r in rows if r["notifiedAt"] is not None and r["cleanedAt"] is not None
            and r["cleanup"] == "DONE" and r["acknowledged"]]
    completed_at = [max(r["recordedAt"], r["notifiedAt"], r["cleanedAt"]) for r in done]
    bins = {}
    for name, times in (("started", [r["startedAt"] for r in started]),
                        ("accepted", [r["respondedAt"] for r in accepted]), ("completed", completed_at)):
        for stamp in times:
            bucket = math.floor(stamp - start)
            bins.setdefault(bucket, {"second": bucket, "started": 0, "accepted": 0, "completed": 0})[name] += 1
    elapsed = max(seconds, max(completed_at, default=end) - start)
    result = {"scheduled": len(requests), "started": len(started), "accepted": len(accepted),
        "generatorDropped": len(requests) - len(started), "admissionErrors": len(started) - len(accepted),
        "unconfirmedAdmissions": sum(not r.get("accepted") and not 400 <= r.get("httpStatus", 0) < 500 for r in started),
        "admissionErrorRate": (len(started) - len(accepted)) / len(started) if started else None,
        "httpStatuses": dict(Counter(str(r.get("httpStatus", "transport-error")) for r in started)),
        "acceptedCases": dict(Counter(r["case"] for r in accepted)),
        "histories": len(rows), "completed": len(done), "billable": sum(r["billable"] for r in rows),
        "unexpectedHistories": sorted({r["messageId"] for r in rows} - {r["messageId"] for r in accepted}),
        "rates": {"startedWithinLoadWindowTps": sum(start <= r["startedAt"] < end for r in started) / seconds,
                  "acceptedWithinLoadWindowTps": sum(start <= r["respondedAt"] < end for r in accepted) / seconds,
                  "completedWithinLoadWindowTps": sum(start <= stamp < end for stamp in completed_at) / seconds,
                  "completedIncludingDrainTps": len(done) / elapsed, "completionWindowSeconds": elapsed},
        "latencyMs": {"httpAll": distribution([r["httpMs"] for r in started]),
                      "httpAccepted": distribution([r["httpMs"] for r in accepted]),
                      "dispatchDelay": distribution([r["dispatchDelayMs"] for r in started]),
                      "receiveToDecision": distribution([(r["decidedAt"] - r["receivedAt"]) * 1000 for r in rows]),
                      "receiveToHistoryRecord": distribution([(r["recordedAt"] - r["receivedAt"]) * 1000 for r in rows]),
                      "receiveToCleanupRecord": distribution([(r["cleanedAt"] - r["receivedAt"]) * 1000 for r in done]),
                      "receiveToCustomerAckRecord": distribution([(r["notifiedAt"] - r["receivedAt"]) * 1000 for r in done]),
                      "receiveToCompletion": distribution([(max(r["recordedAt"], r["notifiedAt"], r["cleanedAt"]) - r["receivedAt"]) * 1000 for r in done])},
        "oneSecondCounts": [bins[b] for b in sorted(bins)]}
    return result


def summarize_windows(requests, rows, samples, start, seconds):
    """One-minute load windows; latency follows admission cohorts through the drain."""
    accepted = {r["messageId"]: r for r in requests if r.get("accepted")}
    completed = {r["messageId"]: r for r in rows if r["messageId"] in accepted
                 and r["cleanup"] == "DONE" and r["acknowledged"]
                 and all(r[field] is not None for field in ("recordedAt", "notifiedAt", "cleanedAt"))}
    completion_times = {identifier: max(r["recordedAt"], r["notifiedAt"], r["cleanedAt"])
                        for identifier, r in completed.items()}
    windows = []
    for offset in range(0, seconds, 60):
        begin, end = start + offset, start + min(offset + 60, seconds)
        cohort = {identifier for identifier, r in accepted.items() if begin <= r["respondedAt"] < end}
        done = cohort & completed.keys()
        pending = {identifier for identifier, r in accepted.items() if r["respondedAt"] < end}
        pending -= {identifier for identifier, stamp in completion_times.items() if stamp < end}
        observed = [s for s in samples if begin <= s["at"] < end]
        valid = sorted((s for s in observed if s["valid"]), key=lambda s: s["at"])
        jobs = sorted({job for s in valid for job in s["heapBytes"]})
        windows.append({"fromSecond": offset, "toSecond": min(offset + 60, seconds),
            "accepted": len(cohort), "cohortCompleted": len(done),
            "completedDuringWindow": sum(begin <= stamp < end for stamp in completion_times.values()),
            "pendingAtWindowEnd": len(pending),
            "cohortCompletionLatencyMs": distribution([
                (completion_times[i] - completed[i]["receivedAt"]) * 1000 for i in done]),
            "cohortCleanupAfterHistoryMs": distribution([
                (completed[i]["cleanedAt"] - completed[i]["recordedAt"]) * 1000 for i in done]),
            "telemetrySamples": len(observed), "invalidTelemetrySamples": len(observed) - len(valid),
            "peakLag": max((sum(s["lag"].values()) for s in valid), default=None),
            "lastObservedLag": sum(valid[-1]["lag"].values()) if valid else None,
            "lastTelemetryAt": valid[-1]["at"] if valid else None,
            "heapBytesByAp": {job: {"min": min(s["heapBytes"][job] for s in valid),
                                      "max": max(s["heapBytes"][job] for s in valid),
                                      "last": valid[-1]["heapBytes"][job]} for job in jobs}})
    return windows


def summarize_ap_observations(samples):
    """Commit progress is records/s, not an isolated AP capacity or message rate."""
    valid = sorted((s for s in samples if s["valid"]), key=lambda s: s["at"])
    result = {}
    for job, topic in AP_TOPICS.items():
        observations = []
        for sample in valid:
            matches = [value for label, value in sample["lag"].items() if label.endswith("/" + topic)]
            if len(matches) != 1 or topic not in sample.get("endOffsets", {}):
                continue
            observation = {"at": sample["probeAt"], "lag": matches[0],
                           "committed": sample["endOffsets"][topic] - matches[0]}
            if not observations or observation["at"] > observations[-1]["at"]:
                observations.append(observation)
        rates = [(b["committed"] - a["committed"]) / (b["at"] - a["at"])
                 for a, b in zip(observations, observations[1:]) if b["committed"] >= a["committed"]]
        resets = any(b["committed"] < a["committed"] for a, b in zip(observations, observations[1:]))
        starts = {s.get("processStartedAt", {}).get(job) for s in valid}
        result[job] = {"topic": topic, "unit": "committed Kafka records/second", "capacityVerdict": "not-measured",
            "observations": len(observations), "offsetRegression": resets,
            "processRestartObserved": len(starts - {None}) > 1,
            "meanObservedCommitRps": ((observations[-1]["committed"] - observations[0]["committed"])
                / (observations[-1]["at"] - observations[0]["at"])) if len(observations) > 1 and not resets else None,
            "peakObservedCommitRps": max(rates, default=None) if not resets else None,
            "peakLag": max((s["lag"] for s in observations), default=None),
            "peakCpuRatio": max((s["cpuRatio"][job] for s in valid), default=None),
            "peakHeapBytes": max((s["heapBytes"][job] for s in valid), default=None)}
        timed = [s for s in valid if job in s.get("listenerCount", {}) and job in s.get("listenerSeconds", {})]
        count = timed[-1]["listenerCount"][job] - timed[0]["listenerCount"][job] if len(timed) > 1 else 0
        duration = timed[-1]["listenerSeconds"][job] - timed[0]["listenerSeconds"][job] if len(timed) > 1 else 0
        mean_ms = duration * 1000 / count if count > 0 and duration >= 0 and not result[job]["processRestartObserved"] else None
        result[job]["listenerInvocations"] = count
        result[job]["listenerTimingUnit"] = "batch" if job == "complete-manager" else "Kafka record"
        result[job]["meanListenerInvocationMs"] = mean_ms
        if job == "complete-manager":
            records = (timed[-1]["completionBatchRecords"] - timed[0]["completionBatchRecords"]
                       if len(timed) > 1 and all(s.get("completionBatchRecords") is not None for s in (timed[0], timed[-1])) else 0)
            result[job]["meanRecordsPerBatch"] = records / count if count > 0 and records > 0 and mean_ms is not None else None
            result[job]["amortizedListenerMsPerRecord"] = duration * 1000 / records if records > 0 and mean_ms is not None else None
    for job, uri in AP_HTTP.items():
        timed = [s for s in valid if job in s.get("httpCount", {}) and job in s.get("httpSeconds", {})]
        count = timed[-1]["httpCount"][job] - timed[0]["httpCount"][job] if len(timed) > 1 else 0
        duration = timed[-1]["httpSeconds"][job] - timed[0]["httpSeconds"][job] if len(timed) > 1 else 0
        starts = {s.get("processStartedAt", {}).get(job) for s in valid} - {None}
        result[job] = {"uri": uri, "unit": "accepted HTTP requests/second", "capacityVerdict": "not-measured",
            "processRestartObserved": len(starts) > 1, "acceptedRequests": count,
            "meanObservedRequestRps": count / (timed[-1]["at"] - timed[0]["at"]) if len(timed) > 1 and count >= 0 and len(starts) <= 1 else None,
            "meanRequestMs": duration * 1000 / count if count > 0 and duration >= 0 and len(starts) <= 1 else None}
    for job, worker in (("result-manager", "result_inbox"), ("result-manager", "result_outbox"),
                        ("complete-manager", "complete_cleanup")):
        timed = [s for s in valid if worker in s.get("scheduledCount", {}) and worker in s.get("scheduledSeconds", {})]
        count = timed[-1]["scheduledCount"][worker] - timed[0]["scheduledCount"][worker] if len(timed) > 1 else 0
        duration = timed[-1]["scheduledSeconds"][worker] - timed[0]["scheduledSeconds"][worker] if len(timed) > 1 else 0
        result[job].setdefault("scheduledWork", {})[worker] = {
            "itemAttempts": count, "meanItemMs": duration * 1000 / count
            if count > 0 and duration >= 0 and not result[job]["processRestartObserved"] else None}
    return result


def analyze_run(run_id):
    # Recalculate older runs without replacing their original report/source hashes.
    directory = ROOT / ".monitoring/performance" / run_id
    report = json.loads((directory / "report.json").read_text())
    if report.get("stage") not in ("measured", "failed") or "finishedAt" not in report:
        raise ValueError("Wait for the saved run to finish before analyzing it")
    requests = [json.loads(line) for line in (directory / "requests.jsonl").read_text().splitlines()]
    rows = json.loads((directory / "results.json").read_text())
    samples = [json.loads(line) for line in (directory / "telemetry.jsonl").read_text().splitlines()]
    result = {"runId": run_id,
              "sourceHash": hashlib.sha256((ROOT / "scripts/monitoring/performance_flow.py").read_bytes()).hexdigest(),
              "inputHashes": {name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
                              for name in ("report.json", "requests.jsonl", "results.json", "telemetry.jsonl")},
              "loadWindows": summarize_windows(requests, rows, samples, report["startedAt"],
                                               report["configuration"]["seconds"])}
    output = directory / "window-analysis.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
    print(output)


def run(args):
    run_id = uuid.uuid4().hex[:8]
    directory = ROOT / ".monitoring/performance" / run_id
    directory.mkdir(parents=True)
    report = {"runId": run_id, "pass": False, "stage": "preflight", "configuration": vars(args),
              "artifacts": str(directory), "capacityOrSlaVerdict": "not-evaluated"}
    requests, samples, rows = [], [], []
    lock, stop = threading.Lock(), threading.Event()

    def save():
        encoded = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)
        for path in (directory / "report.json", ROOT / ".monitoring/performance-latest.json"):
            path.write_text(encoded, encoding="utf-8")

    def sample_loop():
        with (directory / "telemetry.jsonl").open("w") as output:
            while not stop.is_set():
                try:
                    sample = telemetry()
                except Exception as failure:
                    sample = {"at": time.time(), "valid": False, "error": str(failure)}
                samples.append(sample)
                output.write(json.dumps(sample) + "\n")
                output.flush()
                stop.wait(5)

    save()
    monitor = None
    try:
        health = application_health()
        assert health and all(v == "UP" for v in health.values()), health
        baseline = telemetry()
        report["baseline"] = baseline
        assert baseline["valid"] and sum(baseline["lag"].values()) == 0, "Need healthy telemetry and an idle stack"
        report["gitCommit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        report["workingTreeDirty"] = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True))
        report["sourceHashes"] = {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in (
            "scripts/monitoring/performance_flow.py", "scripts/monitoring/main_flow.py",
            "scripts/monitoring/webhook_flow.py", "scripts/primary-flow-mock.py", "monitoring/compose.yml")}
        report["dockerResources"] = json.loads(subprocess.check_output(["docker", "info", "--format",
            '{"cpus":{{.NCPU}},"memoryBytes":{{.MemTotal}},"runningContainers":{{.ContainersRunning}}}'], text=True, timeout=15))
        containers = json.loads(subprocess.check_output(["docker", "inspect", *[
            "platform-messaging-monitoring-" + job + "-1" for job in AP_HEALTH_ENDPOINTS]], text=True, timeout=15))
        report["apRuntime"] = {job: {"container": container["Name"],
            "cpuQuotaNanos": container["HostConfig"]["NanoCpus"], "memoryLimitBytes": container["HostConfig"]["Memory"],
            "jvmOptions": [arg for arg in container["Config"]["Cmd"] if arg.startswith(("-Xms", "-Xmx", "-XX:ActiveProcessorCount="))],
            "concurrency": [value for value in container["Config"]["Env"] if "CONCURRENCY=" in value],
            "startedAt": container["State"]["StartedAt"]} for job, container in zip(AP_HEALTH_ENDPOINTS, containers)}
        clock_start = time.time()
        db_time = float(sql("SELECT extract(epoch from clock_timestamp())"))
        clock_end = time.time()
        report["clockCheck"] = {"serverMinusGeneratorMs": (db_time - (clock_start + clock_end) / 2) * 1000,
                                "uncertaintyMs": (clock_end - clock_start) * 500}
        assert clock_start - 1 <= db_time <= clock_end + 1, "Server/generator clocks differ by more than one second"
        (directory / "jars.env").write_text((ROOT / ".monitoring/jars.env").read_text())
        token = request_json(API + "/api/v1/auth/token",
                             {"username": "local-user", "password": "local-password"})["data"]["accessToken"]
        monitor = threading.Thread(target=sample_loop, daemon=True)
        monitor.start()
        slots = threading.BoundedSemaphore(args.workers)
        start_mono, start_wall = time.monotonic(), time.time()
        report.update({"stage": "sending", "startedAt": start_wall})
        save()

        with (directory / "requests.jsonl").open("w") as output:
            def record(value):
                with lock:
                    requests.append(value)
                    output.write(json.dumps(value) + "\n")
                    output.flush()

            def send(number, due):
                case = case_for(number, args.failure_percent, args.secondary_percent)
                value = {"messageId": f"perf-{run_id}-{number:06d}", "case": case,
                         "scheduledAt": start_wall + due - start_mono}
                begin, wall = time.monotonic(), time.time()
                value.update({"startedAt": wall, "dispatchDelayMs": (begin - due) * 1000, "accepted": False})
                body = {"messageId": value["messageId"], "recipientNumber": "01099990000", "messageCategory": "GENERAL",
                        "payload": {"text": "monitor-force-failure" if case != "primary-success" else "performance test"}}
                if case == "secondary-success":
                    body["secondarySendPayload"] = {"text": "performance TCP fallback"}
                try:
                    request = Request(API + "/api/v1/messages", data=json.dumps(body).encode(),
                        headers={"Content-Type": "application/json", "Authorization": "Bearer " + token})
                    with urlopen(request, timeout=args.request_timeout) as response:
                        value["httpStatus"] = response.status
                        answer = json.load(response)
                    data = answer.get("data", {})
                    value["accepted"] = value["httpStatus"] == 202 and data.get("status") == "RECEIVED" and bool(data.get("clientMsgId"))
                    if value["accepted"]:
                        value["clientMsgId"] = data["clientMsgId"]
                except HTTPError as failure:
                    value["httpStatus"] = failure.code
                except Exception as failure:
                    value["error"] = type(failure).__name__
                finally:
                    value.update({"respondedAt": time.time(), "httpMs": (time.monotonic() - begin) * 1000})
                    record(value)
                    slots.release()

            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                for number in range(args.rate * args.seconds):
                    due = start_mono + number / args.rate
                    time.sleep(max(0, due - time.monotonic()))
                    late = time.monotonic() - due > max(.05, 1 / args.rate)
                    if late or not slots.acquire(blocking=False):
                        record({"messageId": f"perf-{run_id}-{number:06d}", "accepted": False,
                                "dropReason": "scheduler-late" if late else "generator-capacity"})
                    else:
                        pool.submit(send, number, due)
                    if number % (args.rate * 10) == 0:
                        print(f"Performance {run_id}: scheduled {number + 1}/{args.rate * args.seconds}", flush=True)
        accepted = sum(bool(r.get("accepted")) for r in requests)
        deadline = time.monotonic() + args.timeout
        report["stage"] = "draining"
        save()
        while True:
            counts = history_query(run_id, start_wall)
            if counts["completed"] >= accepted or time.monotonic() >= deadline:
                break
            time.sleep(2)
        rows = history_query(run_id, start_wall, detail=True)
        (directory / "results.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2))
        report["measurements"] = summarize(requests, rows, start_wall, args.seconds)
        problems = reconcile(requests, rows)
        if accepted != args.rate * args.seconds:
            problems.append("load-not-fully-accepted")
        if rows and read_items("ORIGIN", [key(r["clientMsgId"], "META") for r in rows]):
            problems.append("origin-not-removed")
        # A fresh collector observation is required; cached pre-load zero lag is insufficient.
        drain_checked_at = time.time()
        while time.monotonic() < deadline:
            current = telemetry()
            if current["valid"] and current["probeAt"] >= drain_checked_at and sum(current["lag"].values()) == 0:
                report["lagZeroObservedAfterLoadSeconds"] = max(0, time.time() - start_wall - args.seconds)
                break
            time.sleep(5)
        else:
            problems.append("lag-not-cleared")
        report["problems"] = problems
        report["applicationHealth"] = application_health()
        if not all(value == "UP" for value in report["applicationHealth"].values()):
            problems.append("application-health-not-up")
        report["stage"] = "measured"
    except Exception as failure:
        report.update({"stage": "failed", "failure": f"{type(failure).__name__}: {failure}"})
        raise
    finally:
        stop.set()
        if monitor:
            monitor.join(timeout=10)
        if "startedAt" in report:
            report["loadWindows"] = summarize_windows(requests, rows, samples, report["startedAt"], args.seconds)
        valid = [s for s in samples if s["valid"]]
        report["telemetry"] = {"samples": len(samples), "invalidSamples": len(samples) - len(valid),
            "peakLag": max((sum(s["lag"].values()) for s in valid), default=None),
            "peakCpuRatioByAp": {job: max((s["cpuRatio"][job] for s in valid), default=None) for job in AP_HEALTH_ENDPOINTS},
            "peakHeapBytesByAp": {job: max((s["heapBytes"][job] for s in valid), default=None) for job in AP_HEALTH_ENDPOINTS}}
        report["apObservations"] = summarize_ap_observations([report["baseline"], *samples] if "baseline" in report else samples)
        if report["stage"] == "measured" and (len(valid) < 3 or len(valid) != len(samples)):
            report["problems"].append("telemetry-incomplete")
        report["finishedAt"] = time.time()
        report["pass"] = report["stage"] == "measured" and not report.get("problems") and len(valid) >= 3 and len(valid) == len(samples)
        save()
    print(json.dumps({k: report[k] for k in ("runId", "pass", "artifacts", "measurements", "telemetry", "problems")}, ensure_ascii=False, indent=2))
    if not report["pass"]:
        raise AssertionError("Performance run incomplete; inspect report and raw artifacts")


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=5)
    parser.add_argument("--seconds", type=int, default=20)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--timeout", type=int, default=180, help="Drain timeout in seconds")
    parser.add_argument("--request-timeout", type=float, default=10)
    parser.add_argument("--failure-percent", type=int, default=0)
    parser.add_argument("--secondary-percent", type=int, default=0)
    parser.add_argument("--analyze-run", help="Recalculate minute windows from a saved run; sends no traffic")
    args = parser.parse_args()
    if args.analyze_run and not re.fullmatch(r"[0-9a-f]{8}", args.analyze_run):
        parser.error("analyze-run must be an eight-character lowercase hexadecimal run ID")
    if not (1 <= args.rate <= 1000 and 10 <= args.seconds <= 600 and args.rate * args.seconds <= 100000
            and 1 <= args.workers <= 256 and 30 <= args.timeout <= 1800 and 0 < args.request_timeout <= 30
            and 0 <= args.failure_percent <= 100 and 0 <= args.secondary_percent <= 100
            and args.failure_percent + args.secondary_percent <= 100):
        parser.error("rate 1..1000, seconds 10..600, total <=100000, workers 1..256, timeout 30..1800, request timeout (0,30], percentages sum <=100")
    return args


if __name__ == "__main__":
    options = arguments()
    if options.analyze_run:
        analyze_run(options.analyze_run)
    else:
        run(options)
