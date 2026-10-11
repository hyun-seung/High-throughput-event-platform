"""Regression checks for performance accounting; no application or network dependencies."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from performance_flow import analyze_run, case_for, distribution, reconcile, summarize, summarize_windows


def request(number=0, case="primary-success", **changes):
    value = {"messageId": f"test-{number}", "clientMsgId": f"id-{number}", "case": case,
             "accepted": True, "startedAt": 100, "respondedAt": 101, "httpMs": 1000,
             "dispatchDelayMs": 5, "httpStatus": 202}
    return value | changes


def history(number=0, **changes):
    value = {"messageId": f"test-{number}", "clientMsgId": f"id-{number}", "outcome": "SUCCESS",
             "stage": "PRIMARY", "cleanup": "DONE", "billable": 1, "acknowledged": True,
             "receivedAt": 100, "decidedAt": 102, "recordedAt": 103, "cleanedAt": 103.5, "notifiedAt": 104}
    return value | changes


class PerformanceAccountingTest(unittest.TestCase):
    def test_nearest_rank_percentiles_and_empty_samples(self):
        self.assertEqual(distribution(range(1, 101)), {"count": 100, "p50": 50, "p95": 95, "p99": 99, "max": 100})
        self.assertIsNone(distribution([])["p99"])
        self.assertEqual(distribution([3])["p99"], 3)

    def test_case_mix_preserves_exact_counts_per_hundred(self):
        cases = [case_for(i, 10, 20) for i in range(200)]
        self.assertEqual(cases.count("primary-failure"), 20)
        self.assertEqual(cases.count("secondary-success"), 40)
        self.assertEqual(cases.count("primary-success"), 140)

    def test_completion_after_load_does_not_inflate_load_window_tps(self):
        result = summarize([request()], [history(notifiedAt=120)], 100, 10)
        self.assertEqual(result["rates"]["completedWithinLoadWindowTps"], 0)
        self.assertEqual(result["rates"]["completedIncludingDrainTps"], .05)
        self.assertEqual(result["latencyMs"]["receiveToCompletion"]["p95"], 20000)
        self.assertEqual(result["oneSecondCounts"][-1]["second"], 20)

    def test_admission_errors_and_generator_drops_have_different_denominators(self):
        requests = [request(), request(1, accepted=False, httpStatus=429),
                    {"messageId": "test-2", "accepted": False, "dropReason": "generator-capacity"}]
        result = summarize(requests, [history()], 100, 10)
        self.assertEqual(result["scheduled"], 3)
        self.assertEqual(result["generatorDropped"], 1)
        self.assertEqual(result["admissionErrors"], 1)
        self.assertEqual(result["admissionErrorRate"], .5)
        self.assertEqual(result["unconfirmedAdmissions"], 0)
        self.assertEqual(result["latencyMs"]["httpAll"]["count"], 2)
        self.assertEqual(result["latencyMs"]["httpAccepted"]["count"], 1)

    def test_id_sets_are_compared_even_when_counts_match(self):
        self.assertIn("accepted-history-id-mismatch", reconcile([request()], [history(1)]))

    def test_provider_failure_is_an_expected_business_result_not_an_admission_error(self):
        requests = [request(0, case="primary-failure"), request(1, case="secondary-success")]
        rows = [history(0, outcome="FAILURE", billable=0), history(1, stage="SECONDARY", billable=0)]
        self.assertEqual(reconcile(requests, rows), [])
        self.assertEqual(summarize(requests, rows, 100, 10)["admissionErrors"], 0)
        self.assertTrue(reconcile(requests, [rows[0], rows[1] | {"billable": 1}]))

    def test_missing_ack_or_clock_reversal_is_not_a_success(self):
        self.assertTrue(reconcile([request()], [history(acknowledged=False)]))
        self.assertIn("negative-server-latency:test-0", reconcile([request()], [history(decidedAt=99)]))

    def test_uncertain_request_completed_on_server_remains_visible(self):
        uncertain = request(accepted=False, error="TimeoutError")
        uncertain.pop("httpStatus")
        result = summarize([uncertain], [history()], 100, 10)
        self.assertEqual(result["unexpectedHistories"], ["test-0"])
        self.assertEqual(result["httpStatuses"], {"transport-error": 1})
        self.assertEqual(result["unconfirmedAdmissions"], 1)
        self.assertTrue(reconcile([uncertain], [history()]))

    def test_empty_run_has_no_fabricated_percentiles_or_completion_rate(self):
        result = summarize([], [], 100, 10)
        self.assertIsNone(result["latencyMs"]["receiveToCompletion"]["p99"])
        self.assertIsNone(result["admissionErrorRate"])
        self.assertEqual(result["rates"]["completedIncludingDrainTps"], 0)

    def test_cleanup_finishing_last_is_included_in_completion_latency(self):
        result = summarize([request()], [history(cleanedAt=125)], 100, 10)
        self.assertEqual(result["latencyMs"]["receiveToCompletion"]["p95"], 25000)
        self.assertEqual(result["rates"]["completedIncludingDrainTps"], .04)
        self.assertTrue(reconcile([request()], [history(cleanedAt=None)]))

    def test_windows_follow_admission_cohort_until_completion_after_load(self):
        requests = [request(), request(1, respondedAt=160)]
        rows = [history(cleanedAt=170), history(1, receivedAt=159, recordedAt=170, cleanedAt=230, notifiedAt=180)]
        windows = summarize_windows(requests, rows, [], 100, 120)
        first, second = windows
        self.assertEqual([w["accepted"] for w in windows], [1, 1])
        self.assertEqual([w["cohortCompleted"] for w in windows], [1, 1])
        self.assertEqual([w["completedDuringWindow"] for w in windows], [0, 1])
        self.assertEqual([w["pendingAtWindowEnd"] for w in windows], [1, 1])
        self.assertEqual(first["cohortCompletionLatencyMs"]["p95"], 70000)
        self.assertEqual(second["cohortCompletionLatencyMs"]["p95"], 71000)
        self.assertEqual(second["cohortCleanupAfterHistoryMs"]["p95"], 60000)

    def test_windows_keep_incomplete_and_unaccepted_messages_distinct(self):
        requests = [request(), request(1, accepted=False),
                    {"messageId": "dropped", "accepted": False}]
        rows = [history(cleanedAt=None), history(1)]
        window = summarize_windows(requests, rows, [], 100, 10)[0]
        self.assertEqual(window["toSecond"], 10)
        self.assertEqual(window["accepted"], 1)
        self.assertEqual(window["cohortCompleted"], 0)
        self.assertEqual(window["completedDuringWindow"], 0)
        self.assertEqual(window["pendingAtWindowEnd"], 1)
        self.assertIsNone(window["cohortCompletionLatencyMs"]["p95"])

    def test_missing_telemetry_is_not_reported_as_zero_lag(self):
        sample = lambda at, heap, lag: {"at": at, "valid": True, "heapBytes": {"api": heap}, "lag": {"group/topic": lag}}
        samples = [sample(159, 20, 3), sample(101, 30, 5), {"at": 110, "valid": False},
                   {"at": 161, "valid": False}, sample(220, 10, 0)]
        first, second = summarize_windows([], [], samples, 100, 120)
        self.assertEqual(first["telemetrySamples"], 3)
        self.assertEqual(first["invalidTelemetrySamples"], 1)
        self.assertEqual(first["peakLag"], 5)
        self.assertEqual(first["lastObservedLag"], 3)
        self.assertEqual(first["lastTelemetryAt"], 159)
        self.assertEqual(first["heapBytesByAp"]["api"], {"min": 20, "max": 30, "last": 20})
        self.assertIsNone(second["peakLag"])
        self.assertIsNone(second["lastObservedLag"])
        self.assertEqual(second["heapBytesByAp"], {})

    def test_completion_at_boundary_belongs_to_next_window(self):
        first, second = summarize_windows([request()], [history(cleanedAt=160)], [], 100, 120)
        self.assertEqual(first["completedDuringWindow"], 0)
        self.assertEqual(first["pendingAtWindowEnd"], 1)
        self.assertEqual(second["completedDuringWindow"], 1)
        self.assertEqual(second["pendingAtWindowEnd"], 0)

    def test_offline_analysis_preserves_original_evidence_and_rejects_active_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / ".monitoring/performance/1234abcd"
            directory.mkdir(parents=True)
            source = root / "scripts/monitoring/performance_flow.py"
            source.parent.mkdir(parents=True)
            source.write_text("test source")
            report = {"runId": "1234abcd", "stage": "draining", "startedAt": 100,
                      "configuration": {"seconds": 10}}
            report_path = directory / "report.json"
            report_path.write_text(json.dumps(report))
            with patch("performance_flow.ROOT", root):
                with self.assertRaisesRegex(ValueError, "finish"):
                    analyze_run("1234abcd")
                report.update({"stage": "measured", "finishedAt": 200})
                report_path.write_text(json.dumps(report))
                (directory / "requests.jsonl").write_text(json.dumps(request()) + "\n")
                (directory / "results.json").write_text(json.dumps([history()]))
                (directory / "telemetry.jsonl").write_text("")
                before = {p.name: p.read_bytes() for p in directory.iterdir()}
                with patch("builtins.print"):
                    analyze_run("1234abcd")
                analysis = json.loads((directory / "window-analysis.json").read_text())
                for name, data in before.items():
                    self.assertEqual((directory / name).read_bytes(), data)
                    self.assertEqual(analysis["inputHashes"][name], hashlib.sha256(data).hexdigest())
                self.assertEqual(analysis["sourceHash"], hashlib.sha256(source.read_bytes()).hexdigest())
                self.assertEqual(analysis["loadWindows"][0]["cohortCompleted"], 1)


if __name__ == "__main__":
    unittest.main()
