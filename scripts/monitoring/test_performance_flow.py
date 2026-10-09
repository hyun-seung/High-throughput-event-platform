"""Regression checks for performance accounting; no application or network dependencies."""

import unittest

from performance_flow import case_for, distribution, reconcile, summarize


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


if __name__ == "__main__":
    unittest.main()
