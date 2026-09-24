import unittest
from log_health import inspect_log


class LogHealthTest(unittest.TestCase):
    def test_broker_histogram_failure_is_not_a_clean_baseline(self):
        report = inspect_log('[2026-09-24] ERROR [GroupCoordinator id=1] Writing records failed: Histogram recorded value cannot be negative.')
        self.assertFalse(report['pass'])
        self.assertEqual(1, report['signals']['negative_histogram'])

    def test_warning_level_commit_failure_also_fails(self):
        report = inspect_log('2026-09-24 WARN ConsumerCoordinator : Offset commit failed on partition p-0: This is not the correct coordinator.')
        self.assertFalse(report['pass'])
        self.assertEqual(1, report['signals']['offset_commit_failed'])

    def test_recovery_message_does_not_erase_previous_failure(self):
        report = inspect_log('ERROR The state machine of the coordinator offsets-8 is out of sync\nINFO Finished loading of metadata')
        self.assertFalse(report['pass'])
        self.assertEqual(1, report['signals']['coordinator_out_of_sync'])

    def test_normal_observations_pass(self):
        self.assertTrue(inspect_log('INFO Delivery stage observed\nINFO partitions assigned: p-0')['pass'])


if __name__ == '__main__': unittest.main()
