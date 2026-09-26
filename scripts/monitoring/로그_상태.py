"""A successful data reconciliation does not make a broker error a clean baseline."""
import re

SIGNALS = {
    'negative_histogram': re.compile(r'Histogram recorded value cannot be negative', re.I),
    'coordinator_out_of_sync': re.compile(r'state machine of the coordinator .*out of sync', re.I),
    'offset_commit_failed': re.compile(r'Offset commit failed', re.I),
    'error_level': re.compile(r'\bERROR\b'),
}


def inspect_log(text):
    counts = {name: 0 for name in SIGNALS}
    samples = []
    for line in text.splitlines():
        matches = [name for name, pattern in SIGNALS.items() if pattern.search(line)]
        for name in matches:
            counts[name] += 1
        if matches and len(samples) < 20:
            samples.append(line)
    return {'pass': not any(counts.values()), 'signals': counts, 'sampleLines': samples}
