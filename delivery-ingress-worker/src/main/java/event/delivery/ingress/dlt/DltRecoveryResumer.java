package event.delivery.ingress.dlt;

import tools.jackson.databind.json.JsonMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import event.common.recovery.FailureBackoff;

/** Bounded sequential operations worker; only persisted PENDING work is eligible for automatic retry. */
public final class DltRecoveryResumer {
    public record Cycle(int selected, List<DltRecoveryStore.Result> results, String status) {
        public Cycle(int selected, List<DltRecoveryStore.Result> results) { this(selected, results, "SCANNED"); }
    }
    private final DltRecoveryStore store;
    private final DltRecoveryPlanner planner;
    private final String cluster, targetTopic, sourceTopic;
    private final DltRecoveryStore.Publisher publisher;
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final FailureBackoff backoff;

    public DltRecoveryResumer(DltRecoveryStore store, DltRecoveryPlanner planner, String cluster,
                             String targetTopic, String sourceTopic, DltRecoveryStore.Publisher publisher) {
        this(store, planner, cluster, targetTopic, sourceTopic, publisher, DltBackoff.defaults());
    }
    public DltRecoveryResumer(DltRecoveryStore store, DltRecoveryPlanner planner, String cluster,
                             String targetTopic, String sourceTopic, DltRecoveryStore.Publisher publisher, FailureBackoff backoff) {
        this.store = store; this.planner = planner; this.cluster = cluster;
        this.targetTopic = targetTopic; this.sourceTopic = sourceTopic; this.publisher = publisher;
        this.backoff = backoff;
    }

    public synchronized Cycle runOnce(int limit, Duration minimumAge) {
        if (limit < 1 || limit > 100 || minimumAge == null || minimumAge.compareTo(Duration.ofSeconds(1)) < 0
                || minimumAge.compareTo(Duration.ofHours(1)) > 0) throw new IllegalArgumentException("Invalid resume bounds");
        var ticket = backoff.acquire();
        if (ticket == null) return new Cycle(0, List.of(), "BACKOFF");
        try {
            var cycle = cycle(ticket.probe() ? 1 : limit, minimumAge);
            if (cycle.status().equals("BACKEND_UNAVAILABLE")) backoff.failed(ticket); else backoff.succeeded(ticket);
            return cycle;
        } catch (RuntimeException failure) {
            if (DltBackoff.unavailable(failure)) backoff.failed(ticket); else backoff.abandon(ticket);
            throw failure;
        }
    }
    private Cycle cycle(int limit, Duration minimumAge) {
        var candidates = store.pending(cluster, targetTopic, limit, minimumAge);
        var results = new ArrayList<DltRecoveryStore.Result>();
        for (var candidate : candidates) {
            if (Thread.currentThread().isInterrupted()) break;
            try {
                var checkpoint = mapper.readValue(candidate.checkpointJson(), DltRecoveryCheckpoint.class);
                if (!sourceTopic.equals(checkpoint.plan().preview().dlt().source().topic())) {
                    results.add(new DltRecoveryStore.Result(candidate.operationId(), "SOURCE_MISMATCH"));
                    continue;
                }
                results.add(store.resume(cluster, targetTopic, candidate, checkpoint,
                        () -> planner.prepare(checkpoint.record(), checkpoint.plan().command()), publisher));
            } catch (RuntimeException failure) {
                // Keep durable work for inspection/retry. Never expose checkpoint payload or credentials.
                results.add(new DltRecoveryStore.Result(candidate.operationId(), "UNCONFIRMED", DltBackoff.unavailable(failure)));
            }
            if (results.getLast().backendUnavailable()) return new Cycle(candidates.size(), List.copyOf(results), "BACKEND_UNAVAILABLE");
        }
        return new Cycle(candidates.size(), List.copyOf(results));
    }
}
