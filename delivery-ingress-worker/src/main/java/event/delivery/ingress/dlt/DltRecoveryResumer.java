package event.delivery.ingress.dlt;

import tools.jackson.databind.json.JsonMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Bounded sequential operations worker; only persisted PENDING work is eligible for automatic retry. */
public final class DltRecoveryResumer {
    public record Cycle(int selected, List<DltRecoveryStore.Result> results) { }
    private final DltRecoveryStore store;
    private final DltRecoveryPlanner planner;
    private final String cluster, targetTopic, sourceTopic;
    private final DltRecoveryStore.Publisher publisher;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public DltRecoveryResumer(DltRecoveryStore store, DltRecoveryPlanner planner, String cluster,
                             String targetTopic, String sourceTopic, DltRecoveryStore.Publisher publisher) {
        this.store = store; this.planner = planner; this.cluster = cluster;
        this.targetTopic = targetTopic; this.sourceTopic = sourceTopic; this.publisher = publisher;
    }

    public Cycle runOnce(int limit, Duration minimumAge) {
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
                results.add(new DltRecoveryStore.Result(candidate.operationId(), "UNCONFIRMED"));
            }
        }
        return new Cycle(candidates.size(), List.copyOf(results));
    }
}
