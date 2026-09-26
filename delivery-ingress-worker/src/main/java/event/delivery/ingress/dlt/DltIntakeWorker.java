package event.delivery.ingress.dlt;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import event.common.recovery.FailureBackoff;

/** Opt-in intake: durable raw archive first, existing fenced recovery second. */
public final class DltIntakeWorker {
    @FunctionalInterface public interface Source { DltInspector.RawPage read(long offset, int limit); }
    public record Cycle(String status, long nextOffset, long beginningOffset, long snapshotEndOffset,
                        int archived, List<DltIntakeStore.Outcome> outcomes, DltIntakeStore.Backlog backlog) { }
    private final DltIntakeStore intake;
    private final DltRecoveryStore recovery;
    private final DltRecoveryPlanner planner;
    private final DltIntakeStore.Scope scope;
    private final Source source;
    private final DltRecoveryStore.Publisher publisher;
    private final FailureBackoff backoff;
    public DltIntakeWorker(DltIntakeStore intake, DltRecoveryStore recovery, DltRecoveryPlanner planner,
                           DltIntakeStore.Scope scope, Source source, DltRecoveryStore.Publisher publisher) {
        this(intake, recovery, planner, scope, source, publisher, DltBackoff.defaults());
    }
    public DltIntakeWorker(DltIntakeStore intake, DltRecoveryStore recovery, DltRecoveryPlanner planner,
                           DltIntakeStore.Scope scope, Source source, DltRecoveryStore.Publisher publisher, FailureBackoff backoff) {
        this.intake = intake; this.recovery = recovery; this.planner = planner;
        this.scope = scope; this.source = source; this.publisher = publisher;
        this.backoff = backoff;
    }
    public synchronized Cycle runOnce(long initialOffset, int limit) {
        if (limit < 1 || limit > 100) throw new IllegalArgumentException("Limit 1..100 required");
        var ticket = backoff.acquire();
        if (ticket == null) return new Cycle("BACKOFF", -1, -1, -1, 0, List.of(), null);
        try {
            var cycle = cycle(initialOffset, ticket.probe() ? 1 : limit);
            if (cycle.status().equals("BACKEND_UNAVAILABLE")) backoff.failed(ticket); else backoff.succeeded(ticket);
            return cycle;
        } catch (RuntimeException failure) {
            if (DltBackoff.unavailable(failure)) backoff.failed(ticket); else backoff.abandon(ticket);
            throw failure;
        }
    }
    private Cycle cycle(long initialOffset, int limit) {
        long offset = intake.cursor(scope, initialOffset);
        var outcomes = new ArrayList<DltIntakeStore.Outcome>();
        var attempted = new HashSet<java.util.UUID>();
        // Already archived work can progress even if Kafka retention has since passed the cursor.
        if (!drain(limit, outcomes, attempted)) return new Cycle("BACKEND_UNAVAILABLE", offset, -1, -1, 0, List.copyOf(outcomes), null);
        var page = source.read(offset, limit);
        if (offset < page.beginningOffset() || offset > page.snapshotEndOffset()) {
            return new Cycle(offset < page.beginningOffset() ? "RETENTION_GAP" : "OFFSET_AFTER_END", offset,
                    page.beginningOffset(), page.snapshotEndOffset(), 0, List.copyOf(outcomes), intake.backlog(scope));
        }
        boolean saved = intake.archive(scope, offset, page);
        if (outcomes.size() < limit && !drain(limit - outcomes.size(), outcomes, attempted))
            return new Cycle("BACKEND_UNAVAILABLE", saved ? page.nextOffset() : -1, page.beginningOffset(), page.snapshotEndOffset(),
                    saved ? page.records().size() : 0, List.copyOf(outcomes), null);
        return new Cycle(saved ? "SCANNED" : "CURSOR_MOVED", intake.cursor(scope, initialOffset),
                page.beginningOffset(), page.snapshotEndOffset(), saved ? page.records().size() : 0,
                List.copyOf(outcomes), intake.backlog(scope));
    }
    private boolean drain(int limit, List<DltIntakeStore.Outcome> outcomes, Set<java.util.UUID> attempted) {
        for (var id : intake.pending(scope, limit)) {
            if (Thread.currentThread().isInterrupted()) return true;
            if (!attempted.add(id)) continue;
            outcomes.add(intake.process(id, record -> {
                var plan = planner.plan(record);
                if (!plan.eligible()) return new DltIntakeStore.Outcome("HELD", plan.preview().decision().name(), null);
                var result = recovery.apply(scope.cluster(), scope.targetTopic(), plan, "dlt-intake", "Recover archived DLT record",
                        () -> planner.prepare(record, plan.command()), publisher, DltRecoveryCheckpoint.capture(plan, record));
                String state = switch (result.status()) {
                    case "ACKNOWLEDGED", "ALREADY_ACKNOWLEDGED", "UNCONFIRMED" -> "REGISTERED";
                    case "HELD_STATE_CHANGED" -> "HELD";
                    default -> "NEW";
                };
                return new DltIntakeStore.Outcome(state, result.status(), result.operationId(), result.backendUnavailable());
            }));
            if (outcomes.getLast().backendUnavailable()) return false;
        }
        return true;
    }
}
