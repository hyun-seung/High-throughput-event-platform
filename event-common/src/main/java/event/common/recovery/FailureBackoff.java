package event.common.recovery;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.*;

/** Process-local admission only: never owns a durable deadline or a delivery claim. */
public final class FailureBackoff {
    public record Ticket(long generation, boolean probe) { }
    private final long initialNanos, maxNanos;
    private final LongSupplier nanos;
    private final DoubleSupplier jitter;
    private long generation, delay, retryAt;
    private boolean open, probing;
    private Ticket healthy = new Ticket(0, false);
    public FailureBackoff(Duration initial, Duration maximum) {
        this(initial, maximum, System::nanoTime, () -> ThreadLocalRandom.current().nextDouble(0.5, 1.0));
    }
    public FailureBackoff(Duration initial, Duration maximum, LongSupplier nanos, DoubleSupplier jitter) {
        if (initial == null || maximum == null || initial.toMillis() < 1 || maximum.compareTo(initial) < 0
                || maximum.compareTo(Duration.ofHours(1)) > 0) throw new IllegalArgumentException("Backoff must satisfy 1ms <= initial <= maximum <= 1h");
        initialNanos = initial.toNanos(); maxNanos = maximum.toNanos(); this.nanos = nanos; this.jitter = jitter;
    }
    public synchronized Ticket acquire() {
        if (!open) return healthy;
        if (probing || nanos.getAsLong() - retryAt < 0) return null;
        probing = true; return new Ticket(generation, true);
    }
    public synchronized boolean blocked() { return open && (probing || nanos.getAsLong() - retryAt < 0); }
    public synchronized boolean failed(Ticket ticket) {
        if (ticket.generation() != generation) return false; // Ignore stale in-flight outcomes.
        delay = delay == 0 ? initialNanos : Math.min(maxNanos, delay * 2);
        double fraction = Math.max(0.5, Math.min(1.0, jitter.getAsDouble()));
        retryAt = nanos.getAsLong() + (long) (delay * fraction);
        generation++; open = true; probing = false; return true;
    }
    public synchronized void succeeded(Ticket ticket) {
        if (ticket.generation() != generation || !ticket.probe()) return;
        generation++; open = false; probing = false; delay = 0; healthy = new Ticket(generation, false);
    }
    public synchronized void abandon(Ticket ticket) {
        if (ticket.generation() == generation && ticket.probe()) probing = false;
    }
    public synchronized double remainingSeconds() { return open ? Math.max(0, retryAt - nanos.getAsLong()) / 1_000_000_000d : 0; }
}
