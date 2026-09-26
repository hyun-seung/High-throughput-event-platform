package event.common.recovery;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class FailureBackoffTest {
    @Test void exponentialDelayCapsAndSuccessfulProbeResets() {
        var now = new AtomicLong(); var gate = new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(4), now::get, () -> 1);
        assertTrue(gate.failed(gate.acquire())); assertEquals(1, gate.remainingSeconds()); assertNull(gate.acquire());
        now.addAndGet(1_000_000_000); assertTrue(gate.failed(gate.acquire())); assertEquals(2, gate.remainingSeconds());
        now.addAndGet(2_000_000_000); gate.failed(gate.acquire()); assertEquals(4, gate.remainingSeconds());
        now.addAndGet(4_000_000_000L); gate.failed(gate.acquire()); assertEquals(4, gate.remainingSeconds());
        now.addAndGet(4_000_000_000L); gate.succeeded(gate.acquire()); assertFalse(gate.blocked());
        gate.failed(gate.acquire()); assertEquals(1, gate.remainingSeconds());
    }
    @Test void staleSuccessAndFailuresCannotResetOrInflateCurrentOutage() {
        var now = new AtomicLong(); var gate = new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(4), now::get, () -> 1);
        var a = gate.acquire(); var b = gate.acquire(); gate.failed(a);
        gate.succeeded(b); assertFalse(gate.failed(b)); assertNull(gate.acquire()); assertEquals(1, gate.remainingSeconds());
        now.set(1_000_000_000); var probe = gate.acquire(); gate.succeeded(probe);
        assertFalse(gate.failed(a)); assertNotNull(gate.acquire());
    }
    @Test void concurrentCallersAdmitOnlyOneProbe() throws Exception {
        var now = new AtomicLong(); var gate = new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(4), now::get, () -> 1);
        gate.failed(gate.acquire()); now.set(1_000_000_000); var count = new AtomicInteger(); var start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(8)) {
            var tasks = new java.util.ArrayList<Future<?>>();
            for (int i = 0; i < 32; i++) tasks.add(executor.submit(() -> { start.await(); if (gate.acquire() != null) count.incrementAndGet(); return null; }));
            start.countDown(); for (var task : tasks) task.get(2, TimeUnit.SECONDS);
        }
        assertEquals(1, count.get()); assertTrue(gate.blocked());
    }
    @Test void unusedProbeCanBeReleasedAndJitterStaysWithinConfiguredWindow() {
        var now = new AtomicLong(); var gate = new FailureBackoff(Duration.ofSeconds(2), Duration.ofSeconds(4), now::get, () -> 0.5);
        gate.failed(gate.acquire()); assertEquals(1, gate.remainingSeconds()); now.set(1_000_000_000);
        var probe = gate.acquire(); gate.abandon(probe); assertNotNull(gate.acquire());
    }
    @Test void invalidWindowsFailAtStartup() {
        assertThrows(IllegalArgumentException.class, () -> new FailureBackoff(Duration.ZERO, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new FailureBackoff(Duration.ofSeconds(2), Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new FailureBackoff(Duration.ofSeconds(1), Duration.ofHours(2)));
    }
}
