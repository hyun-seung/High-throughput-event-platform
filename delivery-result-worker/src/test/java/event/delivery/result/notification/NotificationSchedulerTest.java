package event.delivery.result.notification;

import event.common.recovery.FailureBackoff;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.springframework.dao.DataAccessResourceFailureException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class NotificationSchedulerTest {
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final AtomicLong nanos = new AtomicLong();
    @AfterEach void closeMeters() { meters.close(); }
    NotificationProperties settings() {
        var destination = new NotificationProperties.Destination(URI.create("http://localhost:9999"), "x".repeat(32));
        return new NotificationProperties(true, 1, 100, Duration.ofSeconds(1), Duration.ofSeconds(10),
                Duration.ofSeconds(1), Duration.ofSeconds(60), 16384, Map.of(42L, destination, 43L, destination));
    }
    void idle() throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (meters.get("delivery.notification.active").gauge().value() != 0 && System.nanoTime() < end) Thread.sleep(1);
        assertEquals(0, meters.get("delivery.notification.active").gauge().value());
    }
    @Test void sqlOutageSkipsRepeatedPollingAndAllowsOnlyOneProbe() throws Exception {
        var service = mock(NotificationService.class); var count = new java.util.concurrent.atomic.AtomicInteger();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        doAnswer(call -> {
            if (count.incrementAndGet() == 1) throw new DataAccessResourceFailureException("SQL down");
            entered.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS)); return null;
        }).when(service).deliver(anyLong());
        var gate = new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(4), nanos::get, () -> 1);
        try (var scheduler = new NotificationScheduler(service, settings(), meters, gate)) {
            scheduler.tick(); idle();
            for (int i = 0; i < 100; i++) scheduler.tick(); assertEquals(1, count.get());
            nanos.set(1_000_000_000); scheduler.tick(); assertTrue(entered.await(3, TimeUnit.SECONDS));
            for (int i = 0; i < 100; i++) scheduler.tick(); assertEquals(2, count.get());
            release.countDown(); idle(); scheduler.tick(); idle(); assertTrue(count.get() >= 3);
        } finally { release.countDown(); }
    }
    @Test void oneCustomersDataErrorDoesNotPauseOtherCustomers() throws Exception {
        var service = mock(NotificationService.class);
        doThrow(new IllegalStateException("invalid immutable batch")).when(service).deliver(42L);
        try (var scheduler = new NotificationScheduler(service, settings(), meters)) {
            scheduler.tick(); idle(); scheduler.tick(); idle();
            verify(service, atLeastOnce()).deliver(43L);
            assertEquals(0, meters.get("delivery.notification.recovery.delay").gauge().value());
        }
    }
}
