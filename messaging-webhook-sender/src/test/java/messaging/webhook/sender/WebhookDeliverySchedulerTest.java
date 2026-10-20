package messaging.webhook.sender;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WebhookDeliverySchedulerTest {
    private final WebhookDeliveryService service = mock(WebhookDeliveryService.class);

    @Test
    void aFullTurnRequeuesBehindWaitingCustomersWithoutAnotherTick() throws Exception {
        var seen = new CopyOnWriteArrayList<Long>();
        var done = new CountDownLatch(3);
        var first = new AtomicInteger();
        when(service.deliver(anyLong())).thenAnswer(call -> {
            long customer = call.getArgument(0);
            seen.add(customer);
            done.countDown();
            return customer == 42 && first.incrementAndGet() == 1;
        });
        try (var scheduler = new WebhookDeliveryScheduler(service, settings(1, 42L, 43L))) {
            scheduler.tick();
            assertTrue(done.await(3, TimeUnit.SECONDS));
        }
        assertEquals(List.of(42L, 43L, 42L), seen);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void idleOrFailedCustomersWaitForAnotherTickInsteadOfSpinning(boolean fails) throws Exception {
        var first = new CountDownLatch(1);
        var otherStarted = new CountDownLatch(1);
        var releaseOther = new CountDownLatch(1);
        var retried = new CountDownLatch(1);
        var attempts = new AtomicInteger();
        when(service.deliver(42)).thenAnswer(call -> {
            if (attempts.incrementAndGet() == 1) {
                first.countDown();
                if (fails) throw new IllegalStateException("SQL unavailable");
            } else retried.countDown();
            return false;
        });
        when(service.deliver(43)).thenAnswer(call -> {
            otherStarted.countDown();
            assertTrue(releaseOther.await(5, TimeUnit.SECONDS));
            return false;
        });
        try (var scheduler = new WebhookDeliveryScheduler(service, settings(1, 42L, 43L))) {
            try {
                scheduler.tick();
                assertTrue(first.await(3, TimeUnit.SECONDS));
                assertTrue(otherStarted.await(3, TimeUnit.SECONDS));
                assertEquals(1, attempts.get());
                releaseOther.countDown();
                assertFalse(retried.await(150, TimeUnit.MILLISECONDS), "No immediate retry after idle/failure");
                scheduler.tick();
                assertTrue(retried.await(3, TimeUnit.SECONDS));
            } finally {
                releaseOther.countDown();
            }
        }
        assertEquals(2, attempts.get());
    }

    @Test
    void repeatedTicksDoNotDuplicateCustomersOrExceedWorkerConcurrency() throws Exception {
        var started = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var third = new CountDownLatch(1);
        var active = ConcurrentHashMap.<Long>newKeySet();
        var maximum = new AtomicInteger();
        when(service.deliver(anyLong())).thenAnswer(call -> {
            long customer = call.getArgument(0);
            assertTrue(active.add(customer), "A customer must never overlap itself");
            maximum.accumulateAndGet(active.size(), Math::max);
            try {
                if (customer != 44) {
                    started.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } else third.countDown();
                return false;
            } finally {
                active.remove(customer);
            }
        });
        try (var scheduler = new WebhookDeliveryScheduler(service, settings(2, 42L, 43L, 44L))) {
            try {
                scheduler.tick();
                assertTrue(started.await(3, TimeUnit.SECONDS));
                for (int tick = 0; tick < 100; tick++) scheduler.tick();
                verify(service, times(2)).deliver(anyLong());
                release.countDown();
                assertTrue(third.await(3, TimeUnit.SECONDS));
            } finally {
                release.countDown();
            }
        }
        assertEquals(2, maximum.get());
        for (long customer : List.of(42L, 43L, 44L)) verify(service).deliver(customer);
    }

    @Test
    void closeDrainsTheCurrentTurnAndPreventsContinuationOrNewTicks() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        when(service.deliver(42)).thenAnswer(call -> {
            started.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return true;
        });
        var scheduler = new WebhookDeliveryScheduler(service, settings(1, 42L));
        var closer = new Thread(scheduler::close);
        try {
            scheduler.tick();
            assertTrue(started.await(3, TimeUnit.SECONDS));
            closer.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (closer.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) Thread.sleep(5);
            assertEquals(Thread.State.TIMED_WAITING, closer.getState());
            scheduler.tick();
            release.countDown();
            closer.join(3000);
            assertFalse(closer.isAlive());
            scheduler.tick();
            verify(service).deliver(42);
        } finally {
            release.countDown();
            closer.join(3000);
            scheduler.close();
        }
    }

    private static WebhookSenderProperties settings(int concurrency, Long... customers) {
        var destination = new WebhookSenderProperties.Destination(URI.create("https://example.org/hook"), false);
        var destinations = new java.util.HashMap<Long, WebhookSenderProperties.Destination>();
        for (long customer : customers) destinations.put(customer, destination);
        return new WebhookSenderProperties(true, 100, concurrency, 4, Duration.ofMillis(100),
                Duration.ZERO, 262144, Duration.ofSeconds(3), Duration.ofSeconds(30),
                Duration.ofSeconds(1), Duration.ofSeconds(60), Map.copyOf(destinations));
    }
}
