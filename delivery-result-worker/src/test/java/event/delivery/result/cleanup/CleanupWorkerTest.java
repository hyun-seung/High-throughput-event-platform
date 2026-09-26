package event.delivery.result.cleanup;

import event.common.lifecycle.DeliveryCompactor;
import event.common.lifecycle.DeliveryFinalized;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CleanupWorkerTest {
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    @AfterEach void closeMeters() { meters.close(); }
    @Test void actualStorageFailureStopsDrainAndDefersFurtherClaimsUntilProbe() {
        var repository = mock(CleanupRepository.class); var compactor = mock(DeliveryCompactor.class);
        var claim = new CleanupRepository.Claim(mock(DeliveryFinalized.class), UUID.randomUUID());
        when(repository.claim()).thenReturn(Optional.of(claim), Optional.empty());
        when(compactor.compact(claim.result())).thenThrow(software.amazon.awssdk.core.exception.SdkClientException.create("DDB down"));
        var nanos = new java.util.concurrent.atomic.AtomicLong();
        var gate = new event.common.recovery.FailureBackoff(java.time.Duration.ofSeconds(1), java.time.Duration.ofSeconds(4), nanos::get, () -> 1);
        try (var worker = new CleanupWorker(repository, compactor, meters, 1, 20, gate)) {
            worker.tick(); verify(repository).retry(claim); verify(repository, never()).done(any());
            for (int i = 0; i < 100; i++) worker.tick(); verify(repository, times(1)).claim();
            nanos.set(1_000_000_000); worker.tick(); verify(repository, times(2)).claim(); assertFalse(gate.blocked());
        }
    }
    @Test void sqlFailureDuringRetryAlsoPausesEvenWhenOriginalErrorWasDataSpecific() {
        var repository = mock(CleanupRepository.class); var compactor = mock(DeliveryCompactor.class);
        var claim = new CleanupRepository.Claim(mock(DeliveryFinalized.class), UUID.randomUUID());
        when(repository.claim()).thenReturn(Optional.of(claim));
        when(compactor.compact(claim.result())).thenThrow(new IllegalStateException("completion proof mismatch"));
        doThrow(new org.springframework.dao.DataAccessResourceFailureException("SQL down")).when(repository).retry(claim);
        try (var worker = new CleanupWorker(repository, compactor, meters, 1, 20)) {
            worker.tick(); worker.tick(); verify(repository, times(1)).claim(); verify(repository).retry(claim);
        }
    }
    @Test void boundedLanesDoNotPreclaimOrOverlapTicks() throws Exception {
        var repository = mock(CleanupRepository.class);
        var compactor = mock(DeliveryCompactor.class);
        var result = mock(DeliveryFinalized.class);
        when(repository.claim()).thenAnswer(call -> Optional.of(new CleanupRepository.Claim(result, UUID.randomUUID())));
        when(repository.done(any())).thenReturn(true);
        var active = new AtomicInteger(); var maximum = new AtomicInteger();
        var started = new CountDownLatch(4); var release = new CountDownLatch(1);
        when(compactor.compact(result)).thenAnswer(call -> {
            int count = active.incrementAndGet(); maximum.accumulateAndGet(count, Math::max);
            started.countDown();
            try { assertTrue(release.await(5, TimeUnit.SECONDS)); return true; }
            finally { active.decrementAndGet(); }
        });
        try (var worker = new CleanupWorker(repository, compactor, meters, 4, 3);
             var scheduler = Executors.newFixedThreadPool(2)) {
            var first = scheduler.submit(worker::tick);
            Future<?> second;
            try {
                assertTrue(started.await(5, TimeUnit.SECONDS));
                second = scheduler.submit(worker::tick);
                verify(repository, times(4)).claim(); // SQL work is claimed only when a lane can execute it.
                assertEquals(4, meters.get("delivery.cleanup.active").gauge().value());
            } finally { release.countDown(); }
            first.get(5, TimeUnit.SECONDS); second.get(5, TimeUnit.SECONDS);
            verify(repository, times(24)).claim(); // 2 ticks * 4 lanes * 3 jobs, no unbounded drain.
            verify(repository, times(24)).done(any());
            assertEquals(4, maximum.get());
            assertEquals(0, meters.get("delivery.cleanup.active").gauge().value());
            assertEquals(24, meters.get("delivery.cleanup.duration").timer().count());
        }
    }

    @Test void sqlOutageStopsEachLaneWithoutBusyRetryAndClosedWorkerDoesNotClaim() {
        var repository = mock(CleanupRepository.class); var compactor = mock(DeliveryCompactor.class);
        when(repository.claim()).thenThrow(new IllegalStateException("SQL unavailable"));
        try (var worker = new CleanupWorker(repository, compactor, meters, 4, 20)) {
            worker.tick();
            verify(repository, times(4)).claim(); verifyNoInteractions(compactor);
            assertEquals(4, meters.get("delivery.cleanup.events").tag("outcome", "retained").counter().count());
            worker.close(); worker.tick(); verify(repository, times(4)).claim();
        }
    }

    @Test void failedDeletionAndFailedRetryUpdateLeaveClaimForLeaseRecovery() {
        var repository = mock(CleanupRepository.class); var compactor = mock(DeliveryCompactor.class);
        var claim = new CleanupRepository.Claim(mock(DeliveryFinalized.class), UUID.randomUUID());
        when(repository.claim()).thenReturn(Optional.of(claim));
        when(compactor.compact(claim.result())).thenThrow(new IllegalStateException("DDB unavailable"));
        doThrow(new IllegalStateException("SQL unavailable")).when(repository).retry(claim);
        try (var worker = new CleanupWorker(repository, compactor, meters, 1, 1)) {
            worker.tick();
            verify(repository, never()).done(any()); verify(repository).retry(claim);
            assertEquals(1, meters.get("delivery.cleanup.events").tag("outcome", "retained").counter().count());
        }
    }

    @Test void lostLeaseDoesNotCountAsCompletedCleanup() {
        var repository = mock(CleanupRepository.class); var compactor = mock(DeliveryCompactor.class);
        var claim = new CleanupRepository.Claim(mock(DeliveryFinalized.class), UUID.randomUUID());
        when(repository.claim()).thenReturn(Optional.of(claim));
        when(compactor.compact(claim.result())).thenReturn(true);
        when(repository.done(claim)).thenReturn(false);
        try (var worker = new CleanupWorker(repository, compactor, meters, 1, 1)) {
            worker.tick();
            assertEquals(1, meters.get("delivery.cleanup.events").tag("outcome", "lease_lost").counter().count());
            assertNull(meters.find("delivery.cleanup.events").tag("outcome", "compacted").counter());
            verify(repository, never()).retry(any());
        }
    }

    @Test void invalidLimitsFailBeforeStartingWorkers() {
        for (int[] settings : new int[][]{{0, 20}, {17, 20}, {4, 0}, {4, 1001}})
            assertThrows(IllegalArgumentException.class, () -> new CleanupWorker(null, null, meters, settings[0], settings[1]));
    }

    @Test void interruptedTickRetainsClaimAndStopsFurtherWork() throws Exception {
        var repository = mock(CleanupRepository.class); var compactor = mock(DeliveryCompactor.class);
        var claim = new CleanupRepository.Claim(mock(DeliveryFinalized.class), UUID.randomUUID());
        when(repository.claim()).thenReturn(Optional.of(claim));
        var entered = new CountDownLatch(1); var blocked = new CountDownLatch(1);
        var retained = new CountDownLatch(1);
        when(compactor.compact(claim.result())).thenAnswer(call -> {
            entered.countDown();
            try { blocked.await(); return true; }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt(); throw new IllegalStateException("Interrupted DDB call", interrupted);
            }
        });
        doAnswer(call -> { retained.countDown(); return null; }).when(repository).retry(claim);
        try (var worker = new CleanupWorker(repository, compactor, meters, 1, 20)) {
            var tick = Thread.ofPlatform().start(worker::tick);
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS)); tick.interrupt(); tick.join(5000);
                assertFalse(tick.isAlive()); assertTrue(tick.isInterrupted());
                assertTrue(retained.await(5, TimeUnit.SECONDS));
            } finally { tick.interrupt(); blocked.countDown(); tick.join(5000); }
        }
        verify(repository, times(1)).claim(); verify(repository, never()).done(any());
    }
}
