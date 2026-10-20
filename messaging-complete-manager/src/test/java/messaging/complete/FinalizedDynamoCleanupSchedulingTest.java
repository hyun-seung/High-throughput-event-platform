package messaging.complete;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.SimpleTriggerContext;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class FinalizedDynamoCleanupSchedulingTest {
    private static final Instant NOW = Instant.parse("2026-10-20T00:00:00Z");
    private static final List<FinalizedDynamoCleanup.Pending> FULL_PAGE = List.of(
            new FinalizedDynamoCleanup.Pending("id", "decision", "PRIMARY"));
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final DynamoDbClient db = mock(DynamoDbClient.class);

    @Test
    void fullTurnContinuesImmediatelyButShortAndEmptyTurnsUseIdleDelay() {
        var worker = spy(worker(5000, 5000));
        doReturn(true).when(worker).cleanup(any());
        when(jdbc.query(anyString(), any(RowMapper.class), eq(1)))
                .thenReturn(FULL_PAGE, FULL_PAGE, List.of(), List.of());
        var registrar = new ScheduledTaskRegistrar();
        try {
            worker.configureTasks(registrar);
            var trigger = registrar.getTriggerTaskList().getFirst().getTrigger();
            var context = new SimpleTriggerContext(Clock.fixed(NOW, ZoneOffset.UTC));
            assertEquals(NOW.plusSeconds(5), trigger.nextExecution(context));

            worker.poll();
            // Fixed delay is measured from completion, not the previous start time.
            var completed = NOW.plusSeconds(2);
            context.update(NOW, NOW, completed);
            assertEquals(completed, trigger.nextExecution(context));
            worker.poll();
            assertEquals(completed.plusSeconds(5), trigger.nextExecution(context));
            worker.poll();
            assertEquals(completed.plusSeconds(5), trigger.nextExecution(context));
        } finally {
            registrar.destroy();
        }
    }

    @Test
    void failureAfterAFullTurnRestoresIdleDelay() {
        var worker = spy(worker(0, 5000));
        doReturn(true).when(worker).cleanup(any());
        when(jdbc.query(anyString(), any(RowMapper.class), eq(1)))
                .thenReturn(FULL_PAGE, FULL_PAGE).thenThrow(new IllegalStateException("SQL unavailable"));
        var registrar = new ScheduledTaskRegistrar();
        try {
            worker.configureTasks(registrar);
            var trigger = registrar.getTriggerTaskList().getFirst().getTrigger();
            var context = new SimpleTriggerContext(NOW, NOW, NOW.plusSeconds(2));
            worker.poll();
            assertEquals(NOW.plusSeconds(2), trigger.nextExecution(context));

            assertThrows(IllegalStateException.class, worker::poll);
            assertEquals(NOW.plusSeconds(7), trigger.nextExecution(context));
        } finally {
            registrar.destroy();
        }
    }

    @Test
    void interruptedTurnDoesNotKeepThePreviousImmediateContinuation() {
        var worker = spy(worker(0, 5000));
        doReturn(true).when(worker).cleanup(any());
        when(jdbc.query(anyString(), any(RowMapper.class), eq(1))).thenReturn(FULL_PAGE);
        var registrar = new ScheduledTaskRegistrar();
        try {
            worker.configureTasks(registrar);
            var trigger = registrar.getTriggerTaskList().getFirst().getTrigger();
            var context = new SimpleTriggerContext(NOW, NOW, NOW);
            worker.poll();
            assertEquals(NOW, trigger.nextExecution(context));
            Thread.currentThread().interrupt();
            try {
                worker.poll();
                assertTrue(Thread.currentThread().isInterrupted());
                assertEquals(NOW.plusSeconds(5), trigger.nextExecution(context));
            } finally {
                Thread.interrupted();
            }
            verify(jdbc, times(2)).query(anyString(), any(RowMapper.class), eq(1));
        } finally {
            registrar.destroy();
        }
    }

    @Test
    void springRunsAnotherBoundedTurnWithoutWaitingFiveSecondsAndDoesNotOverlap() {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var drained = new CountDownLatch(1);
        var unexpectedPoll = new CountDownLatch(1);
        var queries = new AtomicInteger();
        when(jdbc.query(anyString(), any(RowMapper.class), eq(1))).thenAnswer(call -> {
            int count = queries.incrementAndGet();
            if (count <= 2) return FULL_PAGE;
            if (count == 3) drained.countDown();
            else unexpectedPoll.countDown();
            return List.of();
        });
        when(db.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            entered.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return GetItemResponse.builder().build();
        });
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());

        runner("messaging.complete.cleanup.poll-ms=5000").run(context -> {
            assertNull(context.getStartupFailure());
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                var probe = new CountDownLatch(1);
                context.getBean(ThreadPoolTaskScheduler.class).schedule(probe::countDown, Instant.now());
                assertTrue(probe.await(3, TimeUnit.SECONDS));
                assertEquals(1, queries.get(), "No next page or overlapping poll while cleanup is running");
                release.countDown();
                assertTrue(drained.await(3, TimeUnit.SECONDS), "Full turn must resume before the 5s idle delay");
                assertFalse(unexpectedPoll.await(150, TimeUnit.MILLISECONDS), "Empty queue must not spin");
                assertEquals(3, queries.get());
            } finally {
                release.countDown();
            }
        });
        verify(db, times(2)).getItem(any(GetItemRequest.class));
    }

    @Test
    void springRetriesQueryFailureAfterIdleDelayEvenAfterAnImmediateTurn() {
        var retried = new CountDownLatch(1);
        var queries = new AtomicInteger();
        var failedAt = new AtomicLong();
        var retriedAt = new AtomicLong();
        when(jdbc.query(anyString(), any(RowMapper.class), eq(1))).thenAnswer(call -> {
            int count = queries.incrementAndGet();
            if (count <= 2) return FULL_PAGE;
            if (count == 3) {
                failedAt.set(System.nanoTime());
                throw new IllegalStateException("SQL unavailable");
            }
            retriedAt.compareAndSet(0, System.nanoTime());
            retried.countDown();
            return List.of();
        });
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());

        runner("messaging.complete.cleanup.poll-ms=200").run(context -> {
            assertNull(context.getStartupFailure());
            assertTrue(retried.await(3, TimeUnit.SECONDS), "A query failure must not stop future scheduling");
            assertTrue(retriedAt.get() - failedAt.get() >= TimeUnit.MILLISECONDS.toNanos(200),
                    "Failure must restore the configured delay instead of an immediate retry loop");
        });
    }

    @Test
    void disabledOrMissingEnabledPropertyDoesNotRegisterCleanup() {
        for (String[] properties : List.of(new String[0], new String[] {"messaging.complete.cleanup.enabled=false"})) {
            new ApplicationContextRunner().withUserConfiguration(Jobs.class).withPropertyValues(properties)
                    .run(context -> {
                        assertNull(context.getStartupFailure());
                        assertTrue(context.getBeansOfType(FinalizedDynamoCleanup.class).isEmpty());
                        assertTrue(context.getBean(ThreadPoolTaskScheduler.class)
                                .getScheduledThreadPoolExecutor().getQueue().isEmpty());
                    });
        }
    }

    @Test
    void rejectsNegativeInitialDelayAndNonPositiveIdleDelay() {
        assertThrows(IllegalArgumentException.class, () -> worker(-1, 5000));
        assertThrows(IllegalArgumentException.class, () -> worker(0, 0));
        assertThrows(IllegalArgumentException.class, () -> worker(0, -1));
    }

    private FinalizedDynamoCleanup worker(long initialDelayMs, long pollMs) {
        return new FinalizedDynamoCleanup(jdbc, db, Clock.fixed(NOW, ZoneOffset.UTC),
                1, 2, Duration.ofDays(7), new SimpleMeterRegistry(), 1, initialDelayMs, pollMs);
    }

    private ApplicationContextRunner runner(String pollProperty) {
        return new ApplicationContextRunner()
                .withInitializer(context -> context.getBeanFactory()
                        .setConversionService(ApplicationConversionService.getSharedInstance()))
                .withUserConfiguration(Jobs.class)
                .withBean(JdbcTemplate.class, () -> jdbc)
                .withBean(DynamoDbClient.class, () -> db)
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .withPropertyValues("messaging.complete.cleanup.enabled=true", "messaging.complete.cleanup.page-size=1",
                        "messaging.complete.cleanup.max-pages-per-poll=2", "messaging.complete.cleanup.concurrency=1",
                        "messaging.complete.cleanup.initial-delay-ms=0", pollProperty);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    @Import(FinalizedDynamoCleanup.class)
    static class Jobs {
        @Bean
        ThreadPoolTaskScheduler taskScheduler() {
            var scheduler = new ThreadPoolTaskScheduler();
            scheduler.setPoolSize(2);
            return scheduler;
        }
    }
}
