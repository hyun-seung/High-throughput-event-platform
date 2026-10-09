package messaging.result;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.task.TaskSchedulingAutoConfiguration;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ResultSchedulingTest {
    @Test
    void slowInboxDoesNotBlockOutbox() { verifyIndependentProgress(true); }

    @Test
    void slowOutboxDoesNotBlockInbox() { verifyIndependentProgress(false); }

    private void verifyIndependentProgress(boolean blockInbox) {
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withConfiguration(AutoConfigurations.of(TaskSchedulingAutoConfiguration.class))
                .withUserConfiguration(Jobs.class)
                .withBean(Probe.class, () -> new Probe(blockInbox))
                .withPropertyValues("messaging.result.webhook.initial-delay-ms=0", "messaging.result.webhook.poll-ms=20",
                        "messaging.result.outbox.enabled=true", "messaging.result.outbox.initial-delay-ms=0",
                        "messaging.result.outbox.poll-ms=20")
                .run(context -> {
                    assertNull(context.getStartupFailure());
                    var probe = context.getBean(Probe.class);
                    var scheduler = context.getBean(ThreadPoolTaskScheduler.class);
                    try {
                        assertTrue(probe.blockedJobStarted.await(5, TimeUnit.SECONDS));
                        assertTrue(probe.otherJobAdvanced.await(5, TimeUnit.SECONDS));
                        assertEquals(1, probe.blockedQueries.get(), "The same poll must not overlap itself");
                        assertEquals(5, scheduler.getScheduledThreadPoolExecutor().getCorePoolSize());
                    } finally {
                        probe.releaseBlockedJob.countDown();
                    }
                });
    }

    static class Probe {
        final boolean blockInbox;
        final CountDownLatch blockedJobStarted = new CountDownLatch(1);
        final CountDownLatch releaseBlockedJob = new CountDownLatch(1);
        final CountDownLatch otherJobAdvanced = new CountDownLatch(1);
        final AtomicInteger blockedQueries = new AtomicInteger();

        Probe(boolean blockInbox) { this.blockInbox = blockInbox; }

        QueryResponse query(boolean inbox) throws InterruptedException {
            if (inbox == blockInbox) {
                blockedQueries.incrementAndGet();
                blockedJobStarted.countDown();
                if (!releaseBlockedJob.await(15, TimeUnit.SECONDS)) throw new AssertionError("Unreleased test job");
            } else if (blockedJobStarted.getCount() == 0 && releaseBlockedJob.getCount() == 1) {
                otherJobAdvanced.countDown();
            }
            return QueryResponse.builder().build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    static class Jobs {
        @Bean PendingResultDispatcher pending(Probe probe) {
            var db = mock(DynamoDbClient.class);
            when(db.query(any(QueryRequest.class))).thenAnswer(call -> probe.query(true));
            return new PendingResultDispatcher(db, mock(MessageResultInboxStore.class),
                    mock(WebhookPrimaryDecisionService.class), mock(HttpFailureFollowupService.class),
                    mock(PrimaryStageDecisionStore.class), mock(SecondaryResultService.class),
                    JsonMapper.builder().build(), Clock.systemUTC(), 100);
        }
        @Bean PrimaryDecisionOutboxDispatcher outbox(Probe probe) {
            var db = mock(DynamoDbClient.class);
            when(db.query(any(QueryRequest.class))).thenAnswer(call -> probe.query(false));
            return new PrimaryDecisionOutboxDispatcher(db, JsonMapper.builder().build(),
                    mock(KafkaTemplate.class), Clock.systemUTC(), 100);
        }
    }
}
