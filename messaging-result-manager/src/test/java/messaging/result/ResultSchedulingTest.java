package messaging.result;

import messaging.common.messages.HttpSendCommand;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ResultSchedulingTest {
    @Test
    void slowInboxDoesNotBlockOtherJobs() { verifyIndependentProgress(Job.INBOX); }

    @Test
    void slowExpiryDoesNotBlockOtherJobs() { verifyIndependentProgress(Job.EXPIRY); }

    @Test
    void slowFollowupDoesNotBlockOtherJobs() { verifyIndependentProgress(Job.FOLLOWUP); }

    private void verifyIndependentProgress(Job blockedJob) {
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withConfiguration(AutoConfigurations.of(TaskSchedulingAutoConfiguration.class))
                .withUserConfiguration(Jobs.class)
                .withBean(Probe.class, () -> new Probe(blockedJob))
                .withPropertyValues("messaging.result.webhook.initial-delay-ms=0", "messaging.result.webhook.poll-ms=20",
                        "messaging.result.expiry.initial-delay-ms=0",
                        "messaging.result.expiry.poll-ms=20",
                        "messaging.result.followup.initial-delay-ms=0",
                        "messaging.result.followup.poll-ms=20")
                .run(context -> {
                    assertNull(context.getStartupFailure());
                    var probe = context.getBean(Probe.class);
                    var scheduler = context.getBean(ThreadPoolTaskScheduler.class);
                    try {
                        assertTrue(probe.blockedJobStarted.await(5, TimeUnit.SECONDS));
                        assertTrue(probe.otherJobsAdvanced.await(5, TimeUnit.SECONDS),
                                "Both other scheduled jobs must advance while one poll is blocked");
                        assertEquals(1, probe.blockedQueries.get(), "The same poll must not overlap itself");
                        assertEquals(5, scheduler.getScheduledThreadPoolExecutor().getCorePoolSize());
                    } finally {
                        probe.releaseBlockedJob.countDown();
                    }
                });
    }

    enum Job { INBOX, EXPIRY, FOLLOWUP }

    static class Probe {
        final Job blockedJob;
        final CountDownLatch blockedJobStarted = new CountDownLatch(1);
        final CountDownLatch releaseBlockedJob = new CountDownLatch(1);
        final CountDownLatch otherJobsAdvanced = new CountDownLatch(2);
        final Set<Job> advanced = ConcurrentHashMap.newKeySet();
        final AtomicInteger blockedQueries = new AtomicInteger();

        Probe(Job blockedJob) { this.blockedJob = blockedJob; }

        QueryResponse query(Job job) throws InterruptedException {
            if (job == blockedJob) {
                blockedQueries.incrementAndGet();
                blockedJobStarted.countDown();
                if (!releaseBlockedJob.await(15, TimeUnit.SECONDS)) throw new AssertionError("Unreleased test job");
            } else if (blockedJobStarted.getCount() == 0 && releaseBlockedJob.getCount() == 1
                    && advanced.add(job)) {
                otherJobsAdvanced.countDown();
            }
            return QueryResponse.builder().build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    static class Jobs {
        @Bean PendingResultDispatcher pending(Probe probe) {
            var db = mock(DynamoDbClient.class);
            when(db.query(any(QueryRequest.class))).thenAnswer(call -> probe.query(Job.INBOX));
            return new PendingResultDispatcher(db, mock(MessageResultInboxStore.class),
                    mock(WebhookPrimaryDecisionService.class), mock(HttpFailureFollowupService.class),
                    mock(PrimaryStageDecisionStore.class), mock(SecondaryResultService.class),
                    JsonMapper.builder().build(), Clock.systemUTC(), 100, new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        }
        @Bean PrimaryExpiryDispatcher expiry(Probe probe) {
            var db = mock(DynamoDbClient.class);
            when(db.query(any(QueryRequest.class))).thenAnswer(call -> probe.query(Job.EXPIRY));
            return new PrimaryExpiryDispatcher(db, mock(PrimaryStageDecisionStore.class), Clock.systemUTC(), 100);
        }

        @Bean
        @SuppressWarnings("unchecked")
        FollowupHttpDispatcher followup(Probe probe) {
            var db = mock(DynamoDbClient.class);
            when(db.query(any(QueryRequest.class))).thenAnswer(call -> probe.query(Job.FOLLOWUP));
            var kafka = (KafkaTemplate<String, HttpSendCommand>) mock(KafkaTemplate.class);
            return new FollowupHttpDispatcher(db, JsonMapper.builder().build(), kafka, Clock.systemUTC(), 100);
        }
    }
}
