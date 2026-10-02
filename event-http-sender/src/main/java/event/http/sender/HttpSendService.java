package event.http.sender;

import event.common.events.EventSubmission;
import event.common.events.EventTopics;
import event.common.events.HttpOutcome;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

@Service
public class HttpSendService {
    private final HttpAttemptRepository attempts;
    private final HttpProviderClient provider;
    private final KafkaTemplate<String, HttpOutcome> kafka;
    private final HttpSenderProperties properties;
    private final Clock clock;

    public HttpSendService(HttpAttemptRepository attempts, HttpProviderClient provider,
                           KafkaTemplate<String, HttpOutcome> kafka, HttpSenderProperties properties, Clock clock) {
        this.attempts = attempts;
        this.provider = provider;
        this.kafka = kafka;
        this.properties = properties;
        this.clock = clock;
    }

    public void send(EventSubmission event) {
        Instant now = clock.instant();
        Instant deadline = event.receivedAt().plus(properties.primaryTtl());
        var claim = attempts.claim(event, properties.provider(), now, now.plus(properties.leaseDuration()), deadline);
        if (claim.state() == HttpAttemptRepository.State.INELIGIBLE
                || claim.state() == HttpAttemptRepository.State.IN_PROGRESS) return;
        if (claim.state() == HttpAttemptRepository.State.OBSERVED) {
            publish(claim.outcome());
            return;
        }
        HttpProviderClient.Observation observed = now.isBefore(deadline)
                ? provider.send(event, claim.attemptId(), 1)
                : new HttpProviderClient.Observation(HttpOutcome.Kind.EXPIRED, null);
        Instant observedAt = clock.instant();
        String outcomeId = UUID.nameUUIDFromBytes(("http-outcome:" + event.executionId() + ":1")
                .getBytes(StandardCharsets.UTF_8)).toString();
        var outcome = new HttpOutcome(outcomeId, event.executionId(), claim.attemptId(), 1, 1,
                observed.kind(), observedAt, observed.providerProcessedAt());
        attempts.record(outcome);
        publish(outcome);
    }

    private void publish(HttpOutcome outcome) {
        try {
            kafka.send(EventTopics.HTTP_OUTCOME, outcome.executionId(), outcome).get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("HTTP outcome publication interrupted", interrupted);
        } catch (Exception uncertain) {
            throw new IllegalStateException("HTTP outcome publication not confirmed", uncertain);
        }
        attempts.published(outcome);
    }
}
