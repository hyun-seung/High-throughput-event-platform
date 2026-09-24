package event.delivery.dispatch.service;

import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.external.client.ProviderFailureException.Kind;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.model.DispatchFailureDecision.State;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class DispatchRetryPolicyTest {
    private static final Instant NOW = Instant.parse("2026-09-24T00:00:00Z");
    private final DispatchProperties properties = new DispatchProperties("test", Duration.ofSeconds(30),
            Duration.ofHours(3), Duration.ofSeconds(2));

    @ParameterizedTest
    @CsvSource({"RETRY_1S,1", "RETRY_10S,10", "NO_RESPONSE,2"})
    void failureSpecificDelayAndLimitAreApplied(Kind kind, int seconds) {
        var first = DispatchRetryPolicy.decide(attempt(0, NOW.plusSeconds(100)), kind, NOW, properties);
        assertEquals(State.RETRY_SCHEDULED, first.state());
        assertEquals(NOW.plusSeconds(seconds), first.nextAttemptAt());
        var exhausted = DispatchRetryPolicy.decide(attempt(3, NOW.plusSeconds(100)), kind, NOW, properties);
        assertEquals(State.DECISION_PENDING, exhausted.state());
        assertEquals("RETRY_EXHAUSTED_" + kind, exhausted.reason());
        assertNull(exhausted.nextAttemptAt());
    }

    @ParameterizedTest
    @CsvSource({"FALLBACK_REQUIRED,DECISION_PENDING", "PERMANENT_REJECTION,DECISION_PENDING",
            "INVALID_RESPONSE,REVIEW_REQUIRED", "HTTP_ERROR,REVIEW_REQUIRED"})
    void nonRetryOutcomesCannotScheduleAnotherCall(Kind kind, State state) {
        var result = DispatchRetryPolicy.decide(attempt(0, NOW.plusSeconds(100)), kind, NOW, properties);
        assertEquals(state, result.state());
        assertNull(result.nextAttemptAt());
    }

    @Test
    void wakeAtDeadlineWithoutSendingEarlierThanRequestedDelay() {
        var deadline = NOW.plusSeconds(5);
        var result = DispatchRetryPolicy.decide(attempt(0, deadline), Kind.RETRY_10S, NOW, properties);
        assertEquals(deadline, result.nextAttemptAt());
        var expired = DispatchRetryPolicy.decide(attempt(0, deadline), Kind.RETRY_10S, deadline.plusSeconds(1), properties);
        assertEquals(State.DECISION_PENDING, expired.state());
        assertEquals("PRIMARY_EXPIRED", expired.reason());
        assertEquals(deadline, expired.observedAt(), "Expiry decision anchor must not move with delayed recovery");
    }

    private DispatchAttempt attempt(int retryCount, Instant deadline) {
        return new DispatchAttempt("delivery", "attempt", "test", retryCount + 1, retryCount, deadline);
    }
}
