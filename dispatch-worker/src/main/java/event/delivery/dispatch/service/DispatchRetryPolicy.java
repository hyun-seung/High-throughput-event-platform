package event.delivery.dispatch.service;

import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.external.client.ProviderFailureException;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.model.DispatchFailureDecision;

import java.time.Duration;
import java.time.Instant;

import static event.delivery.dispatch.model.DispatchFailureDecision.State.*;

public final class DispatchRetryPolicy {
    public static final int MAX_RETRIES = 3;

    private DispatchRetryPolicy() { }

    public static DispatchFailureDecision decide(DispatchAttempt attempt, ProviderFailureException.Kind kind,
                                                  Instant now, DispatchProperties properties) {
        if (!now.isBefore(attempt.primaryDeadline())) {
            return expired(attempt.primaryDeadline());
        }
        Duration delay = switch (kind) {
            case RETRY_1S -> Duration.ofSeconds(1);
            case RETRY_10S -> Duration.ofSeconds(10);
            case NO_RESPONSE -> properties.noResponseRetryDelay();
            default -> null;
        };
        if (delay != null) {
            if (attempt.retryCount() >= MAX_RETRIES) {
                return new DispatchFailureDecision(DECISION_PENDING, "RETRY_EXHAUSTED_" + kind, now, null);
            }
            Instant due = now.plus(delay);
            // Wake at the deadline to record expiry, never send early to squeeze in a retry.
            return new DispatchFailureDecision(RETRY_SCHEDULED, kind.name(), now,
                    due.isBefore(attempt.primaryDeadline()) ? due : attempt.primaryDeadline());
        }
        return new DispatchFailureDecision(switch (kind) {
            case FALLBACK_REQUIRED, PERMANENT_REJECTION -> DECISION_PENDING;
            default -> REVIEW_REQUIRED;
        }, kind.name(), now, null);
    }

    public static DispatchFailureDecision expired(Instant deadline) {
        return new DispatchFailureDecision(DECISION_PENDING, "PRIMARY_EXPIRED", deadline, null);
    }
}
