package event.delivery.dispatch.service;

import event.common.delivery.DeliveryEvent;
import event.common.metrics.DeliveryAudit;
import event.common.metrics.DeliveryMetrics;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.config.SecondaryDispatchProperties;
import event.delivery.dispatch.external.client.ProviderFailureException;
import event.delivery.dispatch.model.DispatchFailureDecision;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.model.DispatchClaimStatus;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.port.DispatchAttemptStore;
import event.delivery.dispatch.port.SecondaryProviderClient;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Clock;

@Service
@RequiredArgsConstructor
public class SecondaryDispatchService {
    private final DispatchAttemptStore store;
    private final SecondaryProviderClient client;
    private final SecondaryDispatchProperties properties;
    private final DispatchProperties dispatchProperties;
    private final Clock dispatchClock;
    private final DeliveryMetrics metrics;

    public void dispatch(DeliveryEvent event, String primaryAttemptId) {
        var planned = store.prepareSecondary(event, primaryAttemptId, properties.provider(), properties.ttl());
        if (planned.isEmpty()) return;
        var route = planned.get();
        if (!route.provider().equals(properties.provider())) {
            throw new IllegalStateException("Persisted secondary provider is not configured; retain input for recovery");
        }
        var now = dispatchClock.instant();
        var claim = metrics.measure(DeliveryMetrics.Stage.DISPATCH_CLAIM,
                () -> store.claimSecondary(event, route, now, now.plus(dispatchProperties.leaseDuration())));
        switch (claim.status()) {
            case IN_PROGRESS -> throw new DispatchAttemptInProgressException(event.deliveryId());
            case RETRY_WAIT -> {
                metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_RETRY_WAIT);
                throw new DispatchRetryPendingException(event.deliveryId());
            }
            case ALREADY_ACCEPTED -> {
                DeliveryAudit.record(event, "secondary", "duplicate", route.attemptId(), route.provider(), claim.status().name());
                return;
            }
            case REVIEW_REQUIRED, DECISION_PENDING -> {
                boolean review = claim.status() == DispatchClaimStatus.REVIEW_REQUIRED;
                DeliveryAudit.record(event, "secondary", review ? "review_required" : "decision_pending",
                        route.attemptId(), route.provider(), claim.status().name());
                metrics.outcome(review ? DeliveryMetrics.Outcome.DISPATCH_REVIEW : DeliveryMetrics.Outcome.DISPATCH_DECISION_PENDING);
                return;
            }
            case CLAIMED -> { }
        }
        var attempt = claim.attempt();
        if (!dispatchClock.instant().isBefore(attempt.deadline())) {
            persistFailure(event, attempt, DispatchRetryPolicy.expired(attempt.deadline(), 2));
            return;
        }
        final ProviderDispatchResponse response;
        try {
            response = metrics.measure(DeliveryMetrics.Stage.DISPATCH_TCP, () -> client.send(event, route.attemptId()));
            if (!Boolean.TRUE.equals(response.accepted())) throw new ProviderFailureException(ProviderFailureException.Kind.INVALID_RESPONSE);
        } catch (ProviderFailureException failure) {
            persistFailure(event, attempt, DispatchRetryPolicy.decide(attempt, failure.kind(), dispatchClock.instant(), dispatchProperties));
            return;
        }
        if (!dispatchClock.instant().isBefore(attempt.deadline())) {
            persistFailure(event, attempt, DispatchRetryPolicy.expired(attempt.deadline(), 2));
            return;
        }
        metrics.measure(DeliveryMetrics.Stage.DISPATCH_STORE,
                () -> store.markAccepted(attempt, response.processedAt(), dispatchClock.instant()));
        metrics.outcome(DeliveryMetrics.Outcome.SECONDARY_ACCEPTED);
        DeliveryAudit.record(event, "secondary", "accepted", route.attemptId(), route.provider(), "RECEIVED");
    }

    private void persistFailure(DeliveryEvent event, DispatchAttempt attempt,
                                DispatchFailureDecision decision) {
        metrics.measure(DeliveryMetrics.Stage.DISPATCH_STORE, () -> store.recordFailure(attempt, decision));
        DeliveryAudit.record(event, "secondary", decision.state().name().toLowerCase(java.util.Locale.ROOT),
                attempt.attemptId(), attempt.provider(), decision.reason());
        if (decision.state() == DispatchFailureDecision.State.RETRY_SCHEDULED) {
            metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_RETRY_SCHEDULED);
            throw new DispatchRetryPendingException(event.deliveryId());
        }
        metrics.outcome(decision.state() == DispatchFailureDecision.State.REVIEW_REQUIRED
                ? DeliveryMetrics.Outcome.DISPATCH_REVIEW : DeliveryMetrics.Outcome.DISPATCH_DECISION_PENDING);
    }
}
