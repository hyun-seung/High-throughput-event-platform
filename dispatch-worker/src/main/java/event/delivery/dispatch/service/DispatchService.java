package event.delivery.dispatch.service;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryIds;
import event.common.metrics.DeliveryMetrics;
import event.common.metrics.DeliveryAudit;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.external.client.ProviderFailureException;
import event.delivery.dispatch.model.DispatchClaim;
import event.delivery.dispatch.model.DispatchFailureDecision;
import event.delivery.dispatch.port.DeliveryProviderClient;
import event.delivery.dispatch.port.DispatchAttemptStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchService {

    private static final int INITIAL_ROUTE_ORDER = 1;
    private static final int INITIAL_ATTEMPT_NUMBER = 1;

    private final DispatchAttemptStore dispatchAttemptStore;
    private final DeliveryProviderClient deliveryProviderClient;
    private final DispatchProperties dispatchProperties;
    private final Clock dispatchClock;
    private final DeliveryMetrics metrics;
    private final SecondaryDispatchService secondaryDispatch;

    public void dispatch(DeliveryEvent event) {
        metrics.measure(DeliveryMetrics.Stage.DISPATCH_PROCESS, () -> process(event));
    }

    private void process(DeliveryEvent event) {
        String attemptId = DeliveryIds.attemptId(
                event.deliveryId(),
                dispatchProperties.provider(),
                INITIAL_ROUTE_ORDER,
                INITIAL_ATTEMPT_NUMBER
        );
        Instant now = dispatchClock.instant();
        DispatchClaim claim = metrics.measure(DeliveryMetrics.Stage.DISPATCH_CLAIM, () -> dispatchAttemptStore.claim(
                event,
                attemptId,
                dispatchProperties.provider(),
                now,
                now.plus(dispatchProperties.leaseDuration()),
                event.occurredAt().plus(dispatchProperties.primaryTtl())
        ));

        switch (claim.status()) {
            case ALREADY_ACCEPTED -> {
                DeliveryAudit.record(event, "dispatch", "duplicate", attemptId, dispatchProperties.provider(), "already_accepted");
                metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_DUPLICATE);
                log.debug("Completed dispatch skipped. deliveryId={}, attemptId={}", event.deliveryId(), attemptId);
            }
            case IN_PROGRESS -> {
                metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_IN_PROGRESS);
                throw new DispatchAttemptInProgressException(event.deliveryId());
            }
            case RETRY_WAIT -> {
                metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_RETRY_WAIT);
                throw new DispatchRetryPendingException(event.deliveryId());
            }
            case DECISION_PENDING -> {
                DeliveryAudit.record(event, "dispatch", "decision_pending", attemptId, dispatchProperties.provider(), "pending_next_stage");
                metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_DECISION_PENDING);
                secondaryDispatch.dispatch(event, attemptId);
            }
            case REVIEW_REQUIRED -> {
                DeliveryAudit.record(event, "dispatch", "review_required", attemptId, dispatchProperties.provider(), "result_unknown");
                metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_REVIEW);
                log.warn(
                    "Dispatch requires operational review; provider not called. deliveryId={}, attemptId={}",
                    event.deliveryId(), attemptId);
            }
            case CLAIMED -> invokeProvider(event, claim, attemptId);
        }
    }

    private void invokeProvider(DeliveryEvent event, DispatchClaim claim, String attemptId) {
        if (!dispatchClock.instant().isBefore(claim.attempt().deadline())) {
            persistFailure(event, claim, attemptId, DispatchRetryPolicy.expired(claim.attempt().deadline()));
            return;
        }
        final ProviderDispatchResponse response;
        try {
            response = metrics.measure(DeliveryMetrics.Stage.DISPATCH_HTTP, () -> {
                ProviderDispatchResponse received = deliveryProviderClient.send(event, attemptId, claim.attempt().retryCount() + 1);
                if (!Boolean.TRUE.equals(received.accepted())) {
                    throw new ProviderFailureException(ProviderFailureException.Kind.INVALID_RESPONSE);
                }
                return received;
            });
        } catch (ProviderFailureException failure) {
            persistFailure(event, claim, attemptId,
                    DispatchRetryPolicy.decide(claim.attempt(), failure.kind(), dispatchClock.instant(), dispatchProperties));
            return;
        }

        if (!dispatchClock.instant().isBefore(claim.attempt().deadline())) {
            persistFailure(event, claim, attemptId, DispatchRetryPolicy.expired(claim.attempt().deadline()));
            return;
        }

        metrics.measure(DeliveryMetrics.Stage.DISPATCH_STORE, () -> dispatchAttemptStore.markAccepted(
                claim.attempt(),
                response.processedAt(),
                dispatchClock.instant()
        ));
        metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_ACCEPTED);
        metrics.accepted(event.occurredAt(), dispatchClock.instant());
        DeliveryAudit.record(event, "dispatch", "accepted", attemptId, dispatchProperties.provider(), "accepted");

        log.debug("Dispatch completed. deliveryId={}, attemptId={}, processedAt={}",
                event.deliveryId(), attemptId, response.processedAt());
    }

    private void persistFailure(DeliveryEvent event, DispatchClaim claim, String attemptId, DispatchFailureDecision decision) {
        metrics.measure(DeliveryMetrics.Stage.DISPATCH_STORE, () -> dispatchAttemptStore.recordFailure(claim.attempt(), decision));
        String outcome = switch (decision.state()) {
            case RETRY_SCHEDULED -> "retry_scheduled";
            case REVIEW_REQUIRED -> "review_required";
            case DECISION_PENDING -> "decision_pending";
        };
        DeliveryAudit.record(event, "dispatch", outcome, attemptId, dispatchProperties.provider(), decision.reason());
        metrics.outcome(switch (decision.state()) {
            case RETRY_SCHEDULED -> DeliveryMetrics.Outcome.DISPATCH_RETRY_SCHEDULED;
            case REVIEW_REQUIRED -> DeliveryMetrics.Outcome.DISPATCH_REVIEW;
            case DECISION_PENDING -> DeliveryMetrics.Outcome.DISPATCH_DECISION_PENDING;
        });
        if (decision.state() == DispatchFailureDecision.State.RETRY_SCHEDULED) {
            throw new DispatchRetryPendingException(event.deliveryId());
        }
        if (decision.state() == DispatchFailureDecision.State.DECISION_PENDING) {
            secondaryDispatch.dispatch(event, attemptId);
        }
    }
}
