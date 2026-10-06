package messaging.common.messages;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/** Code-specific next step after an explicit HTTP failure or a failed webhook. */
public final class PrimaryHttpFailureDecision {
    public sealed interface Action permits Send, FailPrimary { }

    /** Each invocation retains the message's stable clientMsgId; carrier and invocation stay in attempt state. */
    public record Send(HttpCarrier carrier, int invocation, Instant notBefore) implements Action { }

    /** First-send stage failure; the caller must then evaluate secondary-send eligibility. */
    public record FailPrimary(int errorCode) implements Action { }

    /**
     * The caller must check the persisted invocation, active stage and original deadline before
     * applying this decision. A Kafka replay of one result must not create another invocation.
     */
    public static Action decide(int normalizedErrorCode, HttpCarrier currentCarrier, int invocation,
                                Set<HttpCarrier> attemptedCarriers, Instant decidedAt) {
        Objects.requireNonNull(currentCarrier);
        Objects.requireNonNull(attemptedCarriers);
        Objects.requireNonNull(decidedAt);
        if (invocation < 1 || invocation > PrimaryHttpRetryPolicy.TPS_MAX_RETRIES + 1) {
            throw new IllegalArgumentException("Invalid first-send invocation");
        }

        if (normalizedErrorCode == PrimaryHttpFailureCodes.NOT_OUR_CARRIER) {
            EnumSet<HttpCarrier> attempted = EnumSet.noneOf(HttpCarrier.class);
            attempted.addAll(attemptedCarriers);
            attempted.add(currentCarrier);
            return PrimaryHttpCarrierRouting.nextUntried(attempted)
                    .<Action>map(next -> new Send(next, 1, decidedAt))
                    .orElseGet(() -> new FailPrimary(PrimarySendFailureCodes.NO_MATCHING_CARRIER));
        }
        if (normalizedErrorCode == PrimaryHttpFailureCodes.TPS_EXCEEDED) {
            if (invocation <= PrimaryHttpRetryPolicy.TPS_MAX_RETRIES) {
                return new Send(currentCarrier, invocation + 1,
                        decidedAt.plus(PrimaryHttpRetryPolicy.TPS_RETRY_DELAY));
            }
            return new FailPrimary(PrimarySendFailureCodes.TPS_RETRY_EXHAUSTED);
        }
        throw new IllegalArgumentException("No first-send routing policy for error code " + normalizedErrorCode);
    }

    private PrimaryHttpFailureDecision() { }
}
