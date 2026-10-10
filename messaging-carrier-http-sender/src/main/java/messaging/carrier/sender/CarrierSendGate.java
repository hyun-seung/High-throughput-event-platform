package messaging.carrier.sender;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.RedisSendAttemptGuard;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/** Checks the pod route and claims one provider invocation before any network call. */
public class CarrierSendGate {
    public enum Decision { SEND, CLAIM_BUSY, IN_PROGRESS, OBSERVED, INELIGIBLE }

    private final HttpCarrier carrier;
    private final CarrierHttpAttemptStore attempts;
    private final RedisSendAttemptGuard redis;
    private final Clock clock;
    private final Duration claimTtl;

    public CarrierSendGate(HttpCarrier carrier, CarrierHttpAttemptStore attempts,
                           RedisSendAttemptGuard redis, Clock clock, Duration claimTtl) {
        this.carrier = Objects.requireNonNull(carrier);
        this.attempts = Objects.requireNonNull(attempts);
        this.redis = Objects.requireNonNull(redis);
        this.clock = Objects.requireNonNull(clock);
        this.claimTtl = Objects.requireNonNull(claimTtl);
        if (claimTtl.isNegative() || claimTtl.isZero()) throw new IllegalArgumentException("Invalid claim TTL");
    }

    public Decision claim(HttpSendCommand command) {
        Objects.requireNonNull(command);
        if (command.carrier() != carrier) {
            throw new IllegalArgumentException("HTTP command addressed to another carrier pod");
        }
        var reservation = attempts.reserve(command, clock.instant());
        var state = reservation.state();
        if (state == CarrierHttpAttemptStore.State.INELIGIBLE) return Decision.INELIGIBLE;
        if (state == CarrierHttpAttemptStore.State.OBSERVED) return Decision.OBSERVED;
        if (state == CarrierHttpAttemptStore.State.SENDING) return Decision.IN_PROGRESS;
        if (!redis.claimHttp(command, claimTtl)) return Decision.CLAIM_BUSY;
        return attempts.begin(reservation, clock.instant()) ? Decision.SEND : Decision.CLAIM_BUSY;
    }
}
