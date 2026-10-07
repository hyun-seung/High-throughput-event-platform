package messaging.carrier.sender;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageTopics;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/** Invokes the provider once, freezes the observation, and publishes only failures. */
public class CarrierHttpSendService {
    private final CarrierSendGate gate;
    private final CarrierHttpAttemptStore attempts;
    private final CarrierProviderClient provider;
    private final CarrierErrorNormalizer normalizer;
    private final KafkaTemplate<String, CarrierHttpResult> kafka;
    private final Clock clock;
    private final Duration recoveryGrace;

    public CarrierHttpSendService(CarrierSendGate gate, CarrierHttpAttemptStore attempts,
                                  CarrierProviderClient provider, CarrierErrorNormalizer normalizer,
                                  KafkaTemplate<String, CarrierHttpResult> kafka, Clock clock,
                                  Duration recoveryGrace) {
        this.gate = Objects.requireNonNull(gate);
        this.attempts = Objects.requireNonNull(attempts);
        this.provider = Objects.requireNonNull(provider);
        this.normalizer = Objects.requireNonNull(normalizer);
        this.kafka = Objects.requireNonNull(kafka);
        this.clock = Objects.requireNonNull(clock);
        this.recoveryGrace = Objects.requireNonNull(recoveryGrace);
    }

    public void send(HttpSendCommand command) throws ExecutionException, InterruptedException {
        CarrierSendGate.Decision decision = gate.claim(command);
        switch (decision) {
            case INELIGIBLE -> { return; }
            case CLAIM_BUSY -> throw new IllegalStateException("Carrier send claim is held by another execution");
            case IN_PROGRESS -> {
                Instant now = clock.instant();
                var recovered = attempts.recoverStale(command, now, now.minus(recoveryGrace));
                if (recovered.isEmpty()) throw new IllegalStateException("Carrier HTTP invocation is still in progress");
                publishIfNeeded(command, recovered.get());
            }
            case OBSERVED -> publishIfNeeded(command,
                    attempts.observation(command).orElseThrow(() -> new IllegalStateException("Missing HTTP observation")));
            case SEND -> {
                CarrierProviderReply reply = provider.send(command);
                CarrierHttpResult result = result(command, reply, clock.instant());
                attempts.record(command, result);
                publishIfNeeded(command, result);
            }
        }
    }

    private CarrierHttpResult result(HttpSendCommand command, CarrierProviderReply reply, Instant now) {
        String id = CarrierHttpResult.id(command);
        String clientMsgId = command.request().clientMsgId();
        if (reply instanceof CarrierProviderReply.Accepted) {
            return new CarrierHttpResult(id, clientMsgId, command.attemptId(), command.carrier(),
                    command.invocation(), "HTTP_RESPONSE", CarrierHttpResult.Status.ACCEPTED,
                    200, null, null, null, null, now);
        }
        if (reply instanceof CarrierProviderReply.Failed failure) {
            return new CarrierHttpResult(id, clientMsgId, command.attemptId(), command.carrier(),
                    command.invocation(), "HTTP_RESPONSE", CarrierHttpResult.Status.FAILED,
                    failure.httpStatus(), failure.providerStatus(), failure.code(),
                    normalizer.normalize(failure.code()), failure.message(), now);
        }
        return new CarrierHttpResult(id, clientMsgId, command.attemptId(), command.carrier(),
                command.invocation(), "HTTP_TIMEOUT", CarrierHttpResult.Status.TIMEOUT,
                null, null, null, null, null, now);
    }

    private void publishIfNeeded(HttpSendCommand command, CarrierHttpResult result)
            throws ExecutionException, InterruptedException {
        if (!result.needsPublication()) return;
        try {
            kafka.send(MessageTopics.MSG_RESULT, result.clientMsgId(), result).get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
        attempts.published(command, result);
    }
}
