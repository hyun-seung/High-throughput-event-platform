package messaging.carrier.sender;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
    private final MeterRegistry meters;

    public CarrierHttpSendService(CarrierSendGate gate, CarrierHttpAttemptStore attempts,
                                  CarrierProviderClient provider, CarrierErrorNormalizer normalizer,
                                  KafkaTemplate<String, CarrierHttpResult> kafka, Clock clock,
                                  Duration recoveryGrace) {
        this(gate, attempts, provider, normalizer, kafka, clock, recoveryGrace, null);
    }

    public CarrierHttpSendService(CarrierSendGate gate, CarrierHttpAttemptStore attempts,
                                  CarrierProviderClient provider, CarrierErrorNormalizer normalizer,
                                  KafkaTemplate<String, CarrierHttpResult> kafka, Clock clock,
                                  Duration recoveryGrace, MeterRegistry meters) {
        this.gate = Objects.requireNonNull(gate);
        this.attempts = Objects.requireNonNull(attempts);
        this.provider = Objects.requireNonNull(provider);
        this.normalizer = Objects.requireNonNull(normalizer);
        this.kafka = Objects.requireNonNull(kafka);
        this.clock = Objects.requireNonNull(clock);
        this.recoveryGrace = Objects.requireNonNull(recoveryGrace);
        this.meters = meters;
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
                Timer.Sample sample = meters == null ? null : Timer.start(meters);
                CarrierProviderReply reply;
                try {
                    reply = provider.send(command);
                } catch (RuntimeException failure) {
                    recordAttempt(command, "EXCEPTION", sample);
                    throw failure;
                }
                CarrierHttpResult result = result(command, reply, clock.instant());
                recordAttempt(command, result.status().name(), sample);
                attempts.record(command, result);
                publishIfNeeded(command, result);
            }
        }
    }

    private void recordAttempt(HttpSendCommand command, String outcome, Timer.Sample sample) {
        if (meters == null) return;
        String carrier = command.carrier().name();
        meters.counter("messaging.carrier.http.attempts", "carrier", carrier, "outcome", outcome).increment();
        sample.stop(meters.timer("messaging.carrier.http.duration", "carrier", carrier, "outcome", outcome));
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
