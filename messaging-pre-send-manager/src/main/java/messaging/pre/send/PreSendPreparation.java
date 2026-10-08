package messaging.pre.send;

import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageSubmission;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Prepares one stable first-send command from an admitted message and current references. */
public final class PreSendPreparation {
    public sealed interface Decision permits Ready, Rejected, Inactive, Expired { }
    public record Ready(HttpSendCommand command, boolean carrierMapped) implements Decision { }
    public record Rejected(Reason reason) implements Decision { }
    public record Inactive() implements Decision { }
    public record Expired() implements Decision { }
    public enum Reason { CONTRACT_MISSING, CONTRACT_DISABLED }

    private final PreSendReferenceReader references;
    private final InitialCarrierStore carriers;
    private final Clock clock;
    private final Duration primaryTtl;

    public PreSendPreparation(PreSendReferenceReader references, InitialCarrierStore carriers,
                              Clock clock, Duration primaryTtl) {
        this.references = Objects.requireNonNull(references);
        this.carriers = Objects.requireNonNull(carriers);
        this.clock = Objects.requireNonNull(clock);
        this.primaryTtl = Objects.requireNonNull(primaryTtl);
        if (primaryTtl.isNegative() || primaryTtl.isZero()) {
            throw new IllegalArgumentException("Primary TTL must be positive");
        }
    }

    public Decision prepare(MessageSubmission admission) {
        Objects.requireNonNull(admission);
        Instant deadline = admission.receivedAt().plus(primaryTtl);
        if (!clock.instant().isBefore(deadline)) return new Expired();

        Optional<ClientMessageContract> contract = references.findContract(admission.clientId());
        if (contract.isEmpty()) return new Rejected(Reason.CONTRACT_MISSING);
        if (!contract.get().enabled()) return new Rejected(Reason.CONTRACT_DISABLED);

        Optional<CarrierResolution> selected = carriers.resolve(admission,
                () -> references.firstCarrier(admission.recipientNumber()));
        if (selected.isEmpty()) return new Inactive();
        CarrierResolution route = selected.get();
        String attemptId = HttpSendCommand.attemptId(admission.clientMsgId(), route.carrier());
        HttpProviderRequest request = new HttpProviderRequest(admission.clientMsgId(), admission.clientId(),
                admission.messageCategory().name(), admission.recipientNumber(), admission.payload(),
                admission.receivedAt());
        return new Ready(new HttpSendCommand(attemptId, route.carrier(), 1, deadline, request), route.mapped());
    }
}
