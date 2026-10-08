package messaging.pre.send;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageSubmission;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PreSendPreparationTest {
    private static final Instant RECEIVED_AT = Instant.parse("2026-10-07T00:00:00Z");
    private final PreSendReferenceReader references = mock(PreSendReferenceReader.class);
    private final InitialCarrierStore carriers = mock(InitialCarrierStore.class);
    private final MessageSubmission admission = new MessageSubmission("a".repeat(32), 42L, "customer-1",
            "01012345678", MessageCategory.GENERAL, Map.of("message", "hello"),
            Map.of("text", "secondary"), RECEIVED_AT);
    private final PreSendPreparation preparation = new PreSendPreparation(references, carriers,
            Clock.fixed(RECEIVED_AT.plusSeconds(10), ZoneOffset.UTC), Duration.ofHours(3));

    @Test
    void validContractCreatesStableFirstCarrierCommand() {
        when(references.findContract(42L)).thenReturn(Optional.of(new ClientMessageContract(42L, true)));
        when(references.firstCarrier(admission.recipientNumber()))
                .thenReturn(new CarrierResolution(HttpCarrier.KT, true));
        when(carriers.resolve(eq(admission), any()))
                .thenAnswer(call -> Optional.of(call.<java.util.function.Supplier<CarrierResolution>>getArgument(1).get()));

        var first = (PreSendPreparation.Ready) preparation.prepare(admission);
        var replay = (PreSendPreparation.Ready) preparation.prepare(admission);

        assertEquals(first, replay);
        assertEquals(HttpCarrier.KT, first.command().carrier());
        assertEquals(1, first.command().invocation());
        assertEquals(RECEIVED_AT.plus(Duration.ofHours(3)), first.command().deadlineAt());
        assertEquals(admission.clientMsgId(), first.command().request().clientMsgId());
        assertEquals(admission.payload(), first.command().request().payload());
        assertTrue(first.carrierMapped());
    }

    @Test
    void missingOrDisabledContractDoesNotChooseCarrier() {
        when(references.findContract(42L))
                .thenReturn(Optional.empty(), Optional.of(new ClientMessageContract(42L, false)));

        assertEquals(new PreSendPreparation.Rejected(PreSendPreparation.Reason.CONTRACT_MISSING),
                preparation.prepare(admission));
        assertEquals(new PreSendPreparation.Rejected(PreSendPreparation.Reason.CONTRACT_DISABLED),
                preparation.prepare(admission));
        verifyNoInteractions(carriers);
    }

    @Test
    void closedOriginDoesNotProduceACommand() {
        when(references.findContract(42L)).thenReturn(Optional.of(new ClientMessageContract(42L, true)));
        when(carriers.resolve(eq(admission), any())).thenReturn(Optional.empty());

        assertEquals(new PreSendPreparation.Inactive(), preparation.prepare(admission));
    }

    @Test
    void expiredAdmissionDoesNotReadReferencesOrCreateACommand() {
        var expired = new PreSendPreparation(references, carriers,
                Clock.fixed(RECEIVED_AT.plus(Duration.ofHours(3)), ZoneOffset.UTC), Duration.ofHours(3));

        assertEquals(new PreSendPreparation.Expired(), expired.prepare(admission));
        verifyNoInteractions(references, carriers);
    }
}
