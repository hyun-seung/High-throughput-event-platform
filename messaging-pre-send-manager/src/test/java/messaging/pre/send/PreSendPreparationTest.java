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
    private final MessageSubmission admission = new MessageSubmission("a".repeat(32), 42L, "customer-1",
            "01012345678", MessageCategory.GENERAL, Map.of("message", "hello"),
            Map.of("text", "secondary"), RECEIVED_AT);
    private final PreSendPreparation preparation = new PreSendPreparation(references,
            Clock.fixed(RECEIVED_AT.plusSeconds(10), ZoneOffset.UTC), Duration.ofHours(3));

    @Test
    void validContractCreatesStableFirstCarrierCommand() {
        when(references.findContract(42L)).thenReturn(Optional.of(new ClientMessageContract(42L, true)));
        when(references.firstCarrier(admission.recipientNumber()))
                .thenReturn(new CarrierResolution(HttpCarrier.KT, true));

        var first = (PreSendPreparation.Ready) preparation.prepare(admission, null);
        var replay = (PreSendPreparation.Ready) preparation.prepare(admission, null);

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
                preparation.prepare(admission, null));
        assertEquals(new PreSendPreparation.Rejected(PreSendPreparation.Reason.CONTRACT_DISABLED),
                preparation.prepare(admission, null));
        verify(references, never()).firstCarrier(any());
    }

    @Test
    void storedCarrierWinsOverChangedPhoneMapping() {
        when(references.findContract(42L)).thenReturn(Optional.of(new ClientMessageContract(42L, true)));
        var ready = (PreSendPreparation.Ready) preparation.prepare(admission, new CarrierResolution(HttpCarrier.LGU, false));
        assertEquals(HttpCarrier.LGU, ready.command().carrier());
        assertFalse(ready.carrierMapped());
        verify(references, never()).firstCarrier(any());
    }

    @Test
    void expiredAdmissionDoesNotReadReferencesOrCreateACommand() {
        var expired = new PreSendPreparation(references,
                Clock.fixed(RECEIVED_AT.plus(Duration.ofHours(3)), ZoneOffset.UTC), Duration.ofHours(3));

        assertEquals(new PreSendPreparation.Expired(), expired.prepare(admission, null));
        verifyNoInteractions(references);
    }
}
