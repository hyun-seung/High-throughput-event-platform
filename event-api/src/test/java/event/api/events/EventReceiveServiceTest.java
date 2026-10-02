package event.api.events;

import event.common.events.EventSubmission;
import event.common.events.EventType;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EventReceiveServiceTest {
    private final EventUsageLimiter usage = mock(EventUsageLimiter.class);
    private final EventDuplicateGuard duplicates = mock(EventDuplicateGuard.class);
    private final EventOriginStore origins = mock(EventOriginStore.class);
    private final EventReceiveService service = new EventReceiveService(usage, duplicates, origins,
            Clock.fixed(Instant.parse("2026-10-02T00:00:00Z"), ZoneOffset.UTC));
    private final EventReceiveRequest request = new EventReceiveRequest("customer-1", "01012345678",
            EventType.GENERAL, Map.of("message", "hello"), true);

    @Test
    void chargesBeforeDeduplicationAndReusesExistingExecution() {
        String id = "00000000-0000-0000-0000-000000000001";
        when(duplicates.claim(eq(42L), eq(request), anyString()))
                .thenReturn(new EventDuplicateGuard.Claim(id, "key", "value", false, true));
        when(origins.find(id)).thenReturn(Optional.of(event(id)));

        EventReceiveResponse response = service.receive(42L, request);

        assertEquals(id, response.executionId());
        var order = inOrder(usage, duplicates, origins);
        order.verify(usage).charge(42L, EventType.GENERAL);
        order.verify(duplicates).claim(eq(42L), eq(request), anyString());
        order.verify(origins).find(id);
        verify(origins, never()).save(any());
    }

    @Test
    void duplicateMarkerWithoutConfirmedOriginNeverReturns202() {
        String id = "00000000-0000-0000-0000-000000000001";
        when(duplicates.claim(eq(42L), eq(request), anyString()))
                .thenReturn(new EventDuplicateGuard.Claim(id, "key", "value", false, true));
        when(origins.find(id)).thenReturn(Optional.empty());

        EventAdmissionException failure = assertThrows(EventAdmissionException.class,
                () -> service.receive(42L, request));
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, failure.status());
    }

    @Test
    void ambiguousPutThatCommittedConvergesByExecutionId() {
        String id = "00000000-0000-0000-0000-000000000001";
        when(duplicates.claim(eq(42L), eq(request), anyString()))
                .thenReturn(new EventDuplicateGuard.Claim(id, "key", "value", true, true));
        doThrow(DynamoDbException.builder().message("response lost").build()).when(origins).save(any());
        when(origins.find(id)).thenReturn(Optional.of(event(id)));

        assertEquals(id, service.receive(42L, request).executionId());
        verify(duplicates, never()).release(any());
    }

    @Test
    void invalidRecipientIsChargedButNeverClaimsDuplicateOrSavesOrigin() {
        var invalid = new EventReceiveRequest("customer-1", "+821012345678", EventType.GENERAL,
                request.payload(), true);
        EventAdmissionException failure = assertThrows(EventAdmissionException.class,
                () -> service.receive(42L, invalid));
        assertEquals(HttpStatus.BAD_REQUEST, failure.status());
        verify(usage).charge(42L, EventType.GENERAL);
        verifyNoInteractions(duplicates, origins);
    }

    private EventSubmission event(String id) {
        return new EventSubmission(id, 42L, request.eventId(), request.recipientNumber(), request.eventType(),
                request.payload(), true, Instant.parse("2026-10-02T00:00:00Z"));
    }
}
