package messaging.api.messages;

import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageCategory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MessageReceiveServiceTest {
    private final MessageUsageLimiter usage = mock(MessageUsageLimiter.class);
    private final MessageDuplicateGuard duplicates = mock(MessageDuplicateGuard.class);
    private final MessageOriginStore origins = mock(MessageOriginStore.class);
    private final MessageRequestPublisher publisher = mock(MessageRequestPublisher.class);
    private final MessageReceiveService service = new MessageReceiveService(usage, duplicates, origins, publisher,
            Clock.fixed(Instant.parse("2026-10-02T00:00:00Z"), ZoneOffset.UTC));
    private final MessageReceiveRequest request = new MessageReceiveRequest("customer-1", "01012345678",
            MessageCategory.GENERAL, Map.of("message", "hello"), true);

    @BeforeEach
    void publishAcceptedOrigin() {
        when(publisher.publish(any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void reusesExistingExecutionAndRepublishesSameRequest() {
        String id = "00000000-0000-0000-0000-000000000001";
        when(duplicates.claim(eq(42L), eq(request), anyString()))
                .thenReturn(new MessageDuplicateGuard.Claim(id, "key", "value", false, true));
        when(origins.find(id)).thenReturn(Optional.of(event(id)));

        MessageReceiveResponse response = service.receive(42L, request);

        assertEquals(id, response.sendRequestId());
        var order = inOrder(usage, duplicates, origins, publisher);
        order.verify(usage).charge(42L, MessageCategory.GENERAL);
        order.verify(duplicates).claim(eq(42L), eq(request), anyString());
        order.verify(origins).find(id);
        order.verify(publisher).publish(any(MessageSubmission.class));
        verify(origins, never()).save(any());
    }

    @Test
    void duplicateMarkerWithoutConfirmedOriginNeverReturns202() {
        String id = "00000000-0000-0000-0000-000000000001";
        when(duplicates.claim(eq(42L), eq(request), anyString()))
                .thenReturn(new MessageDuplicateGuard.Claim(id, "key", "value", false, true));
        when(origins.find(id)).thenReturn(Optional.empty());

        MessageAdmissionException failure = assertThrows(MessageAdmissionException.class,
                () -> service.receive(42L, request));
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, failure.status());
    }

    @Test
    void ambiguousPutThatCommittedConvergesByExecutionId() {
        String id = "00000000-0000-0000-0000-000000000001";
        when(duplicates.claim(eq(42L), eq(request), anyString()))
                .thenReturn(new MessageDuplicateGuard.Claim(id, "key", "value", true, true));
        doThrow(DynamoDbException.builder().message("response lost").build()).when(origins).save(any());
        when(origins.find(id)).thenReturn(Optional.of(event(id)));

        assertEquals(id, service.receive(42L, request).sendRequestId());
        verify(duplicates, never()).release(any());
        verify(publisher).publish(any(MessageSubmission.class));
    }

    @Test
    void invalidRecipientIsCountedButNeverClaimsDuplicateOrSavesOrigin() {
        var invalid = new MessageReceiveRequest("customer-1", "+821012345678", MessageCategory.GENERAL,
                request.payload(), true);
        MessageAdmissionException failure = assertThrows(MessageAdmissionException.class,
                () -> service.receive(42L, invalid));
        assertEquals(HttpStatus.BAD_REQUEST, failure.status());
        verify(usage).charge(42L, MessageCategory.GENERAL);
        verifyNoInteractions(duplicates, origins);
        verifyNoInteractions(publisher);
    }

    @Test
    void usageLimitRejectsBeforeDuplicateClaimOrOriginSave() {
        doThrow(new MessageAdmissionException(HttpStatus.TOO_MANY_REQUESTS, "quota exceeded"))
                .when(usage).charge(42L, MessageCategory.GENERAL);

        MessageAdmissionException failure = assertThrows(MessageAdmissionException.class,
                () -> service.receive(42L, request));

        assertEquals(HttpStatus.TOO_MANY_REQUESTS, failure.status());
        verifyNoInteractions(duplicates, origins, publisher);
    }

    @Test
    void originSavePrecedesDirectKafkaSendAndUnconfirmedAckKeeps202() {
        String id = "00000000-0000-0000-0000-000000000001";
        when(duplicates.claim(eq(42L), eq(request), anyString()))
                .thenReturn(new MessageDuplicateGuard.Claim(id, "key", "value", true, true));
        when(publisher.publish(any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

        assertEquals(id, service.receive(42L, request).sendRequestId());

        var order = inOrder(origins, publisher);
        order.verify(origins).save(any(MessageSubmission.class));
        order.verify(publisher).publish(any(MessageSubmission.class));
    }

    @Test
    void definitiveOriginFailureNeverPublishes() {
        String id = "00000000-0000-0000-0000-000000000001";
        when(duplicates.claim(eq(42L), eq(request), anyString()))
                .thenReturn(new MessageDuplicateGuard.Claim(id, "key", "value", true, true));
        doThrow(ConditionalCheckFailedException.builder().message("collision").build())
                .when(origins).save(any());

        assertThrows(MessageAdmissionException.class, () -> service.receive(42L, request));

        verifyNoInteractions(publisher);
        verify(duplicates).release(any());
    }

    private MessageSubmission event(String id) {
        return new MessageSubmission(id, 42L, request.messageId(), request.recipientNumber(), request.messageCategory(),
                request.payload(), true, Instant.parse("2026-10-02T00:00:00Z"));
    }
}
