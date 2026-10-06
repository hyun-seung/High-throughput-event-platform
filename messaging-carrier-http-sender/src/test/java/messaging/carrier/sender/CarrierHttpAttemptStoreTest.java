package messaging.carrier.sender;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.PreSendDispatch;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CarrierHttpAttemptStoreTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:00:00Z");
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final CarrierHttpAttemptStore store = new CarrierHttpAttemptStore(db, mapper);
    private final MessageSubmission admission = new MessageSubmission("a".repeat(32), 42L, "customer-id",
            "01012345678", MessageCategory.GENERAL, Map.of("text", "hello"), null, NOW);
    private final HttpSendCommand command = new HttpSendCommand("attempt-kt", HttpCarrier.KT, 1,
            NOW.plusSeconds(3600), new HttpProviderRequest(admission.clientMsgId(), 42L, "GENERAL",
            "01012345678", admission.payload(), NOW));

    @Test
    void reserveRequiresTheExactFrozenCommand() {
        var origin = new HashMap<>(MessageOriginCodec.encode(admission, mapper));
        origin.put("pre_send_dispatch", AttributeValue.fromS(
                mapper.writeValueAsString(new PreSendDispatch(command, null))));
        when(db.getItem(any(software.amazon.awssdk.services.dynamodb.model.GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(origin).build());

        assertEquals(CarrierHttpAttemptStore.State.PENDING, store.reserve(command, NOW));
        verify(db).transactWriteItems(any(java.util.function.Consumer.class));

        var forged = new HttpSendCommand(command.attemptId(), command.carrier(), 1, command.deadlineAt(),
                new HttpProviderRequest(admission.clientMsgId(), 42L, "GENERAL", "01012345678",
                        Map.of("text", "different"), NOW));
        assertThrows(IllegalStateException.class, () -> store.reserve(forged, NOW));
    }

    @Test
    void noOriginCannotReserveAnAttempt() {
        when(db.getItem(any(software.amazon.awssdk.services.dynamodb.model.GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(Map.of()).build());
        assertEquals(CarrierHttpAttemptStore.State.INELIGIBLE, store.reserve(command, NOW));
        verify(db, never()).transactWriteItems(any(java.util.function.Consumer.class));
    }
}
