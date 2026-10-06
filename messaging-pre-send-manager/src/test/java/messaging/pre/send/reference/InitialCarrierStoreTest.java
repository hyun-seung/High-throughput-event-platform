package messaging.pre.send.reference;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class InitialCarrierStoreTest {
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final InitialCarrierStore store = new InitialCarrierStore(db, mapper);
    private final MessageSubmission admission = new MessageSubmission(
            "00000000-0000-0000-0000-000000000001", 42L, "customer-1", "01012345678",
            MessageCategory.GENERAL, Map.of("message", "hello"), false,
            Instant.parse("2026-10-04T00:00:00Z"));

    @Test
    void firstSelectionIsConditionallyStoredOnMatchingOrigin() {
        when(db.getItem(any(GetItemRequest.class))).thenReturn(origin(originItem()));
        CarrierResolution selected = new CarrierResolution(HttpCarrier.KT, true);

        assertEquals(Optional.of(selected), store.resolve(admission, () -> selected));

        ArgumentCaptor<UpdateItemRequest> update = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(db).updateItem(update.capture());
        assertEquals("ORIGIN", update.getValue().tableName());
        assertTrue(update.getValue().conditionExpression().contains("attribute_not_exists(#carrier)"));
        assertTrue(update.getValue().conditionExpression().contains("attribute_not_exists(completion_event_id)"));
        assertEquals("KT", update.getValue().expressionAttributeValues().get(":carrier").s());
        assertTrue(update.getValue().expressionAttributeValues().get(":mapped").bool());
        ArgumentCaptor<GetItemRequest> read = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(db).getItem(read.capture());
        assertTrue(read.getValue().consistentRead());
    }

    @Test
    void redeliveryReusesStoredSelectionWithoutReadingChangedCache() {
        Map<String, AttributeValue> existing = originItem();
        existing.put(InitialCarrierStore.CARRIER, AttributeValue.fromS("SKT"));
        existing.put(InitialCarrierStore.MAPPED, AttributeValue.fromBool(false));
        when(db.getItem(any(GetItemRequest.class))).thenReturn(origin(existing));
        Supplier<CarrierResolution> changedCache = mock(Supplier.class);

        assertEquals(Optional.of(new CarrierResolution(HttpCarrier.SKT, false)),
                store.resolve(admission, changedCache));
        verifyNoInteractions(changedCache);
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void concurrentSelectionReturnsStoredWinner() {
        Map<String, AttributeValue> winner = originItem();
        winner.put(InitialCarrierStore.CARRIER, AttributeValue.fromS("KT"));
        winner.put(InitialCarrierStore.MAPPED, AttributeValue.fromBool(true));
        when(db.getItem(any(GetItemRequest.class)))
                .thenReturn(origin(originItem()), origin(winner));
        when(db.updateItem(any(UpdateItemRequest.class)))
                .thenThrow(ConditionalCheckFailedException.builder().build());

        assertEquals(Optional.of(new CarrierResolution(HttpCarrier.KT, true)),
                store.resolve(admission, () -> new CarrierResolution(HttpCarrier.SKT, false)));
    }

    @Test
    void completedOrMissingOriginCannotChooseCarrier() {
        Map<String, AttributeValue> completed = originItem();
        completed.put("completion_event_id", AttributeValue.fromS("final-1"));
        when(db.getItem(any(GetItemRequest.class)))
                .thenReturn(origin(Map.of()), origin(completed));
        Supplier<CarrierResolution> choose = mock(Supplier.class);

        assertEquals(Optional.empty(), store.resolve(admission, choose));
        assertEquals(Optional.empty(), store.resolve(admission, choose));
        verifyNoInteractions(choose);
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void mismatchedKafkaAdmissionDoesNotFreezeRoute() {
        when(db.getItem(any(GetItemRequest.class))).thenReturn(origin(originItem()));
        MessageSubmission changed = new MessageSubmission(admission.clientMsgId(), admission.clientId(),
                admission.messageId(), admission.recipientNumber(), admission.messageCategory(),
                Map.of("message", "different"), admission.fallbackAllowed(), admission.receivedAt());

        assertThrows(IllegalStateException.class,
                () -> store.resolve(changed, () -> new CarrierResolution(HttpCarrier.SKT, false)));
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    private Map<String, AttributeValue> originItem() {
        return new HashMap<>(MessageOriginCodec.encode(admission, mapper));
    }

    private static GetItemResponse origin(Map<String, AttributeValue> item) {
        return GetItemResponse.builder().item(item).build();
    }
}
