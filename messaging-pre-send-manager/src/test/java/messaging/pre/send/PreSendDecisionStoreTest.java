package messaging.pre.send;

import messaging.common.messages.*;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.databind.json.JsonMapper;

import java.time.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PreSendDecisionStoreTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:00:00Z");
    private final MessageSubmission admission = new MessageSubmission("a".repeat(32), 42L, "customer-id",
            "01012345678", MessageCategory.GENERAL, Map.of("text", "hello"), null, NOW);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final PreSendReferenceReader references = mock(PreSendReferenceReader.class);
    private final Clock clock = Clock.fixed(NOW.plusSeconds(5), ZoneOffset.UTC);
    private final PreSendPreparation preparation = new PreSendPreparation(references, clock, Duration.ofHours(3));
    private final PreSendDecisionStore store = new PreSendDecisionStore(db, mapper, preparation, clock);

    @Test
    void freezesCarrierAndCommandTogetherWithOneReadAndOneWrite() {
        origin(originItem());
        validContract();
        when(references.firstCarrier(admission.recipientNumber())).thenReturn(new CarrierResolution(HttpCarrier.KT, true));

        var result = store.prepareOrLoad(admission).orElseThrow();

        assertEquals(HttpCarrier.KT, result.command().carrier());
        var read = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(db).getItem(read.capture());
        assertTrue(read.getValue().consistentRead());
        var update = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(db).updateItem(update.capture());
        var request = update.getValue();
        assertEquals("KT", request.expressionAttributeValues().get(":carrier").s());
        assertTrue(request.expressionAttributeValues().get(":mapped").bool());
        assertEquals(result, mapper.readValue(request.expressionAttributeValues().get(":dispatch").s(), PreSendDispatch.class));
        assertTrue(request.conditionExpression().contains("attribute_not_exists(completion_event_id)"));
        assertTrue(request.conditionExpression().contains("attribute_not_exists(#dispatch)"));
    }

    @Test
    void replayDoesNotReevaluateReferencesOrWrite() {
        var item = originItem();
        var expected = dispatch(HttpCarrier.KT);
        item.put(PreSendDecisionStore.DISPATCH, AttributeValue.fromS(mapper.writeValueAsString(expected)));
        origin(item);

        assertEquals(Optional.of(expected), store.prepareOrLoad(admission));
        verifyNoInteractions(references);
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void legacyFrozenCarrierIsPreservedWithoutReadingPhoneCache() {
        var item = originItem();
        item.put(PreSendDecisionStore.CARRIER, AttributeValue.fromS("LGU"));
        item.put(PreSendDecisionStore.MAPPED, AttributeValue.fromBool(false));
        origin(item);
        validContract();

        assertEquals(HttpCarrier.LGU, store.prepareOrLoad(admission).orElseThrow().command().carrier());
        verify(references, never()).firstCarrier(any());
    }

    @Test
    void freezesContractRejectionAsAPreSendResult() {
        origin(originItem());
        when(references.findContract(42L)).thenReturn(Optional.of(new ClientMessageContract(42L, false)));
        var result = store.prepareOrLoad(admission).orElseThrow();
        assertEquals(PreSendFailure.Reason.CONTRACT_DISABLED, result.failure().reason());
        var update = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(db).updateItem(update.capture());
        assertFalse(update.getValue().expressionAttributeValues().containsKey(":carrier"));
        verify(references, never()).firstCarrier(any());
    }

    @Test
    void concurrentDecisionReturnsTheStoredWinner() {
        var winner = originItem();
        var expected = dispatch(HttpCarrier.LGU);
        winner.put(PreSendDecisionStore.DISPATCH, AttributeValue.fromS(mapper.writeValueAsString(expected)));
        when(db.getItem(any(GetItemRequest.class))).thenReturn(response(originItem()), response(winner));
        validContract();
        when(references.firstCarrier(any())).thenReturn(new CarrierResolution(HttpCarrier.SKT, false));
        when(db.updateItem(any(UpdateItemRequest.class))).thenThrow(ConditionalCheckFailedException.builder().build());

        assertEquals(Optional.of(expected), store.prepareOrLoad(admission));
    }

    @Test
    void completionWinningDuringPreparationDoesNotPublish() {
        var completed = originItem();
        completed.put("completion_event_id", AttributeValue.fromS("final-1"));
        when(db.getItem(any(GetItemRequest.class))).thenReturn(response(originItem()), response(completed));
        validContract();
        when(references.firstCarrier(any())).thenReturn(new CarrierResolution(HttpCarrier.SKT, false));
        when(db.updateItem(any(UpdateItemRequest.class))).thenThrow(ConditionalCheckFailedException.builder().build());

        assertTrue(store.prepareOrLoad(admission).isEmpty());
    }

    @Test
    void failedWriteNeverReturnsAnUnpersistedCommand() {
        origin(originItem());
        validContract();
        when(references.firstCarrier(any())).thenReturn(new CarrierResolution(HttpCarrier.SKT, false));
        when(db.updateItem(any(UpdateItemRequest.class))).thenThrow(DynamoDbException.builder().message("unavailable").build());
        assertThrows(DynamoDbException.class, () -> store.prepareOrLoad(admission));
    }

    @Test
    void missingOrClosedOriginDoesNotReadReferences() {
        var closed = originItem();
        closed.put("completion_event_id", AttributeValue.fromS("final-1"));
        var inactive = originItem();
        inactive.put("status", AttributeValue.fromS("COMPLETED"));
        when(db.getItem(any(GetItemRequest.class)))
                .thenReturn(response(Map.of()), response(closed), response(inactive));
        for (int i = 0; i < 3; i++) assertTrue(store.prepareOrLoad(admission).isEmpty());
        verifyNoInteractions(references);
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void mismatchedAdmissionCannotFreezeACommand() {
        origin(originItem());
        var changed = new MessageSubmission(admission.clientMsgId(), admission.clientId(), admission.messageId(),
                admission.recipientNumber(), admission.messageCategory(), Map.of("text", "changed"), null, NOW);
        assertThrows(IllegalStateException.class, () -> store.prepareOrLoad(changed));
        verifyNoInteractions(references);
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void incompleteLegacyCarrierCannotBeOverwritten() {
        var item = originItem();
        item.put(PreSendDecisionStore.MAPPED, AttributeValue.fromBool(true));
        origin(item);
        assertThrows(IllegalStateException.class, () -> store.prepareOrLoad(admission));
        verifyNoInteractions(references);
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    private void validContract() {
        when(references.findContract(42L)).thenReturn(Optional.of(new ClientMessageContract(42L, true)));
    }
    private Map<String, AttributeValue> originItem() { return new HashMap<>(MessageOriginCodec.encode(admission, mapper)); }
    private void origin(Map<String, AttributeValue> item) { when(db.getItem(any(GetItemRequest.class))).thenReturn(response(item)); }
    private static GetItemResponse response(Map<String, AttributeValue> item) { return GetItemResponse.builder().item(item).build(); }
    private PreSendDispatch dispatch(HttpCarrier carrier) {
        return new PreSendDispatch(new HttpSendCommand(HttpSendCommand.attemptId(admission.clientMsgId(), carrier),
                carrier, 1, NOW.plus(Duration.ofHours(3)), new HttpProviderRequest(admission.clientMsgId(),
                admission.clientId(), admission.messageCategory().name(), admission.recipientNumber(), admission.payload(), NOW)), null);
    }
}
