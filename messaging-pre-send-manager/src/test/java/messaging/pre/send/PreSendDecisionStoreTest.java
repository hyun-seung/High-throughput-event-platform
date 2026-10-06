package messaging.pre.send;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.PreSendFailure;
import messaging.pre.send.reference.PreSendPreparation;
import messaging.pre.send.reference.PreSendReferenceReader;
import messaging.pre.send.reference.InitialCarrierStore;
import messaging.pre.send.reference.ClientMessageContract;
import messaging.pre.send.reference.CarrierResolution;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
    private final InitialCarrierStore carriers = mock(InitialCarrierStore.class);
    private final Clock clock = Clock.fixed(NOW.plusSeconds(5), ZoneOffset.UTC);
    private final PreSendPreparation preparation = new PreSendPreparation(references, carriers, clock, Duration.ofHours(3));

    @Test
    void storesTheCommandBeforeReplayAndDoesNotReevaluateReferences() {
        var item = new HashMap<>(MessageOriginCodec.encode(admission, mapper));
        when(db.getItem(any(software.amazon.awssdk.services.dynamodb.model.GetItemRequest.class)))
                .thenAnswer(ignored -> GetItemResponse.builder().item(item).build());
        var command = new HttpSendCommand("attempt-1", HttpCarrier.KT, 1, NOW.plusSeconds(100),
                new HttpProviderRequest(admission.clientMsgId(), 42L, "GENERAL", "01012345678",
                        admission.payload(), NOW));
        when(references.findContract(42L)).thenReturn(Optional.of(new ClientMessageContract(42L, true)));
        when(carriers.resolve(eq(admission), any())).thenReturn(Optional.of(new CarrierResolution(HttpCarrier.KT, true)));
        when(db.updateItem(any(UpdateItemRequest.class))).thenAnswer(call -> {
            UpdateItemRequest request = call.getArgument(0);
            item.put(PreSendDecisionStore.DISPATCH, request.expressionAttributeValues().get(":dispatch"));
            return software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse.builder().build();
        });

        var first = new PreSendDecisionStore(db, mapper, preparation, clock).prepareOrLoad(admission);
        var replay = new PreSendDecisionStore(db, mapper, preparation, clock).prepareOrLoad(admission);

        assertEquals(first, replay);
        assertEquals(command.carrier(), first.orElseThrow().command().carrier());
        assertEquals(command.request(), first.orElseThrow().command().request());
        verify(references, times(1)).findContract(42L);
        verify(db, times(1)).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void freezesContractRejectionAsAPreSendResult() {
        var item = MessageOriginCodec.encode(admission, mapper);
        when(db.getItem(any(software.amazon.awssdk.services.dynamodb.model.GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());
        when(references.findContract(42L)).thenReturn(Optional.of(new ClientMessageContract(42L, false)));

        var result = new PreSendDecisionStore(db, mapper, preparation, clock).prepareOrLoad(admission);

        assertEquals(PreSendFailure.Reason.CONTRACT_DISABLED, result.orElseThrow().failure().reason());
        assertEquals("PRE_SEND", result.orElseThrow().failure().source());
        verify(db).updateItem(any(UpdateItemRequest.class));
    }
}
